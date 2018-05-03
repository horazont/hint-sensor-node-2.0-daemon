import array
import asyncio
import bz2
import functools
import logging
import math
import pathlib
import time

from datetime import datetime, timedelta

import aioxmpp

import hintlib.core
import hintlib.services
import hintlib.xso

from . import protocol, sensor_stream, control_protocol
from hintlib import utils, rewrite, sample


def dig(mapping, *args, default=None):
    for step in args:
        try:
            mapping = mapping[step]
        except KeyError:
            return default
    return mapping


def rtcify_samples(samples, rtcifier):
    for s in samples:
        yield s.replace(timestamp=rtcifier.map_to_rtc(s.timestamp))


def deenumify_samples(samples):
    for s in samples:
        yield s.replace(
            sensor=s.sensor.replace(
                part=s.sensor.part.value,
                subpart=s.sensor.subpart.value if s.sensor.subpart else None,
            )
        )


def batch_samples(samples):
    curr_batch_ts = None
    curr_batch_bare_path = None
    curr_batch_samples = {}
    for s in samples:
        bare_path = s.sensor.replace(subpart=None)
        if (curr_batch_ts != s.timestamp or
                curr_batch_bare_path != bare_path):
            if curr_batch_samples:
                yield (
                    curr_batch_ts,
                    curr_batch_bare_path,
                    curr_batch_samples,
                )
                curr_batch_samples = {}

            curr_batch_ts = s.timestamp
            curr_batch_bare_path = bare_path

        if s.sensor.subpart in curr_batch_samples:
            raise RuntimeError

        curr_batch_samples[s.sensor.subpart] = s.value

    if curr_batch_samples:
        yield (
            curr_batch_ts,
            curr_batch_bare_path,
            curr_batch_samples,
        )


class SensorNode2Daemon:
    def __init__(self, args, config, loop):
        super().__init__()
        self.logger = logging.getLogger("sn2d")
        self.__loop = loop
        self.__args = args
        self.__config = config

        self._indivdual_rewriter = rewrite.IndividualSampleRewriter(
            config["samples"]["rewrite"],
            self.logger.getChild("rewrite").getChild("individual")
        )

        self._batch_rewriter = rewrite.SampleBatchRewriter(
            config["samples"]["batch"]["rewrite"],
            self.logger.getChild("rewrite").getChild("batch")
        )

        self._stream_ranges = {
            (sample.Part(part),
             sample.PART_SUBPARTS[sample.Part(part)](subpart)): range_
            for part, subpart, range_ in (
                (item["part"], item["subpart"], item["range"])
                for item in config["streams"]["ranges"]
            )
        }

        self._cputime_prev_data = None

        self.__xmpp = hintlib.core.BotCore(config["xmpp"])
        self.__xmpp.client.summon(aioxmpp.PresenceClient)
        sender = self.__xmpp.client.summon(hintlib.services.SenderService)
        sender.peer_jid = aioxmpp.JID.fromstr(config["sink"]["jid"])

        self._stream_service = \
            self._xmpp.client.summon(hintlib.services.StreamSubmitterService)
        self._stream_service.queue_size = \
            config.get("streams", {}).get("queue_length", 16)
        self._stream_service.module_name = config["sensors"]["module_name"]
        self._sample_service = \
            self._xmpp.client.summon(hintlib.services.BatchSubmitterService)
        self._sample_service.queue_size = \
            config.get("samples", {}).get("queue_length", 16)
        self._sample_service.module_name = config["sensors"]["module_name"]

        self._timeline = utils.Timeline(
            2**16,  # wraparound
            30000,  # 30s slack
        )
        self._rtcifier = utils.RTCifier(
            self._timeline,
            self.logger.getChild("rtcifier")
        )
        self._had_status = False

        self._protocol = None
        self._control_protocol = None

        imu_datadir = pathlib.Path(
            config["streams"]["datadir"]
        )
        imu_datadir.mkdir(exist_ok=True)

        # configure all the stream buffers
        self._stream_buffers = {
            path: sensor_stream.Buffer(
                imu_datadir / utils.escape_path(str(path)),
                functools.partial(
                    self._on_stream_emit,
                    path
                ),
                sample_type="h",
                logger=self.logger.getChild("buffers.{}".format(
                    path.subpart.value.replace("-", ".")
                ))
            )
            for path in (
                sample.SensorPath(
                    sample.Part.LSM303D,
                    0,
                    sample.LSM303DSubpart(
                        "{}-{}".format(subpart, axis)
                    )
                )
                for subpart in ["accel", "compass"]
                for axis in ["x", "y", "z"]
            )
        }

        self.__pre_status_buffer = []

        for buf in self._stream_buffers.values():
            buf.batch_size = config.get(
                "streams", {}
            ).get(
                "batch_size", 1024
            )

    def _on_stream_emit(self, path, t0, seq0, period, data, handle):
        item = path, t0, seq0, period, data, handle
        self._stream_service.submit_block(item)

    def _enqueue_sample_batches(self, batches):
        self._sample_service.enqueue_batches(batches)

    def _make_protocol(self):
        if self._protocol is not None:
            self.logger.warning("protocol already initialised!")
        self._protocol = protocol.SensorNode2Protocol()
        self._protocol.on_message_received.connect(self._on_message)
        return self._protocol

    def _make_control_protocol(self):
        if self._control_protocol is not None:
            self.logger.warning("control_protocol already initialised!")
        self._control_protocol = control_protocol.ControlProtocol()
        return self._control_protocol

    def _print_status(self, obj, now):
        self.logger.debug(
            "status: "
            "rtc = %s, uptime = %d",
            obj.rtc,
            obj.uptime,
        )
        self.logger.debug(
            "status: "
            "rtc offset = %s, rtcified uptime offset = %s",
            abs(now - obj.rtc),
            abs(now - self._rtcifier.map_to_rtc(obj.uptime)),
        )

        if not self.__config.get("logging", {}).get("verbose_status", False):
            return

        if obj.v1_accel_stream_state is not None:
            self.logger.debug(
                "status: "
                "accel stream info: seq = %d, timestamp = %d, period = %s",
                obj.v1_accel_stream_state[0],
                obj.v1_accel_stream_state[1],
                obj.v1_accel_stream_state[2],
            )

        if obj.v1_compass_stream_state is not None:
            self.logger.debug(
                "status: "
                "compass stream info: seq = %d, timestamp = %d, period = %s",
                obj.v1_compass_stream_state[0],
                obj.v1_compass_stream_state[1],
                obj.v1_compass_stream_state[2],
            )

        for i, bus_metrics in enumerate((obj.v2_i2c_metrics or []), 1):
            self.logger.debug("status: i2c%d: transaction overruns = %d",
                              i, bus_metrics.transaction_overruns)

        for i, bme_metrics in enumerate((obj.v4_bme280_metrics or [])):
            self.logger.debug("status: bme280[%d]: status = %d",
                              i, bme_metrics.configure_status)
            self.logger.debug("status: bme280[%d]: timeouts = %d",
                              i, bme_metrics.timeouts)

        if obj.v5_tx_metrics is not None:
            self.logger.debug(
                "status: tx buffers: allocated = %d (most: %d, %.0f%%), "
                "ready = %d, total = %d",
                obj.v5_tx_metrics.buffers_allocated,
                obj.v5_tx_metrics.most_buffers_allocated,
                (obj.v5_tx_metrics.most_buffers_allocated /
                 obj.v5_tx_metrics.buffers_total * 100),
                obj.v5_tx_metrics.buffers_ready,
                obj.v5_tx_metrics.buffers_total,
            )

        if obj.v5_task_metrics is not None:
            if self._cputime_prev_data is not None:
                old_uptime, old_task_metrics = self._cputime_prev_data
                dt = (obj.uptime - old_uptime) % (2**16)
            else:
                dt = 1
                old_task_metrics = obj.v5_task_metrics

            self.logger.debug(
                "status: "
                "cpu metrics: "
                "idle ticks: %5d (%5.1f%%)",
                obj.v5_task_metrics.idle_ticks,
                ((obj.v5_task_metrics.idle_ticks -
                  old_task_metrics.idle_ticks) % (2**16)) / dt * 100

            )
            for i, (task, old_task) in enumerate(
                    zip(obj.v5_task_metrics.tasks,
                        old_task_metrics.tasks)):
                self.logger.debug(
                    "status: "
                    "cpu metrics: "
                    "  task[%2d]: %5d (%5.1f%%)",
                    i,
                    task.cpu_ticks,
                    ((task.cpu_ticks - old_task.cpu_ticks) % (2**16)) / dt * 100
                )

            self._cputime_prev_data = (
                obj.uptime,
                obj.v5_task_metrics,
            )

        if obj.v6_cpu_metrics is not None:
            self.logger.debug(
                "status: "
                "cpu metrics: "
                "idle : %5d",
                obj.v6_cpu_metrics.idle,
            )

            self.logger.debug(
                "status: "
                "cpu metrics: "
                "sched: %5d",
                obj.v6_cpu_metrics.sched,
            )

            max_len = max(map(len, obj.v6_cpu_metrics.interrupts.keys()))

            for intr, hits in sorted(obj.v6_cpu_metrics.interrupts.items()):
                self.logger.debug(
                    "status: "
                    "cpu metrics: "
                    "intr[%{}s]: %5d".format(max_len),
                    intr,
                    hits,
                )

            for i, hits in enumerate(obj.v6_cpu_metrics.tasks):
                self.logger.debug(
                    "status: "
                    "cpu metrics: "
                    "task[%2d]: %5d",
                    i,
                    hits,
                )

    def _process_non_status_message(self, obj):
        if hasattr(obj, "get_samples"):
            batches = list(map(
                self._batch_rewriter.rewrite,
                batch_samples(
                    rtcify_samples(
                        map(
                            self._indivdual_rewriter.rewrite,
                            deenumify_samples(obj.get_samples())
                        ),
                        self._rtcifier
                    )
                )
            ))
            self._enqueue_sample_batches(batches)
            # for ts, bare_path, samples in :
            #     print(
            #         "{} sensor={}".format(
            #             self._rtcifier.map_to_rtc(ts),
            #             bare_path,
            #         ),
            #         end="\n" if len(samples) > 1 else ""
            #     )
            #     for subpart, value in samples.items():
            #         if subpart is None:
            #             print("  value={}".format(value))
            #         else:
            #             print("  subpart={} value={}".format(
            #                 subpart,
            #                 value,
            #             ))

        elif isinstance(obj, protocol.SensorStreamMessage):
            stream_buffer = self._stream_buffers[
                obj.path
            ]
            stream_buffer.submit(
                obj.seq,
                obj.data
            )

    def _on_message(self, obj):
        # print(obj)

        if obj.type_ == protocol.MsgType.STATUS:
            now = datetime.utcnow()
            retardation = now - obj.rtc
            if retardation > timedelta(seconds=60):
                # suspicious, discard
                self.logger.debug(
                    "discarding status package which is late by %s",
                    retardation
                )
                return

            self._had_status = True
            self._rtcifier.align(
                obj.rtc,
                obj.uptime
            )

            for subpart, state in zip(["accel", "compass"],
                                      [obj.v1_accel_stream_state,
                                       obj.v1_compass_stream_state]):
                seq = state.sequence_number
                rtc = self._rtcifier.map_to_rtc(state.timestamp)
                period = state.period
                for axis in "xyz":
                    stream_buffer = self._stream_buffers[
                        sample.SensorPath(
                            sample.Part.LSM303D,
                            0,
                            sample.LSM303DSubpart("{}-{}".format(subpart, axis))
                        )
                    ]
                    stream_buffer.align(
                        seq,
                        rtc,
                        period,
                    )

            self._print_status(obj, now)

        if self._had_status:
            if self.__pre_status_buffer:
                for msg in self.__pre_status_buffer:
                    self._process_non_status_message(msg)
                self.__pre_status_buffer.clear()
            self._process_non_status_message(obj)
        else:
            self.__pre_status_buffer.append(obj)

    def _task_failed(self, task):
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except:  # NOQA
            self.logger.exception("%r failed", task)

    async def _detect_and_configure(
            self,
            ctrl_remote_address,
            ctrl_timeout,
            local_address,
            sntp_server):
        remote_addr, (dest_addr, sntp_addr), rtt = \
            await self._control_protocol.detect(
                ctrl_remote_address,
                timeout=ctrl_timeout,
            )

        self.logger.debug(
            "found ESP at %s (rtt/2 = %s)",
            remote_addr,
            timedelta(seconds=rtt/2),
        )

        if (dest_addr != local_address or
                (sntp_server is not None and
                 sntp_server != sntp_addr)):
            self.logger.info("ESP needs re-configuration")
            await self._control_protocol.configure(
                remote_addr,
                local_address,
                sntp_server or sntp_addr,
                timeout=ctrl_timeout,
            )

    async def run(self):
        interval = dig(self.__config, 'net', 'detect', 'interval', default=5)
        local_address = self.__config['net']['local_address']
        sntp_server = dig(self.__config, 'net', 'config', 'sntp_server',
                          default=None)
        ctrl_remote_address = dig(
            self.__config, 'net', 'detect', 'remote_address',
            default='255.255.255.255')
        ctrl_timeout = dig(
            self.__config, 'net', 'detect', 'timeout',
            default=5)

        async with self.__xmpp:
            await self.__loop.create_datagram_endpoint(
                self._make_protocol,
                local_addr=(
                    local_address,
                    7284,
                ),
            )

            await self.__loop.create_datagram_endpoint(
                self._make_control_protocol,
                local_addr=(
                    dig(self.__config, 'net', 'detect', 'local_address',
                        default="0.0.0.0"),
                    dig(self.__config, 'net', 'detect', 'local_port',
                        default=0),
                ),
            )

            while True:
                try:
                    await self._detect_and_configure(
                        ctrl_remote_address,
                        ctrl_timeout,
                        local_address,
                        sntp_server,
                    )
                except TimeoutError:
                    self.logger.info(
                        "cannot find ESP currently, will re-try in %d seconds",
                        interval,
                    )
                await asyncio.sleep(interval)
