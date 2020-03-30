import array
import asyncio
import contextlib
import bz2
import functools
import logging
import math
import pathlib
import time
import typing

from datetime import datetime, timedelta

import aioxmpp

import hintlib.core
import hintlib.services
import hintlib.xso

from . import sbx_protocol, datagram_stream, sensor_stream, sink
from hintlib import utils, rewrite, sample, timeline


def dig(mapping, *args, default=None):
    for step in args:
        try:
            mapping = mapping[step]
        except KeyError:
            return default
    return mapping


def rtcify_samples(
        samples: typing.Iterable[sample.RawSample],
        rtcifier: timeline.RTCifier
        ) -> typing.Iterable[sample.Sample]:
    for s in samples:
        if isinstance(s.timestamp, datetime):
            yield sample.Sample(
                timestamp=s.timestamp,
                sensor=s.sensor,
                value=s.value,
            )
        else:
            yield sample.Sample(
                timestamp=rtcifier.map_to_rtc(s.timestamp),
                sensor=s.sensor,
                value=s.value,
            )


def deenumify_samples(samples):
    for s in samples:
        yield s.replace(
            sensor=s.sensor.replace(
                part=s.sensor.part.value,
                subpart=s.sensor.subpart.value if s.sensor.subpart else None,
            )
        )


def batch_samples(
        samples: typing.Iterable[sample.Sample]
        ) -> typing.Iterable[sample.SampleBatch]:
    curr_batch_ts = None
    curr_batch_bare_path = None
    curr_batch_samples = {}
    for s in samples:
        bare_path = s.sensor.replace(subpart=None)
        if (curr_batch_ts != s.timestamp or
                curr_batch_bare_path != bare_path):
            if curr_batch_samples:
                yield sample.SampleBatch(
                    timestamp=curr_batch_ts,
                    bare_path=curr_batch_bare_path,
                    samples=curr_batch_samples,
                )
                curr_batch_samples = {}

            curr_batch_ts = s.timestamp
            curr_batch_bare_path = bare_path

        if s.sensor.subpart in curr_batch_samples:
            raise RuntimeError

        curr_batch_samples[s.sensor.subpart] = s.value

    if curr_batch_samples:
        yield sample.SampleBatch(
            timestamp=curr_batch_ts,
            bare_path=curr_batch_bare_path,
            samples=curr_batch_samples,
        )


def configure_client(xmpp_cfg, logger) -> hintlib.core.BotCore:
    core = hintlib.core.BotCore(
        xmpp_cfg,
        client_logger=logger,
    )
    core.client.summon(aioxmpp.PresenceClient)
    return core


def configure_mc_sink(sink_cfg, core: hintlib.core.BotCore) -> sink.Sink:
    if not core.sn2d_has_metric_collector:
        raise ValueError("client for sink has no metric collector destination")

    service = core.client.summon(hintlib.services.BatchSubmitterService)
    service.queue_size = sink_cfg.get("queue_size", 16)
    service.module_name = sink_cfg["module_name"]

    return sink.MetricCollectorSink(service)


def configure_pubsub_sink(sink_cfg, core: hintlib.core.BotCore) -> sink.Sink:
    client = core.client.summon(aioxmpp.PubSubClient)
    return sink.PubSubSink(
        client,
        aioxmpp.JID.fromstr(sink_cfg["service"]),
        queue_size=sink_cfg.get("queue_size", 16),
    )


def configure_sink(sink_cfg, core: hintlib.core.BotCore) -> sink.Sink:
    protocol = sink_cfg["protocol"]
    if protocol == "metric-collector":
        return configure_mc_sink(sink_cfg.get("metric-collector", {}), core)
    elif protocol == "pubsub":
        return configure_pubsub_sink(sink_cfg["pubsub"], core)
    else:
        raise ValueError("unknown sink protocol: {!r}".format(protocol))


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

        self.__xmpp_clients = {}
        stream_client = None
        for name, cfg in config["xmpp"].items():
            client = configure_client(
                cfg,
                self.logger.getChild("xmpp").getChild(name)
            )
            self.__xmpp_clients[name] = client

            if "metric-collector" in cfg:
                sender = client.client.summon(
                    hintlib.services.SenderService
                )
                sender.peer_jid = aioxmpp.JID.fromstr(cfg["metric-collector"])
                if stream_client is None:
                    stream_client = client
                client.sn2d_has_metric_collector = True
            else:
                client.sn2d_has_metric_collector = False

        self._sample_sinks = []
        for sink_cfg in config["sinks"]:
            client_name = sink_cfg["via"]
            client = self.__xmpp_clients[client_name]
            self._sample_sinks.append(configure_sink(sink_cfg, client))

        if stream_client is None:
            raise ValueError("no metric-collector client defined")

        self._stream_service = \
            stream_client.client.summon(hintlib.services.StreamSubmitterService)
        self._stream_service.queue_size = \
            config.get("streams", {}).get("queue_length", 16)
        self._stream_service.module_name = config["sensors"]["module_name"]

        self._timeline = timeline.Timeline(
            2**16,  # wraparound
            30000,  # 30s slack
        )
        self._rtcifier = timeline.RTCifier(
            self._timeline,
            self.logger.getChild("rtcifier")
        )
        self._had_status = False

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
        range_ = self._stream_ranges.get(
            (path.part, path.subpart), 1
        )
        item = path, t0, seq0, period, data, range_, handle
        self._stream_service.submit_block(item)

    def _enqueue_sample_batches(self, batches):
        for sink in self._sample_sinks:
            sink.submit_batches(batches)

    def _print_status(self, rtc_timestamp, obj, now):
        self.logger.debug(
            "status: "
            "rtc = %s, uptime = %d",
            rtc_timestamp,
            obj.uptime,
        )
        self.logger.debug(
            "status: "
            "rtc offset = %s, rtcified uptime offset = %s",
            abs(now - rtc_timestamp),
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

        elif isinstance(obj, sbx_protocol.SensorStreamMessage):
            stream_buffer = self._stream_buffers[
                obj.path
            ]
            stream_buffer.submit(
                obj.seq,
                obj.data
            )

    def _on_message(self, rtc_timestamp, obj):
        # print(obj)

        if obj.type_ == sbx_protocol.MsgType.STATUS:
            now = datetime.utcnow()
            retardation = now - rtc_timestamp
            if retardation > timedelta(seconds=60):
                # suspicious, discard
                self.logger.debug(
                    "discarding status package which is late by %s",
                    retardation
                )
                return

            self._had_status = True
            self._rtcifier.align(
                rtc_timestamp,
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

            self._print_status(rtc_timestamp, obj, now)

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

    async def run(self):
        interval = dig(self.__config, 'net', 'detect', 'interval', default=5)
        sntp_server = dig(self.__config, 'net', 'config', 'sntp_server',
                          default=None)
        ctrl_remote_address = dig(
            self.__config, 'net', 'detect', 'remote_address',
            default='255.255.255.255')
        ctrl_timeout = dig(
            self.__config, 'net', 'detect', 'timeout',
            default=5)

        protocol = datagram_stream.DatagramStreamProtocol(
            sbx_protocol.SENDER_PORT,
        )

        client = sbx_protocol.SBXClient(protocol)
        client.on_message.connect(self._on_message)

        def get_protocol():
            return protocol

        async with contextlib.AsyncExitStack() as stack:
            for client in self.__xmpp_clients.values():
                await stack.enter_async_context(client)

            await self.__loop.create_datagram_endpoint(
                get_protocol,
                local_addr=(
                    dig(self.__config, 'net', 'detect', 'local_address',
                        default="0.0.0.0"),
                    dig(self.__config, 'net', 'detect', 'local_port',
                        default=sbx_protocol.RECEIVER_PORT),
                ),
            )

            while True:
                await asyncio.sleep(interval)
