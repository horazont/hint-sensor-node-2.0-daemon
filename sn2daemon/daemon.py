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

import botlib

import hintxso.sensor

from . import protocol, sensor_stream, sample, control_protocol
from hintutils import pathutils, timeline


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


class SenderService(aioxmpp.service.Service):
    ORDER_AFTER = [aioxmpp.PresenceClient]

    def __init__(self, client, **kwargs):
        super().__init__(client, **kwargs)
        self.__task_funs = []
        self.__locked_to = None
        self.__lock_event = asyncio.Event()
        self.__presence = self.dependencies[aioxmpp.PresenceClient]
        self.peer_jid = None
        self.__task = asyncio.ensure_future(
            self._supervisor()
        )
        self.__task.add_done_callback(
            self._supervisor_done,
        )

    def _task_done(self, task):
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except:
            self.logger.exception(
                "task crashed"
            )

    def _supervisor_done(self, task):
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except:
            self.logger.exception(
                "supervisor crashed"
            )

    @aioxmpp.service.depsignal(aioxmpp.PresenceClient, "on_available")
    def _on_available(self, full_jid, stanza):
        if stanza.from_.bare() != self.peer_jid:
            return
        self.logger.debug("locked to %s", full_jid)
        self.__locked_to = full_jid
        self.__lock_event.set()

    @aioxmpp.service.depsignal(aioxmpp.PresenceClient, "on_bare_unavailable")
    def _on_bare_unavailable(self, stanza):
        if stanza.from_.bare() != self.peer_jid:
            return
        self.logger.debug("%s went offline, unlocking", stanza.from_.bare())
        self.__locked_to = None
        self.__lock_event.clear()

    async def _wrapper(self, coro):
        try:
            try:
                await coro
            except asyncio.CancelledError:
                return
        except:
            await asyncio.sleep(1)
            raise
        await asyncio.sleep(1)

    async def _manage_tasks(self, tasks):
        self.__lock_event.clear()

        if self.__locked_to:
            # (re-)spawn tasks
            for fun in self.__task_funs:
                item = None
                try:
                    task = tasks[fun]
                except KeyError:
                    pass
                else:
                    if not task.done():
                        continue
                    try:
                        item = task.result()
                    except:
                        pass

                self.logger.debug(
                    "starting %s with %r", fun, item
                )
                task = asyncio.ensure_future(
                    self._wrapper(
                        fun(self.__locked_to, item)
                    )
                )
                task.add_done_callback(
                    self._task_done
                )
                tasks[fun] = task

        ev_fut = asyncio.ensure_future(
            self.__lock_event.wait()
        )
        await asyncio.wait(
            list(tasks.values()) + [ev_fut],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if ev_fut.done():
            ev_fut.result()
        else:
            ev_fut.cancel()

    async def _supervisor(self):
        tasks = {}
        try:
            while True:
                await self._manage_tasks(tasks)
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                await task  # tasks are wrapped

    async def _shutdown(self):
        self.__task.cancel()
        try:
            await self.__task
        except asyncio.CancelledError:
            pass

    def add_task(self, coro_fun):
        self.__task_funs.append(coro_fun)
        self.__lock_event.set()


def _rewrite_instance(rule, logger):
    try:
        old_instance = rule["instance"]
        new_instance = rule["new_instance"]
        part = sample.Part(rule["part"])
    except KeyError as e:
        raise ValueError("rewrite rule needs key {!r}", str(e))
    except ValueError:
        raise ValueError("unknown part: {!r}", rule["part"])

    def do_rewrite_instance(sample_obj):
        if (sample_obj.sensor.part == part and
                sample_obj.sensor.instance == old_instance):
            logger.debug("rewrote %r instance (%r -> %r)",
                         part,
                         old_instance,
                         new_instance)
            return sample_obj.replace(
                sensor=sample_obj.sensor.replace(
                    instance=new_instance
                )
            )
        return sample_obj

    logger.debug("built instance rewrite rule for %r: (%r -> %r)",
                 part,
                 old_instance,
                 new_instance)

    return do_rewrite_instance


def _rewrite_value_scale(rule, logger):
    try:
        part = sample.Part(rule["part"])
        factor = rule["factor"]
        subpart = rule.get("subpart")
    except KeyError as e:
        raise ValueError("rewrite rule needs key {!r}", str(e))
    except ValueError:
        raise ValueError("unknown part: {!r}", rule["part"])

    def do_rewrite_value_scale(sample_obj):
        if (sample_obj.sensor.part == part and
                sample_obj.sensor.subpart == subpart):
            old_value = sample_obj.value
            new_value = old_value * factor
            logger.debug("rewrote %r value (%r -> %r)",
                         part,
                         old_value,
                         new_value)
            return sample_obj.replace(
                value=new_value
            )
        return sample_obj

    logger.debug("built value rewrite rule for %r: multiply with %r",
                 part,
                 factor)

    return do_rewrite_value_scale


class IndividualSampleRewriter:
    def __init__(self, config, logger):
        super().__init__()
        self.logger = logger
        self.logger.debug("compiling individual rewrite rules: %r", config)
        self._rewrite_rules = [
            self._compile_rewrite_rule(rule, logger)
            for rule in config
        ]

    REWRITERS = {
        "instance": _rewrite_instance,
        "value-scale": _rewrite_value_scale,
    }

    def _compile_rewrite_rule(self, rule, logger):
        try:
            rewrite_builder = self.REWRITERS[rule["rewrite"]]
        except KeyError:
            raise ValueError(
                "missing 'rewrite' key in rewrite rule {!r}".format(rule)
            )

        return rewrite_builder(rule, logger)

    def rewrite(self, sample_obj):
        for rule in self._rewrite_rules:
            sample_obj = rule(sample_obj)
        return sample_obj


CALC_REWRITE_GLOBALS = {
    "exp": math.exp,
}


def _rewrite_batch_value(rule, logger):
    part = sample.Part(rule["part"])
    subpart = sample.PART_SUBPARTS[part](rule["subpart"])
    instance = rule.get("instance")
    expression = compile(
        rule["new_value"],
        '<rewrite rule {!r}>'.format(rule),
        'eval'
    )
    constants = rule.get("constants", {})

    globals_ = CALC_REWRITE_GLOBALS.copy()

    def do_rewrite_batch_value(sample_batch):
        ts, bare_path, samples = sample_batch
        if bare_path.part != part:
            return sample_batch
        if instance is not None and bare_path.instance != instance:
            return sample_batch

        locals_ = dict(constants)
        for key, value in samples.items():
            locals_[key.value] = value

        try:
            new_value = eval(expression, globals_, locals_)
        except NameError as exc:
            logger.warning("failed to evaluate rewrite rule",
                           exc_info=True)
            return sample_batch

        new_samples = dict(samples)
        new_samples[subpart] = new_value

        logger.debug("rewrote %r value (%r -> %r)",
                     subpart,
                     samples.get(subpart),
                     new_value)

        return ts, bare_path, new_samples

    return do_rewrite_batch_value


def _rewrite_batch_create(rule, logger):
    part = sample.Part(rule["part"])
    new_subpart = rule["subpart"]
    instance = rule.get("instance")
    expression = compile(
        rule["new_value"],
        '<rewrite rule {!r}>'.format(rule),
        'eval'
    )
    precondition = compile(
        rule.get("precondition", None),
        '<rewrite rule {!r}>'.format(rule),
        'eval'
    )
    constants = rule.get("constants", {})

    globals_ = CALC_REWRITE_GLOBALS.copy()

    def do_rewrite_batch_value(sample_batch):
        ts, bare_path, samples = sample_batch
        if bare_path.part != part:
            return sample_batch
        if instance is not None and bare_path.instance != instance:
            return sample_batch

        locals_ = dict(constants)
        for key, value in samples.items():
            locals_[key.value] = value

        try:
            new_value = eval(precondition, globals_, locals_)
        except NameError as exc:
            logger.warning("failed to evaluate precondition rule",
                           exc_info=True)
            return sample_batch

        try:
            new_value = eval(expression, globals_, locals_)
        except NameError as exc:
            logger.warning("failed to evaluate rewrite rule",
                           exc_info=True)
            return sample_batch

        new_samples = dict(samples)
        new_samples[new_subpart] = new_value

        logger.debug("created %r value (%r -> %r)",
                     new_subpart,
                     new_value)

        return ts, bare_path, new_samples

    return do_rewrite_batch_value


class SampleBatchRewriter:
    def __init__(self, config, logger):
        super().__init__()
        self.logger = logger
        self.logger.debug("compiling batch rewrite rules: %r", config)
        self._rewrite_rules = [
            self._compile_rewrite_batch_rule(rule, logger)
            for rule in config
        ]

    REWRITERS = {
        "value": _rewrite_batch_value,
        "create": _rewrite_batch_create,
    }

    def _compile_rewrite_batch_rule(self, batch_rule, logger):
        try:
            rewrite_builder = self.REWRITERS[batch_rule["rewrite"]]
        except KeyError:
            raise ValueError(
                "missing 'rewrite' key in rewrite rule {!r}".format(rule)
            )

        return rewrite_builder(batch_rule, logger)

    def rewrite(self, sample_batch):
        for rule in self._rewrite_rules:
            sample_batch = rule(sample_batch)
        return sample_batch


class SensorNode2Daemon:
    def __init__(self, args, config, loop):
        super().__init__()
        self.logger = logging.getLogger("sn2d")
        self.__loop = loop
        self.__args = args
        self.__config = config

        self._indivdual_rewriter = IndividualSampleRewriter(
            config["samples"]["rewrite"],
            self.logger.getChild("rewrite").getChild("individual")
        )

        self._batch_rewriter = SampleBatchRewriter(
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

        self.__xmpp = botlib.BotCore(config["xmpp"])
        self.__xmpp.client.summon(aioxmpp.PresenceClient)
        sender = self.__xmpp.client.summon(SenderService)
        sender.peer_jid = aioxmpp.JID.fromstr(config["sink"]["jid"])
        sender.add_task(self._submit_streams_to)
        sender.add_task(self._submit_sample_batches_to)

        self.__stream_queue = asyncio.Queue(
            maxsize=config.get(
                "streams", {}
            ).get(
                "queue_length", 16
            ),
            loop=loop,
        )

        self.__sample_queue = asyncio.Queue(
            maxsize=config.get(
                "samples", {}
            ).get(
                "queue_length", 16
            ),
            loop=loop,
        )

        self._timeline = timeline.Timeline(
            2**16,  # wraparound
            30000,  # 30s slack
        )
        self._rtcifier = timeline.RTCifier(
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
                imu_datadir / pathutils.escape_path(str(path)),
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

    async def _submit_streams_to(self, full_jid, cached_item=None):
        while True:
            if cached_item is None:
                item = await self.__stream_queue.get()
            else:
                item = cached_item
                cached_item = None
            path, t0, seq0, period, data, handle = item

            bin_data = array.array("h", data).tobytes()
            ct0 = time.monotonic()
            bz2_data = await self.__loop.run_in_executor(
                None,
                bz2.compress,
                bin_data
            )
            ct1 = time.monotonic()
            self.logger.debug("sample compression took %.1f ms (%.0f%%)",
                              (ct1-ct0) * 1000,
                              (
                                  (ct1-ct0) /
                                  (period*len(data)).total_seconds()
                              ) * 100)

            payload = hintxso.sensor.Query()
            payload.stream = hintxso.sensor.Stream()
            payload.stream.path = str(path)
            payload.stream.t0 = t0
            payload.stream.period = round(period.total_seconds() * 1e6)
            payload.stream.sample_type = "h"
            payload.stream.data = bz2_data
            payload.stream.seq0 = seq0
            payload.stream.range_ = self._stream_ranges.get(
                (path.part, path.subpart), 1
            )

            iq = aioxmpp.IQ(
                type_=aioxmpp.IQType.SET,
                to=full_jid,
                payload=payload
            )

            self.logger.debug("submitting %d bytes of compressed data to %s",
                              len(bz2_data),
                              full_jid)
            try:
                ct0 = time.monotonic()
                await self.__xmpp.client.stream.send(iq)
                ct1 = time.monotonic()
            except aioxmpp.errors.XMPPError as exc:
                self.logger.warning(
                    "failed to submit data for ts %s (%s)",
                    t0,
                    exc
                )
                await asyncio.sleep(1)
                return item
            else:
                handle.close()
                self.logger.debug("submission took %.1f ms (%.0f%%); "
                                  "remaining queue length: %d",
                                  (ct1-ct0) * 1000,
                                  (
                                      (ct1-ct0) /
                                      (period*len(data)).total_seconds()
                                  ) * 100,
                                  self.__stream_queue.qsize())

    async def _submit_sample_batches_to(self, full_jid, cached_item=None):
        while True:
            if cached_item is None:
                item = await self.__sample_queue.get()
            else:
                item = cached_item
                cached_item = None
            batches = item

            payload = hintxso.sensor.Query()
            payload.sample_batches = hintxso.sensor.SampleBatches()
            payload.sample_batches.module = "sensor-node-2"
            for t0, bare_path, samples in batches:
                batch_xso = hintxso.sensor.SampleBatch()
                batch_xso.timestamp = t0
                batch_xso.bare_path = str(bare_path)
                for subpart, value in samples.items():
                    sample_xso = hintxso.sensor.NumericSample()
                    if subpart is not None:
                        sample_xso.subpart = subpart.value
                    else:
                        sample_xso.subpart = None
                    sample_xso.value = value
                    batch_xso.samples.append(sample_xso)
                payload.sample_batches.batches.append(batch_xso)

            iq = aioxmpp.IQ(
                type_=aioxmpp.IQType.SET,
                to=full_jid,
                payload=payload
            )

            self.logger.debug("submitting %d batches",
                              len(batches))
            try:
                await self.__xmpp.client.stream.send(iq)
            except aioxmpp.errors.XMPPError as exc:
                self.logger.warning(
                    "failed to submit data for ts %s (%s)",
                    t0,
                    exc
                )
                await asyncio.sleep(1)
                return item

    def _on_stream_emit(self, path, t0, seq0, period, data, handle):
        item = path, t0, seq0, period, data, handle
        while True:
            self.logger.debug("enqueue-ing item %s", item[:-2])
            try:
                self.__stream_queue.put_nowait(item)
            except asyncio.QueueFull:
                try:
                    *_, data, handle = self.__stream_queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                else:
                    self.logger.warning(
                        "dropped %d samples due to queue overflow",
                        len(data)
                    )
                    handle.close()
            else:
                break

    def _enqueue_sample_batches(self, batches):
        while True:
            self.logger.debug("enqueue-ing %d batches: %r", len(batches),
                              batches)
            try:
                self.__sample_queue.put_nowait(batches)
            except asyncio.QueueFull:
                try:
                    self.__sample_queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                else:
                    self.logger.warning(
                        "dropped samples due to queue overflow",
                    )
            else:
                break

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
                            obj.get_samples()
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
        except:
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
