import asyncio
import binascii
import collections
import logging
import math
import struct
import time

from datetime import datetime, timedelta
from enum import Enum

import aioxmpp.callbacks

from . import sensor_stream, bme280
from hintlib.utils import unpack_and_splice, unpack_all
from hintlib import sample

from _sn2d_comm import lib


class MsgType(Enum):
    STATUS = lib.STATUS
    SENSOR_DS18B20 = lib.SENSOR_DS18B20
    SENSOR_LIGHT = lib.SENSOR_LIGHT
    SENSOR_NOISE = lib.SENSOR_NOISE
    SENSOR_BME280 = lib.SENSOR_BME280
    SENSOR_STREAM_ACCEL_X = lib.SENSOR_STREAM_ACCEL_X
    SENSOR_STREAM_ACCEL_Y = lib.SENSOR_STREAM_ACCEL_Y
    SENSOR_STREAM_ACCEL_Z = lib.SENSOR_STREAM_ACCEL_Z
    SENSOR_STREAM_COMPASS_X = lib.SENSOR_STREAM_COMPASS_X
    SENSOR_STREAM_COMPASS_Y = lib.SENSOR_STREAM_COMPASS_Y
    SENSOR_STREAM_COMPASS_Z = lib.SENSOR_STREAM_COMPASS_Z


msgtype_to_ctype = {
    MsgType.STATUS: "struct sbx_msg_status_t*",
    MsgType.SENSOR_DS18B20: "struct sbx_msg_ds18b20_t*",
    MsgType.SENSOR_LIGHT: "struct sbx_msg_light_t*",
    MsgType.SENSOR_NOISE: "struct sbx_msg_noise_t*",
    MsgType.SENSOR_STREAM_ACCEL_X: "struct sbx_msg_sensor_stream_t*",
    MsgType.SENSOR_STREAM_ACCEL_Y: "struct sbx_msg_sensor_stream_t*",
    MsgType.SENSOR_STREAM_ACCEL_Z: "struct sbx_msg_sensor_stream_t*",
    MsgType.SENSOR_STREAM_COMPASS_X: "struct sbx_msg_sensor_stream_t*",
    MsgType.SENSOR_STREAM_COMPASS_Y: "struct sbx_msg_sensor_stream_t*",
    MsgType.SENSOR_STREAM_COMPASS_Z: "struct sbx_msg_sensor_stream_t*",
}


class StatusMessage:
    rtc = None
    uptime = None
    v1_accel_stream_state = None
    v1_compass_stream_state = None
    v2_i2c_metrics = None
    v5_tx_metrics = None
    v5_task_metrics = None
    v6_cpu_metrics = None

    class I2CMetrics:
        transaction_overruns = None

        _v2 = struct.Struct(
            "<"
            "H"
        )

        @classmethod
        def unpack_and_splice(cls, version, buf):
            result = cls()
            buf, (result.transaction_overruns,) = unpack_and_splice(
                buf,
                cls._v2
            )
            return buf, result

    class BME280Metrics:
        configure_status = 0xff
        timeouts = 0

        _v2 = struct.Struct(
            "<"
            "H"
        )

        _v3 = struct.Struct(
            "<"
            "BH"
        )

        @classmethod
        def unpack_and_splice(cls, version, buf):
            result = cls()
            if version < 3:
                buf, (result.timeouts,) = unpack_and_splice(
                    buf,
                    cls._v2
                )
                result.configure_status = 0x00
            else:
                buf, (result.configure_status, result.timeouts,) = \
                    unpack_and_splice(
                        buf,
                        cls._v3
                    )
            return buf, result

    class IMUStreamState(collections.namedtuple(
            "_IMUStreamState",
            ["sequence_number", "timestamp", "period"])):

        _v1 = struct.Struct(
            "<"
            "HHH"
        )

        @classmethod
        def unpack_and_splice(cls, version, buf):
            buf, (seq, ts, period) = unpack_and_splice(
                buf,
                cls._v1
            )
            period = timedelta(milliseconds=period)
            return buf, cls(seq, ts, period)

    class TXMetrics(collections.namedtuple(
            "_TXState",
            ["most_buffers_allocated",
             "buffers_allocated",
             "buffers_ready",
             "buffers_total"])):

        _v1 = struct.Struct(
            "<"
            "HHHH"
        )

        @classmethod
        def unpack_and_splice(cls, version, buf):
            buf, (most_buffers_allocated,
                  buffers_allocated,
                  buffers_ready,
                  buffers_total) = unpack_and_splice(
                buf,
                cls._v1,
            )

            return buf, cls(most_buffers_allocated,
                            buffers_allocated,
                            buffers_ready,
                            buffers_total)

    class TasksMetrics(collections.namedtuple(
            "_TasksMetrics",
            ["idle_ticks", "tasks"])):

        class TaskMetrics(collections.namedtuple(
            "_TaskMetrics",
            ["cpu_ticks"])):

            _v1 = struct.Struct(
                "<"
                "H"
            )

            @classmethod
            def unpack_and_splice(cls, version, buf):
                buf, (cpu_ticks,) = unpack_and_splice(
                    buf,
                    cls._v1,
                )
                return buf, cls(cpu_ticks)

        _v1 = struct.Struct(
            "<"
            "BH"
        )

        @classmethod
        def unpack_and_splice(cls, version, buf):
            buf, (count, idle_ticks) = unpack_and_splice(
                buf,
                cls._v1,
            )

            tasks = []
            for i in range(count):
                buf, task = cls.TaskMetrics.unpack_and_splice(
                    version,
                    buf,
                )
                tasks.append(task)

            return buf, cls(idle_ticks, tuple(tasks))

    class CPUMetrics(collections.namedtuple(
            "_CPUMetrics",
            [
                "idle",
                "sched",
                "interrupts",
                "tasks",
            ])):

        _v1 = struct.Struct(
            "<" +
            "H"*0x20
        )

        INTERRUPT_MAP = {
            getattr(lib, key): key[9:].lower()
            for key in dir(lib)
            if key.startswith("CPU_INTR_")
        }

        @classmethod
        def unpack_and_splice(cls, version, buf):
            buf, (*data,) = unpack_and_splice(
                buf,
                cls._v1,
            )

            idle = data[lib.CPU_IDLE]
            sched = data[lib.CPU_SCHED]
            interrupts = {
                name: data[index]
                for index, name in cls.INTERRUPT_MAP.items()
            }
            tasks = data[lib.CPU_TASK_BASE:]

            return buf, cls(idle, sched, interrupts, tasks)

    _base_header = struct.Struct(
        "<"
        "LHBB"
    )

    _v1_stream_state = struct.Struct(
        "<"
        "HHH"
    )

    @classmethod
    def from_buf(cls, type_, buf):
        result = cls()
        result.type_ = type_
        buf, (rtc,
              uptime,
              protocol_version,
              status_version) = unpack_and_splice(
                  buf,
                  cls._base_header,
              )

        if protocol_version != 1:
            raise ValueError("unsupported protocol")

        if status_version > 6:
            raise ValueError("unsupported status version")

        result.rtc = datetime.utcfromtimestamp(rtc)
        result.uptime = uptime
        if 1 <= status_version:
            buf, result.v1_accel_stream_state = \
                cls.IMUStreamState.unpack_and_splice(status_version, buf)
            buf, result.v1_compass_stream_state = \
                cls.IMUStreamState.unpack_and_splice(status_version, buf)

        if 2 <= status_version:
            result.v2_i2c_metrics = []
            for i2c_bus_no in range(2):
                buf, metrics = cls.I2CMetrics.unpack_and_splice(
                    status_version,
                    buf,
                )
                result.v2_i2c_metrics.append(metrics)

            if status_version >= 4:
                result.v4_bme280_metrics = []
                buf, metrics = \
                    cls.BME280Metrics.unpack_and_splice(
                        status_version,
                        buf,
                    )
                result.v4_bme280_metrics.append(metrics)

                buf, metrics = \
                    cls.BME280Metrics.unpack_and_splice(
                        status_version,
                        buf,
                    )
                result.v4_bme280_metrics.append(metrics)

                result.v2_bme280_metrics = result.v4_bme280_metrics[0]
            else:
                buf, result.v2_bme280_metrics = \
                    cls.BME280Metrics.unpack_and_splice(
                        status_version,
                        buf,
                    )
                result.v4_bme280_metrics = [
                    result.v2_bme280_metrics,
                    cls.BME280Metrics(),
                ]

        if 5 <= status_version:
            buf, result.v5_tx_metrics = cls.TXMetrics.unpack_and_splice(
                status_version,
                buf,
            )

        if 5 <= status_version < 6:
            buf, result.v5_task_metrics = cls.TasksMetrics.unpack_and_splice(
                status_version,
                buf,
            )

        if 6 <= status_version:
            buf, result.v6_cpu_metrics = cls.CPUMetrics.unpack_and_splice(
                status_version,
                buf,
            )

        return result

    def __repr__(self):
        return "<{}.{} rtc={} uptime={} at 0x{:x}>".format(
            __name__,
            type(self).__qualname__,
            self.rtc,
            self.uptime,
            id(self),
        )


class DS18B20Message:
    timestamp = None
    samples = None

    _header = struct.Struct(
        "<"
        "H"
    )

    _sample = struct.Struct(
        "<"
        "8sh"
    )

    def __init__(self, timestamp, samples, type_=MsgType.SENSOR_DS18B20):
        super().__init__()
        self.type_ = type_
        self.timestamp = timestamp
        self.samples = list(samples)

    @classmethod
    def from_buf(cls, type_, buf):
        buf, (timestamp,) = unpack_and_splice(
            buf,
            cls._header,
        )

        return cls(
            timestamp,
            (
                (id_, value/16)
                for id_, value in unpack_all(buf, cls._sample)
            ),
            type_=type_,
        )

    def __repr__(self):
        return "<{}.{} timestamp={} samples={} at 0x{:x}>".format(
            __name__,
            type(self).__qualname__,
            self.timestamp,
            self.samples,
            id(self),
        )

    def get_samples(self):
        for id_, value in self.samples:
            yield sample.Sample(
                self.timestamp,
                sample.SensorPath(
                    sample.Part.DS18B20,
                    binascii.b2a_hex(id_).decode(),
                ),
                value,
            )


class NoiseMessage:
    samples = None

    _header = struct.Struct(
        "<"
        "B"
    )

    _sample = struct.Struct(
        "<"
        "HLhh"
    )

    _sensor_path = sample.SensorPath(
        sample.Part.CUSTOM_NOISE,
        0,
    )

    def __init__(self, samples, type_=MsgType.SENSOR_NOISE):
        super().__init__()
        self.type_ = type_
        self.samples = list(samples)

    @classmethod
    def from_buf(cls, type_, buf):
        buf, (factor,) = unpack_and_splice(buf, cls._header)
        return cls(
            [
                (ts, sqavg / (2**24-1) / factor, min_, max_)
                for ts, sqavg, min_, max_
                in unpack_all(buf, cls._sample)
            ],
            type_=type_
        )

    def __repr__(self):
        return "<{}.{} samples={} at 0x{:x}>".format(
            __name__,
            type(self).__qualname__,
            self.samples,
            id(self),
        )

    def get_samples(self):
        for ts, sqavg, min_, max_ in self.samples:
            rms = math.sqrt(sqavg)
            yield sample.Sample(
                ts,
                self._sensor_path.replace(
                    subpart=sample.CustomNoiseSubpart.RMS
                ),
                rms,
            )
            yield sample.Sample(
                ts,
                self._sensor_path.replace(
                    subpart=sample.CustomNoiseSubpart.MIN
                ),
                min_ / (2**15-1)
            )
            yield sample.Sample(
                ts,
                self._sensor_path.replace(
                    subpart=sample.CustomNoiseSubpart.MAX
                ),
                max_ / (2**15-1)
            )


class LightMessage:
    samples = None

    _sample = struct.Struct(
        "<"
        "H4H"
    )

    _ch_parts = [
        sample.TCS3200Subpart.RED,
        sample.TCS3200Subpart.GREEN,
        sample.TCS3200Subpart.BLUE,
        sample.TCS3200Subpart.CLEAR,
    ]

    def __init__(self, samples, type_=MsgType.SENSOR_LIGHT):
        super().__init__()
        self.type_ = type_
        self.samples = list(samples)

    @classmethod
    def from_buf(cls, type_, buf):
        return cls(
            (
                (timestamp, tuple(values))
                for timestamp, *values in unpack_all(buf, cls._sample)
            ),
            type_=type_,
        )

    def __repr__(self):
        return "<{}.{} samples={} at 0x{:x}>".format(
            __name__,
            type(self).__qualname__,
            self.samples,
            id(self),
        )

    def get_samples(self):
        for ts, channels in self.samples:
            for ch_i, ch_subpart in enumerate(self._ch_parts):
                yield sample.Sample(
                    ts,
                    sample.SensorPath(
                        sample.Part.TCS3200,
                        0,
                        ch_subpart,
                    ),
                    channels[ch_i]
                )


class BME280Message:
    timestamp = None
    instance = None
    temperature = None
    pressure = None
    humidity = None

    _message = struct.Struct(
        "<"
        "HB26s7s8s"
    )

    def __init__(self, timestamp, temperature, pressure, humidity,
                 type_=MsgType.SENSOR_BME280,
                 instance=0):
        super().__init__()
        self.type_ = type_
        self.timestamp = timestamp
        self.instance = instance
        self.temperature = temperature
        self.pressure = pressure
        self.humidity = humidity

    @classmethod
    def from_buf(cls, type_, buf):
        buf, (timestamp,
              instance,
              dig88,
              dige1,
              readout) = unpack_and_splice(buf, cls._message)
        if buf:
            raise ValueError("too much data in buffer")

        calibration = bme280.get_calibration(dig88, dige1)
        temp_raw, pressure_raw, humidity_raw = bme280.get_readout(readout)

        temperature = bme280.compensate_temperature(
            calibration,
            temp_raw,
        )

        pressure = bme280.compensate_pressure(
            calibration,
            pressure_raw,
            temperature,
        )

        humidity = bme280.compensate_humidity(
            calibration,
            humidity_raw,
            temperature,
        )

        return cls(
            timestamp,
            temperature,
            pressure,
            humidity,
            type_=type_,
            instance=instance,
        )

    def __repr__(self):
        return "<{}.{} temperature={} pressure={} humidity={} at 0x{:x}>".format(
            __name__,
            type(self).__qualname__,
            self.temperature,
            self.pressure,
            self.humidity,
            id(self),
        )

    def get_samples(self):
        yield sample.Sample(
            self.timestamp,
            sample.SensorPath(
                sample.Part.BME280,
                self.instance,
                sample.BME280Subpart.TEMPERATURE,
            ),
            self.temperature,
        )

        yield sample.Sample(
            self.timestamp,
            sample.SensorPath(
                sample.Part.BME280,
                self.instance,
                sample.BME280Subpart.PRESSURE,
            ),
            self.pressure,
        )

        yield sample.Sample(
            self.timestamp,
            sample.SensorPath(
                sample.Part.BME280,
                self.instance,
                sample.BME280Subpart.HUMIDITY,
            ),
            self.humidity,
        )


class SensorStreamMessage:
    seq = None
    data = None

    _header = struct.Struct(
        "<"
        "HH"
    )

    _partmap = {
        MsgType.SENSOR_STREAM_ACCEL_X: sample.LSM303DSubpart.ACCEL_X,
        MsgType.SENSOR_STREAM_ACCEL_Y: sample.LSM303DSubpart.ACCEL_Y,
        MsgType.SENSOR_STREAM_ACCEL_Z: sample.LSM303DSubpart.ACCEL_Z,
        MsgType.SENSOR_STREAM_COMPASS_X: sample.LSM303DSubpart.COMPASS_X,
        MsgType.SENSOR_STREAM_COMPASS_Y: sample.LSM303DSubpart.COMPASS_Y,
        MsgType.SENSOR_STREAM_COMPASS_Z: sample.LSM303DSubpart.COMPASS_Z,
    }

    def __init__(self, type_, seq, data):
        super().__init__()
        self.type_ = type_
        self.seq = seq
        self.data = data

    @classmethod
    def from_buf(cls, type_, buf):
        buf, (seq, reference) = unpack_and_splice(
            buf,
            cls._header,
        )
        data = sensor_stream.decompress(
            reference,
            buf
        )
        return cls(
            type_,
            seq,
            data,
        )

    def __repr__(self):
        return (
            "<{}.{} "
            "type={} seq={} data={} "
            "at 0x{:x}>".format(
                __name__,
                type(self).__qualname__,
                self.type_,
                self.seq,
                self.data,
                id(self),
            )
        )

    @property
    def path(self):
        return sample.SensorPath(
            sample.Part.LSM303D,
            0,
            self._partmap[self.type_]
        )


msgtype_to_cls = {
    MsgType.STATUS: StatusMessage,
    MsgType.SENSOR_DS18B20: DS18B20Message,
    MsgType.SENSOR_NOISE: NoiseMessage,
    MsgType.SENSOR_LIGHT: LightMessage,
    MsgType.SENSOR_BME280: BME280Message,
    MsgType.SENSOR_STREAM_ACCEL_X: SensorStreamMessage,
    MsgType.SENSOR_STREAM_ACCEL_Y: SensorStreamMessage,
    MsgType.SENSOR_STREAM_ACCEL_Z: SensorStreamMessage,
    MsgType.SENSOR_STREAM_COMPASS_X: SensorStreamMessage,
    MsgType.SENSOR_STREAM_COMPASS_Y: SensorStreamMessage,
    MsgType.SENSOR_STREAM_COMPASS_Z: SensorStreamMessage,
}


def decode_message(buf):
    try:
        type_ = MsgType(buf[0])
    except ValueError:
        raise ValueError("unknown message type: 0x{:02x}".format(buf[0]))

    try:
        cls = msgtype_to_cls[type_]
    except KeyError:
        raise ValueError("no handler for message: {}".format(type_))

    return cls.from_buf(type_, buf[1:])


class SensorNode2Protocol(asyncio.Protocol):
    on_message_received = aioxmpp.callbacks.Signal()

    class State(Enum):
        LENGTH = 0
        PAYLOAD = 1

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(
            ".".join(
                [__name__, type(self).__qualname__]
            )
        )

        self._buffer = bytearray()
        self._length = None
        self._state = self.State.LENGTH
        self.acceptable_sources = set()
        self._last_data_ts = None
        self._transport = None

    @property
    def last_message_ts(self):
        return self._last_data_ts

    @property
    def connected(self):
        return self._transport is not None

    def _push(self, buf):
        self._last_data_ts = time.monotonic()
        self._buffer.extend(buf)

    def _process(self):
        if self._state == self.State.LENGTH:
            if len(self._buffer) < 2:
                return False
            self._length = int.from_bytes(self._buffer[:2], "little")
            del self._buffer[:2]
            self._state = self.State.PAYLOAD
            return True
        elif self._state == self.State.PAYLOAD:
            if len(self._buffer) < self._length:
                return False

            view = memoryview(self._buffer)[:self._length]
            type_ = view[0]

            try:
                obj = decode_message(view)
            except Exception as exc:
                self.logger.warning("failed to decode message: %r",
                                    bytes(view),
                                    exc_info=True)
                obj = None
            finally:
                del view
                del self._buffer[:self._length]

            if obj is None:
                self.logger.warning("failed to decode message. type=0x%02x",
                                    type_)
            else:
                self.on_message_received(obj)

            self._state = self.State.LENGTH
            return True
        else:
            raise RuntimeError("invalid state")

    def data_received(self, buf):
        self._push(buf)
        while self._process():
            pass

    def connection_made(self, transport):
        peername = transport.get_extra_info("peername")
        if peername[0] not in self.acceptable_sources:
            self.logger.warning(
                "rejecting inbound connection from %s as it is not a valid "
                "source address (acceptable are %r)",
                peername[0],
                self.acceptable_sources
            )
            transport.abort()
            transport.close()
            return

        if self._transport is not None:
            self.logger.warning(
                "a transport is already connected, "
                "replacing with new"
            )
            self._transport.abort()
            self._transport.close()
            return

        # write EOF immediately -- we won’t be sending, ever
        try:
            transport.write_eof()
        except NotImplementedError:
            pass

        self._transport = transport
        self.logger.debug("connected via %r to %s",
                          self._transport,
                          peername)

    def connection_lost(self, exc):
        self.logger.warning("lost connection: %s", exc)
