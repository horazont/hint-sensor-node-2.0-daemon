import asyncio
import binascii
import collections
import logging
import struct

from datetime import datetime, timedelta
from enum import Enum

import aioxmpp.callbacks

from . import sensor_stream, bme280, sample
from .struct_utils import unpack_and_splice, unpack_all

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
        timeouts = None

        _v2 = struct.Struct(
            "<"
            "H"
        )

        @classmethod
        def unpack_and_splice(cls, version, buf):
            result = cls()
            buf, (result.timeouts,) = unpack_and_splice(
                buf,
                cls._v2
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

        if status_version > 2:
            raise ValueError("unsupported status version")

        result.rtc = datetime.utcfromtimestamp(rtc)
        result.uptime = uptime
        if 1 <= status_version <= 2:
            buf, result.v1_accel_stream_state = \
                cls.IMUStreamState.unpack_and_splice(status_version, buf)
            buf, result.v1_compass_stream_state = \
                cls.IMUStreamState.unpack_and_splice(status_version, buf)

        if 2 <= status_version <= 2:
            result.v2_i2c_metrics = []
            for i2c_bus_no in range(2):
                buf, metrics = cls.I2CMetrics.unpack_and_splice(
                    status_version,
                    buf,
                )
                result.v2_i2c_metrics.append(metrics)

            buf, result.v2_bme280_metrics = \
                cls.BME280Metrics.unpack_and_splice(
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

    _sample = struct.Struct(
        "<"
        "HH"
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
        return cls(
            unpack_all(buf, cls._sample),
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
        for ts, value in self.samples:
            yield sample.Sample(
                ts,
                self._sensor_path,
                value,
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
    temperature = None
    pressure = None
    humidity = None

    _message = struct.Struct(
        "<"
        "H26s7s8s"
    )

    def __init__(self, timestamp, temperature, pressure, humidity,
                 type_=MsgType.SENSOR_BME280):
        super().__init__()
        self.type_ = type_
        self.timestamp = timestamp
        self.temperature = temperature
        self.pressure = pressure
        self.humidity = humidity

    @classmethod
    def from_buf(cls, type_, buf):
        buf, (timestamp,
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
                0,
                sample.BME280Subpart.TEMPERATURE,
            ),
            self.temperature,
        )

        yield sample.Sample(
            self.timestamp,
            sample.SensorPath(
                sample.Part.BME280,
                0,
                sample.BME280Subpart.PRESSURE,
            ),
            self.pressure,
        )

        yield sample.Sample(
            self.timestamp,
            sample.SensorPath(
                sample.Part.BME280,
                0,
                sample.BME280Subpart.HUMIDITY,
            ),
            self.humidity,
        )


class SensorStreamMessage:
    seq = None
    data = None

    _header = struct.Struct(
        "<"
        "Hh"
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
        return None

    try:
        cls = msgtype_to_cls[type_]
    except KeyError:
        return None

    return cls.from_buf(type_, buf[1:])


class SensorNode2Protocol(asyncio.DatagramProtocol):
    on_message_received = aioxmpp.callbacks.Signal()

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(
            ".".join(
                [__name__, type(self).__qualname__]
            )
        )

    def datagram_received(self, buf, addr):
        obj = decode_message(buf)
        if obj is None:
            self.logger.warning("failed to decode message. type=0x%02x",
                                buf[0])
        else:
            self.on_message_received(obj)

    def error_received(self, exc):
        pass

    def connection_made(self, transport):
        self.logger.debug("connected via %r", transport)

    def connection_lost(self, exc):
        pass
