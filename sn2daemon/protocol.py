import asyncio
import logging
import struct

from enum import Enum

from . import sensor_stream

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


def unpack_and_splice(buf, struct_obj):
    result = buf[struct_obj.size:]
    return result, struct_obj.unpack(buf[:struct_obj.size])


def unpack_all(buf, struct_obj):
    size = struct_obj.size
    if len(buf) % size != 0:
        raise ValueError(
            "buffer does not contain an integer number of structs"
        )
    return (
        struct_obj.unpack(buf[i*size:(i+1)*size])
        for i in range(len(buf)//size)
    )


class StatusMessage:
    rtc = None
    uptime = None
    v1_accel_stream_state = None
    v1_compass_stream_state = None

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

        if protocol_version != 1 or status_version != 1:
            raise ValueError("unsupported protocol")

        result.rtc = rtc
        result.uptime = uptime
        if status_version == 1:
            buf, result.v1_accel_stream_state = unpack_and_splice(
                buf,
                cls._v1_stream_state,
            )
            buf, result.v1_compass_stream_state = unpack_and_splice(
                buf,
                cls._v1_stream_state,
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

    @classmethod
    def from_buf(cls, type_, buf):
        result = cls()
        result.type_ = type_
        buf, (result.timestamp,) = unpack_and_splice(
            buf,
            cls._header,
        )

        result.samples = [
            (id_, value/16)
            for id_, value in unpack_all(buf, cls._sample)
        ]

        return result

    def __repr__(self):
        return "<{}.{} timestamp={} samples={} at 0x{:x}>".format(
            __name__,
            type(self).__qualname__,
            self.timestamp,
            self.samples,
            id(self),
        )


class NoiseMessage:
    samples = None

    _sample = struct.Struct(
        "<"
        "HH"
    )

    @classmethod
    def from_buf(cls, type_, buf):
        result = cls()
        result.type_ = type_
        result.samples = list(
            unpack_all(buf, cls._sample)
        )
        return result

    def __repr__(self):
        return "<{}.{} samples={} at 0x{:x}>".format(
            __name__,
            type(self).__qualname__,
            self.samples,
            id(self),
        )


class LightMessage:
    samples = None

    _sample = struct.Struct(
        "<"
        "H4H"
    )

    @classmethod
    def from_buf(cls, type_, buf):
        result = cls()
        result.type_ = type_
        result.samples = list(
            (timestamp, tuple(values))
            for timestamp, *values in unpack_all(buf, cls._sample)
        )
        return result

    def __repr__(self):
        return "<{}.{} samples={} at 0x{:x}>".format(
            __name__,
            type(self).__qualname__,
            self.samples,
            id(self),
        )


class BME280Message:
    timestamp = None
    dig88 = None
    dige1 = None
    readout = None

    _message = struct.Struct(
        "<"
        "H26s7s8s"
    )

    @classmethod
    def from_buf(cls, type_, buf):
        result = cls()
        result.type_ = type_
        buf, (result.timestamp,
              result.dig88,
              result.dige1,
              result.readout) = unpack_and_splice(buf, cls._message)
        if buf:
            raise ValueError("too much data in buffer")
        return result

    def __repr__(self):
        return "<{}.{} readout={} at 0x{:x}>".format(
            __name__,
            type(self).__qualname__,
            self.readout,
            id(self),
        )


class SensorStreamMessage:
    seq = None
    data = None

    _header = struct.Struct(
        "<"
        "Hh"
    )

    @classmethod
    def from_buf(cls, type_, buf):
        result = cls()
        result.type_ = type_
        buf, (result.seq, reference) = unpack_and_splice(
            buf,
            cls._header,
        )
        result.data = sensor_stream.decompress(
            reference,
            buf
        )
        return result

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
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(
            ".".join(
                [__name__, type(self).__qualname__]
            )
        )

    def datagram_received(self, buf, addr):
        obj = decode_message(buf)
        if obj is None:
            self.logger.warning("failed to decode message. type=0x%02x", buf[0])
        else:
            self.logger.debug("decoded object: %r", obj)

    def error_received(self, exc):
        pass

    def connection_made(self, transport):
        self.logger.debug("connected via %r", transport)

    def connection_lost(self, exc):
        pass
