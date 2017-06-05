import contextlib
import os
import unittest
import unittest.mock

import sn2daemon.protocol as protocol

import _sn2d_comm


class TestStatusMessage(unittest.TestCase):
    def test_from_buf(self):
        buf = bytearray(
            _sn2d_comm.ffi.sizeof("uint8_t") +
            _sn2d_comm.ffi.sizeof("struct sbx_msg_status_t")
        )
        struct = _sn2d_comm.ffi.cast(
            "struct sbx_msg_t*",
            _sn2d_comm.ffi.from_buffer(buf)
        )

        struct.type = _sn2d_comm.lib.STATUS
        struct.payload.status.rtc = 12345678
        struct.payload.status.uptime = 12345
        struct.payload.status.protocol_version = 1
        struct.payload.status.status_version = 1
        struct.payload.status.imu.stream_state[0].sequence_number = 12
        struct.payload.status.imu.stream_state[0].timestamp = 123
        struct.payload.status.imu.stream_state[0].period = 5
        struct.payload.status.imu.stream_state[1].sequence_number = 13
        struct.payload.status.imu.stream_state[1].timestamp = 124
        struct.payload.status.imu.stream_state[1].period = 64

        result = protocol.StatusMessage.from_buf(
            unittest.mock.sentinel.type_,
            buf[1:]
        )

        self.assertEqual(
            result.type_,
            unittest.mock.sentinel.type_,
        )

        self.assertIsInstance(
            result,
            protocol.StatusMessage,
        )

        self.assertEqual(
            result.rtc,
            12345678,
        )

        self.assertEqual(
            result.uptime,
            12345,
        )

        self.assertEqual(
            result.v1_accel_stream_state,
            (12, 123, 5)
        )

        self.assertEqual(
            result.v1_compass_stream_state,
            (13, 124, 64)
        )


class TestDS18B20Message(unittest.TestCase):
    def test_from_buf(self):
        buf = bytearray(
            _sn2d_comm.ffi.sizeof("uint8_t") +
            _sn2d_comm.ffi.sizeof("uint16_t") +
            _sn2d_comm.ffi.sizeof("struct sbx_msg_ds18b20_sample_t")*3
        )
        struct = _sn2d_comm.ffi.cast(
            "struct sbx_msg_t*",
            _sn2d_comm.ffi.from_buffer(buf)
        )

        struct.type = _sn2d_comm.lib.SENSOR_DS18B20
        struct.payload.ds18b20.timestamp = 12345
        struct.payload.ds18b20.samples[0].id = b"01234567"
        struct.payload.ds18b20.samples[0].raw_value = 1234
        struct.payload.ds18b20.samples[1].id = b"abcdefgh"
        struct.payload.ds18b20.samples[1].raw_value = 2345
        struct.payload.ds18b20.samples[2].id = b"xyz12345"
        struct.payload.ds18b20.samples[2].raw_value = 65372

        result = protocol.DS18B20Message.from_buf(
            unittest.mock.sentinel.type_,
            buf[1:]
        )

        self.assertEqual(
            result.type_,
            unittest.mock.sentinel.type_,
        )

        self.assertIsInstance(
            result,
            protocol.DS18B20Message,
        )

        self.assertEqual(
            result.timestamp,
            12345,
        )

        self.assertEqual(
            len(result.samples),
            3
        )

        self.assertEqual(
            result.samples[0],
            (
                b"01234567",
                1234/16,
            )
        )

        self.assertEqual(
            result.samples[1],
            (
                b"abcdefgh",
                2345/16,
            )
        )

        self.assertEqual(
            result.samples[2],
            (
                b"xyz12345",
                -10.25
            )
        )


class TestNoiseMessage(unittest.TestCase):
    def test_from_buf(self):
        buf = bytearray(
            _sn2d_comm.ffi.sizeof("uint8_t") +
            _sn2d_comm.ffi.sizeof("struct sbx_msg_noise_sample_t")*10
        )
        struct = _sn2d_comm.ffi.cast(
            "struct sbx_msg_t*",
            _sn2d_comm.ffi.from_buffer(buf)
        )

        struct.type = _sn2d_comm.lib.SENSOR_NOISE
        for i in range(10):
            struct.payload.noise.samples[i].timestamp = i*10
            struct.payload.noise.samples[i].value = i*11

        result = protocol.NoiseMessage.from_buf(
            unittest.mock.sentinel.type_,
            buf[1:]
        )

        self.assertEqual(
            result.type_,
            unittest.mock.sentinel.type_,
        )

        self.assertIsInstance(
            result,
            protocol.NoiseMessage,
        )

        self.assertEqual(
            len(result.samples),
            10
        )

        for i, (ts, value) in enumerate(result.samples):
            self.assertEqual(
                ts,
                i*10,
            )
            self.assertEqual(
                value,
                i*11,
            )


class TestLightMessage(unittest.TestCase):
    def test_from_buf(self):
        buf = bytearray(
            _sn2d_comm.ffi.sizeof("uint8_t") +
            _sn2d_comm.ffi.sizeof("struct sbx_msg_light_sample_t")*4
        )
        struct = _sn2d_comm.ffi.cast(
            "struct sbx_msg_t*",
            _sn2d_comm.ffi.from_buffer(buf)
        )

        struct.type = _sn2d_comm.lib.SENSOR_LIGHT
        for i in range(4):
            struct.payload.light.samples[i].timestamp = i*1000
            for c in range(4):
                struct.payload.light.samples[i].ch[c] = i*100+c*4

        result = protocol.LightMessage.from_buf(
            unittest.mock.sentinel.type_,
            buf[1:]
        )

        self.assertEqual(
            result.type_,
            unittest.mock.sentinel.type_,
        )

        self.assertIsInstance(
            result,
            protocol.LightMessage,
        )

        self.assertEqual(
            len(result.samples),
            4
        )

        for i, (ts, values) in enumerate(result.samples):
            self.assertEqual(
                ts, i*1000
            )
            self.assertEqual(
                values,
                (i*100+0, i*100+4, i*100+8, i*100+12),
            )


class TestBME280Message(unittest.TestCase):
    def test_from_buf(self):
        buf = bytearray(
            _sn2d_comm.ffi.sizeof("uint8_t") +
            _sn2d_comm.ffi.sizeof("struct sbx_msg_bme280_t")
        )
        struct = _sn2d_comm.ffi.cast(
            "struct sbx_msg_t*",
            _sn2d_comm.ffi.from_buffer(buf)
        )

        struct.type = _sn2d_comm.lib.SENSOR_BME280
        struct.payload.bme280.timestamp = 12345
        struct.payload.bme280.dig88 = os.urandom(26)
        struct.payload.bme280.dige1 = os.urandom(7)
        struct.payload.bme280.readout = os.urandom(8)

        result = protocol.BME280Message.from_buf(
            unittest.mock.sentinel.type_,
            buf[1:]
        )

        self.assertEqual(
            result.type_,
            unittest.mock.sentinel.type_,
        )

        self.assertIsInstance(
            result,
            protocol.BME280Message,
        )

        self.assertEqual(
            result.timestamp,
            12345,
        )

        self.assertEqual(
            result.dig88,
            bytes(struct.payload.bme280.dig88),
        )

        self.assertEqual(
            result.dige1,
            bytes(struct.payload.bme280.dige1),
        )

        self.assertEqual(
            result.readout,
            bytes(struct.payload.bme280.readout),
        )


class TestSensorStreamMessage(unittest.TestCase):
    def test_from_buf(self):
        base_size = (_sn2d_comm.ffi.sizeof("uint8_t") +
                     _sn2d_comm.ffi.sizeof("struct sbx_msg_sensor_stream_t"))
        buf = bytearray(
            base_size + 32
        )
        struct = _sn2d_comm.ffi.cast(
            "struct sbx_msg_t*",
            _sn2d_comm.ffi.from_buffer(buf)
        )

        struct.type = _sn2d_comm.lib.SENSOR_BME280
        struct.payload.sensor_stream.seq = 123
        struct.payload.sensor_stream.average = 0xffee
        buf[base_size:] = os.urandom(32)

        result = protocol.SensorStreamMessage.from_buf(
            unittest.mock.sentinel.type_,
            buf[1:],
        )

        self.assertEqual(
            result.type_,
            unittest.mock.sentinel.type_,
        )

        self.assertIsInstance(
            result,
            protocol.SensorStreamMessage,
        )

        self.assertEqual(
            result.seq,
            123,
        )

        self.assertEqual(
            result.average,
            0xffee
        )

        self.assertEqual(
            result.packed_data,
            buf[base_size:]
        )


class Testdecode_message(unittest.TestCase):
    def test_STATUS(self):
        data = bytes([0x82, 0xde, 0xad, 0xbe, 0xef])  # status msg type
        with contextlib.ExitStack() as stack:
            from_buf = stack.enter_context(unittest.mock.patch.object(
                protocol.StatusMessage,
                "from_buf",
            ))

            result = protocol.decode_message(data)

        from_buf.assert_called_once_with(
            protocol.MsgType.STATUS,
            data[1:]
        )

        self.assertEqual(from_buf(), result)

    def test_SENSOR_DS18B20(self):
        data = bytes([0xf1, 0xde, 0xad, 0xbe, 0xef])  # ds18b20 msg type
        with contextlib.ExitStack() as stack:
            from_buf = stack.enter_context(unittest.mock.patch.object(
                protocol.DS18B20Message,
                "from_buf",
            ))

            result = protocol.decode_message(data)

        from_buf.assert_called_once_with(
            protocol.MsgType.SENSOR_DS18B20,
            data[1:]
        )

        self.assertEqual(from_buf(), result)

    def test_SENSOR_NOISE(self):
        data = bytes([0xf2, 0xde, 0xad, 0xbe, 0xef])  # noise msg type
        with contextlib.ExitStack() as stack:
            from_buf = stack.enter_context(unittest.mock.patch.object(
                protocol.NoiseMessage,
                "from_buf",
            ))

            result = protocol.decode_message(data)

        from_buf.assert_called_once_with(
            protocol.MsgType.SENSOR_NOISE,
            data[1:]
        )

        self.assertEqual(from_buf(), result)

    def test_SENSOR_LIGHT(self):
        data = bytes([0xf4, 0xde, 0xad, 0xbe, 0xef])  # light msg type
        with contextlib.ExitStack() as stack:
            from_buf = stack.enter_context(unittest.mock.patch.object(
                protocol.LightMessage,
                "from_buf",
            ))

            result = protocol.decode_message(data)

        from_buf.assert_called_once_with(
            protocol.MsgType.SENSOR_LIGHT,
            data[1:]
        )

        self.assertEqual(from_buf(), result)

    def test_SENSOR_BME280(self):
        data = bytes([0xf5, 0xde, 0xad, 0xbe, 0xef])  # bme280 msg type
        with contextlib.ExitStack() as stack:
            from_buf = stack.enter_context(unittest.mock.patch.object(
                protocol.BME280Message,
                "from_buf",
            ))

            result = protocol.decode_message(data)

        from_buf.assert_called_once_with(
            protocol.MsgType.SENSOR_BME280,
            data[1:]
        )

        self.assertEqual(from_buf(), result)

    def test_SENSOR_STREAM_ACCEL_X(self):
        data = bytes([0xf8, 0xde, 0xad, 0xbe, 0xef])  # accel x msg type
        with contextlib.ExitStack() as stack:
            from_buf = stack.enter_context(unittest.mock.patch.object(
                protocol.SensorStreamMessage,
                "from_buf",
            ))

            result = protocol.decode_message(data)

        from_buf.assert_called_once_with(
            protocol.MsgType.SENSOR_STREAM_ACCEL_X,
            data[1:]
        )

        self.assertEqual(from_buf(), result)

    def test_SENSOR_STREAM_ACCEL_Y(self):
        data = bytes([0xf9, 0xde, 0xad, 0xbe, 0xef])  # accel y msg type
        with contextlib.ExitStack() as stack:
            from_buf = stack.enter_context(unittest.mock.patch.object(
                protocol.SensorStreamMessage,
                "from_buf",
            ))

            result = protocol.decode_message(data)

        from_buf.assert_called_once_with(
            protocol.MsgType.SENSOR_STREAM_ACCEL_Y,
            data[1:]
        )

        self.assertEqual(from_buf(), result)

    def test_SENSOR_STREAM_ACCEL_Z(self):
        data = bytes([0xfa, 0xde, 0xad, 0xbe, 0xef])  # accel z msg type
        with contextlib.ExitStack() as stack:
            from_buf = stack.enter_context(unittest.mock.patch.object(
                protocol.SensorStreamMessage,
                "from_buf",
            ))

            result = protocol.decode_message(data)

        from_buf.assert_called_once_with(
            protocol.MsgType.SENSOR_STREAM_ACCEL_Z,
            data[1:]
        )

        self.assertEqual(from_buf(), result)

    def test_SENSOR_STREAM_COMPASS_X(self):
        data = bytes([0xfb, 0xde, 0xad, 0xbe, 0xef])  # compass x msg type
        with contextlib.ExitStack() as stack:
            from_buf = stack.enter_context(unittest.mock.patch.object(
                protocol.SensorStreamMessage,
                "from_buf",
            ))

            result = protocol.decode_message(data)

        from_buf.assert_called_once_with(
            protocol.MsgType.SENSOR_STREAM_COMPASS_X,
            data[1:]
        )

        self.assertEqual(from_buf(), result)

    def test_SENSOR_STREAM_COMPASS_Y(self):
        data = bytes([0xfc, 0xde, 0xad, 0xbe, 0xef])  # compass y msg type
        with contextlib.ExitStack() as stack:
            from_buf = stack.enter_context(unittest.mock.patch.object(
                protocol.SensorStreamMessage,
                "from_buf",
            ))

            result = protocol.decode_message(data)

        from_buf.assert_called_once_with(
            protocol.MsgType.SENSOR_STREAM_COMPASS_Y,
            data[1:]
        )

        self.assertEqual(from_buf(), result)

    def test_SENSOR_STREAM_COMPASS_Z(self):
        data = bytes([0xfd, 0xde, 0xad, 0xbe, 0xef])  # compass z msg type
        with contextlib.ExitStack() as stack:
            from_buf = stack.enter_context(unittest.mock.patch.object(
                protocol.SensorStreamMessage,
                "from_buf",
            ))

            result = protocol.decode_message(data)

        from_buf.assert_called_once_with(
            protocol.MsgType.SENSOR_STREAM_COMPASS_Z,
            data[1:]
        )

        self.assertEqual(from_buf(), result)
