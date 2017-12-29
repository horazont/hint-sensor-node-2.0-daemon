import contextlib
import os
import unittest
import unittest.mock

from datetime import datetime, timedelta

import sn2daemon.protocol as protocol
import sn2daemon.sample as sample

import _sn2d_comm


class TestStatusMessage(unittest.TestCase):
    def test_from_buf_v1(self):
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
            datetime.utcfromtimestamp(12345678),
        )

        self.assertEqual(
            result.uptime,
            12345,
        )

        self.assertEqual(
            result.v1_accel_stream_state,
            (12, 123, timedelta(milliseconds=5))
        )

        self.assertEqual(
            result.v1_compass_stream_state,
            (13, 124, timedelta(milliseconds=64))
        )

        self.assertIsNone(
            result.v2_i2c_metrics,
        )

    def test_from_buf_v2(self):
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
        struct.payload.status.status_version = 2
        struct.payload.status.imu.stream_state[0].sequence_number = 12
        struct.payload.status.imu.stream_state[0].timestamp = 123
        struct.payload.status.imu.stream_state[0].period = 5
        struct.payload.status.imu.stream_state[1].sequence_number = 13
        struct.payload.status.imu.stream_state[1].timestamp = 124
        struct.payload.status.imu.stream_state[1].period = 64
        struct.payload.status.i2c_metrics[0].transaction_overruns = 2
        struct.payload.status.i2c_metrics[1].transaction_overruns = 3
        struct.payload.status.bme280_metrics[0].configure_status = 20
        struct.payload.status.bme280_metrics[0].timeouts = 0

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
            datetime.utcfromtimestamp(12345678),
        )

        self.assertEqual(
            result.uptime,
            12345,
        )

        self.assertEqual(
            result.v1_accel_stream_state,
            (12, 123, timedelta(milliseconds=5))
        )

        self.assertEqual(
            result.v1_compass_stream_state,
            (13, 124, timedelta(milliseconds=64))
        )

        self.assertEqual(
            result.v2_i2c_metrics[0].transaction_overruns,
            2,
        )

        self.assertEqual(
            result.v2_i2c_metrics[1].transaction_overruns,
            3,
        )

        self.assertEqual(
            result.v2_bme280_metrics.timeouts,
            20,
        )

        self.assertEqual(
            result.v2_bme280_metrics.configure_status,
            0,
        )

        self.assertEqual(
            result.v4_bme280_metrics[0].timeouts,
            20,
        )

        self.assertEqual(
            result.v4_bme280_metrics[0].configure_status,
            0,
        )

        self.assertEqual(
            result.v4_bme280_metrics[1].timeouts,
            0,
        )

        self.assertEqual(
            result.v4_bme280_metrics[1].configure_status,
            0xff,
        )

    def test_from_buf_v3(self):
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
        struct.payload.status.status_version = 3
        struct.payload.status.imu.stream_state[0].sequence_number = 12
        struct.payload.status.imu.stream_state[0].timestamp = 123
        struct.payload.status.imu.stream_state[0].period = 5
        struct.payload.status.imu.stream_state[1].sequence_number = 13
        struct.payload.status.imu.stream_state[1].timestamp = 124
        struct.payload.status.imu.stream_state[1].period = 64
        struct.payload.status.i2c_metrics[0].transaction_overruns = 2
        struct.payload.status.i2c_metrics[1].transaction_overruns = 3
        struct.payload.status.bme280_metrics[0].configure_status = 0xff
        struct.payload.status.bme280_metrics[0].timeouts = 20

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
            datetime.utcfromtimestamp(12345678),
        )

        self.assertEqual(
            result.uptime,
            12345,
        )

        self.assertEqual(
            result.v1_accel_stream_state,
            (12, 123, timedelta(milliseconds=5))
        )

        self.assertEqual(
            result.v1_compass_stream_state,
            (13, 124, timedelta(milliseconds=64))
        )

        self.assertEqual(
            result.v2_i2c_metrics[0].transaction_overruns,
            2,
        )

        self.assertEqual(
            result.v2_i2c_metrics[1].transaction_overruns,
            3,
        )

        self.assertEqual(
            result.v2_bme280_metrics.timeouts,
            20,
        )

        self.assertEqual(
            result.v2_bme280_metrics.configure_status,
            0xff,
        )

        self.assertEqual(
            result.v4_bme280_metrics[1].timeouts,
            0,
        )

        self.assertEqual(
            result.v4_bme280_metrics[1].configure_status,
            0xff,
        )

    def test_from_buf_v4(self):
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
        struct.payload.status.status_version = 4
        struct.payload.status.imu.stream_state[0].sequence_number = 12
        struct.payload.status.imu.stream_state[0].timestamp = 123
        struct.payload.status.imu.stream_state[0].period = 5
        struct.payload.status.imu.stream_state[1].sequence_number = 13
        struct.payload.status.imu.stream_state[1].timestamp = 124
        struct.payload.status.imu.stream_state[1].period = 64
        struct.payload.status.i2c_metrics[0].transaction_overruns = 2
        struct.payload.status.i2c_metrics[1].transaction_overruns = 3
        struct.payload.status.bme280_metrics[0].configure_status = 0x12
        struct.payload.status.bme280_metrics[0].timeouts = 20
        struct.payload.status.bme280_metrics[1].configure_status = 0x34
        struct.payload.status.bme280_metrics[1].timeouts = 1204

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
            datetime.utcfromtimestamp(12345678),
        )

        self.assertEqual(
            result.uptime,
            12345,
        )

        self.assertEqual(
            result.v1_accel_stream_state,
            (12, 123, timedelta(milliseconds=5))
        )

        self.assertEqual(
            result.v1_compass_stream_state,
            (13, 124, timedelta(milliseconds=64))
        )

        self.assertEqual(
            result.v2_i2c_metrics[0].transaction_overruns,
            2,
        )

        self.assertEqual(
            result.v2_i2c_metrics[1].transaction_overruns,
            3,
        )

        self.assertEqual(
            result.v2_bme280_metrics.timeouts,
            20,
        )

        self.assertEqual(
            result.v2_bme280_metrics.configure_status,
            0x12,
        )

        self.assertEqual(
            result.v4_bme280_metrics[1].timeouts,
            1204,
        )

        self.assertEqual(
            result.v4_bme280_metrics[1].configure_status,
            0x34,
        )


class TestDS18B20Message(unittest.TestCase):
    def setUp(self):
        self.msg = protocol.DS18B20Message(
            12345,
            [
                (b"12345678", 12.3),
                (b"01234567", 23.5),
                (b"abcdefgh", -12.3),
            ]
        )

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

    def test_init(self):
        ds18b20_msg = protocol.DS18B20Message(
            12345,
            [
                (b"12345678", 12.3),
                (b"01234567", 23.5),
                (b"abcdefgh", -12.3),
            ]
        )

        self.assertEqual(
            ds18b20_msg.type_,
            protocol.MsgType.SENSOR_DS18B20,
        )

        self.assertEqual(
            ds18b20_msg.timestamp,
            12345,
        )

        self.assertEqual(
            ds18b20_msg.samples,
            [
                (b"12345678", 12.3),
                (b"01234567", 23.5),
                (b"abcdefgh", -12.3),
            ]
        )

    def test_get_samples(self):
        self.assertCountEqual(
            list(self.msg.get_samples()),
            [
                sample.Sample(
                    12345,
                    sample.SensorPath(
                        sample.Part.DS18B20,
                        "3132333435363738",
                    ),
                    12.3,
                ),
                sample.Sample(
                    12345,
                    sample.SensorPath(
                        sample.Part.DS18B20,
                        "3031323334353637",
                    ),
                    23.5,
                ),
                sample.Sample(
                    12345,
                    sample.SensorPath(
                        sample.Part.DS18B20,
                        "6162636465666768",
                    ),
                    -12.3,
                ),
            ]
        )


class TestNoiseMessage(unittest.TestCase):
    def setUp(self):
        self.msg = protocol.NoiseMessage(
            [
                (1001, 235),
                (2001, 435),
                (3004, 237),
            ]
        )

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

    def test_init(self):
        noise_msg = protocol.NoiseMessage(
            [
                (1001, 235),
                (2001, 435),
                (3004, 237),
            ]
        )

        self.assertEqual(
            noise_msg.type_,
            protocol.MsgType.SENSOR_NOISE,
        )

        self.assertEqual(
            noise_msg.samples,
            [
                (1001, 235),
                (2001, 435),
                (3004, 237),
            ]
        )

    def test_get_samples(self):
        self.assertCountEqual(
            list(self.msg.get_samples()),
            [
                sample.Sample(
                    1001,
                    sample.SensorPath(
                        sample.Part.CUSTOM_NOISE,
                        0,
                    ),
                    235,
                ),
                sample.Sample(
                    2001,
                    sample.SensorPath(
                        sample.Part.CUSTOM_NOISE,
                        0,
                    ),
                    435,
                ),
                sample.Sample(
                    3004,
                    sample.SensorPath(
                        sample.Part.CUSTOM_NOISE,
                        0,
                    ),
                    237,
                ),
            ]
        )


class TestLightMessage(unittest.TestCase):
    def setUp(self):
        self.msg = protocol.LightMessage(
            [
                (1000, (1, 2, 3, 4)),
                (2001, (5, 6, 7, 8)),
            ]
        )

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

    def test_init(self):
        light_msg = protocol.LightMessage(
            [
                (1000, (1, 2, 3, 4)),
                (2001, (5, 6, 7, 8)),
            ]
        )

        self.assertEqual(
            light_msg.type_,
            protocol.MsgType.SENSOR_LIGHT,
        )

        self.assertEqual(
            light_msg.samples,
            [
                (1000, (1, 2, 3, 4)),
                (2001, (5, 6, 7, 8)),
            ]
        )

    def test_get_samples(self):
        self.assertCountEqual(
            list(self.msg.get_samples()),
            [
                sample.Sample(
                    1000,
                    sample.SensorPath(
                        sample.Part.TCS3200,
                        0,
                        sample.TCS3200Subpart.RED,
                    ),
                    1,
                ),
                sample.Sample(
                    1000,
                    sample.SensorPath(
                        sample.Part.TCS3200,
                        0,
                        sample.TCS3200Subpart.GREEN,
                    ),
                    2,
                ),
                sample.Sample(
                    1000,
                    sample.SensorPath(
                        sample.Part.TCS3200,
                        0,
                        sample.TCS3200Subpart.BLUE,
                    ),
                    3,
                ),
                sample.Sample(
                    1000,
                    sample.SensorPath(
                        sample.Part.TCS3200,
                        0,
                        sample.TCS3200Subpart.CLEAR,
                    ),
                    4,
                ),
                sample.Sample(
                    2001,
                    sample.SensorPath(
                        sample.Part.TCS3200,
                        0,
                        sample.TCS3200Subpart.RED,
                    ),
                    5,
                ),
                sample.Sample(
                    2001,
                    sample.SensorPath(
                        sample.Part.TCS3200,
                        0,
                        sample.TCS3200Subpart.GREEN,
                    ),
                    6,
                ),
                sample.Sample(
                    2001,
                    sample.SensorPath(
                        sample.Part.TCS3200,
                        0,
                        sample.TCS3200Subpart.BLUE,
                    ),
                    7,
                ),
                sample.Sample(
                    2001,
                    sample.SensorPath(
                        sample.Part.TCS3200,
                        0,
                        sample.TCS3200Subpart.CLEAR,
                    ),
                    8,
                ),
            ]
        )


class TestBME280Message(unittest.TestCase):
    def setUp(self):
        self.msg = protocol.BME280Message(
            1234,
            23.4,
            1234.53,
            44.5
        )

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
        struct.payload.bme280.instance = 3
        struct.payload.bme280.dig88 = os.urandom(26)
        struct.payload.bme280.dige1 = os.urandom(7)
        struct.payload.bme280.readout = os.urandom(8)

        with contextlib.ExitStack() as stack:
            get_calibration = stack.enter_context(
                unittest.mock.patch("sn2daemon.bme280.get_calibration")
            )

            get_readout = stack.enter_context(
                unittest.mock.patch("sn2daemon.bme280.get_readout")
            )
            get_readout.return_value = (
                unittest.mock.sentinel.temp_raw,
                unittest.mock.sentinel.pressure_raw,
                unittest.mock.sentinel.humidity_raw,
            )

            compensate_temperature = stack.enter_context(
                unittest.mock.patch("sn2daemon.bme280.compensate_temperature")
            )

            compensate_humidity = stack.enter_context(
                unittest.mock.patch("sn2daemon.bme280.compensate_humidity")
            )

            compensate_pressure = stack.enter_context(
                unittest.mock.patch("sn2daemon.bme280.compensate_pressure")
            )

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
            result.instance,
            3,
        )

        get_calibration.assert_called_once_with(
            bytes(struct.payload.bme280.dig88),
            bytes(struct.payload.bme280.dige1),
        )

        get_readout.assert_called_once_with(
            bytes(struct.payload.bme280.readout),
        )

        compensate_temperature.assert_called_once_with(
            get_calibration(),
            unittest.mock.sentinel.temp_raw,
        )

        self.assertEqual(
            result.temperature,
            compensate_temperature(),
        )

        compensate_pressure.assert_called_once_with(
            get_calibration(),
            unittest.mock.sentinel.pressure_raw,
            compensate_temperature(),
        )

        self.assertEqual(
            result.pressure,
            compensate_pressure(),
        )

        compensate_humidity.assert_called_once_with(
            get_calibration(),
            unittest.mock.sentinel.humidity_raw,
            compensate_temperature(),
        )

        self.assertEqual(
            result.humidity,
            compensate_humidity(),
        )

    def test_init(self):
        bme280_msg = protocol.BME280Message(
            1234,
            23.4,
            1234.53,
            44.5
        )

        self.assertEqual(
            bme280_msg.type_,
            protocol.MsgType.SENSOR_BME280,
        )

        self.assertEqual(
            bme280_msg.timestamp,
            1234,
        )

        self.assertEqual(
            bme280_msg.temperature,
            23.4
        )

        self.assertEqual(
            bme280_msg.pressure,
            1234.53,
        )

        self.assertEqual(
            bme280_msg.humidity,
            44.5
        )

    def test_get_samples(self):
        self.assertCountEqual(
            list(self.msg.get_samples()),
            [
                sample.Sample(
                    1234,
                    sample.SensorPath(
                        sample.Part.BME280,
                        0,
                        sample.BME280Subpart.TEMPERATURE,
                    ),
                    23.4,
                ),
                sample.Sample(
                    1234,
                    sample.SensorPath(
                        sample.Part.BME280,
                        0,
                        sample.BME280Subpart.HUMIDITY,
                    ),
                    44.5
                ),
                sample.Sample(
                    1234,
                    sample.SensorPath(
                        sample.Part.BME280,
                        0,
                        sample.BME280Subpart.PRESSURE,
                    ),
                    1234.53,
                ),
            ]
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

        with contextlib.ExitStack() as stack:
            decompress = stack.enter_context(
                unittest.mock.patch("sn2daemon.sensor_stream.decompress")
            )

            result = protocol.SensorStreamMessage.from_buf(
                unittest.mock.sentinel.type_,
                buf[1:],
            )

        decompress.assert_called_once_with(
            -18,
            buf[base_size:]
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
            result.data,
            decompress(),
        )

    def test_init(self):
        sm_msg = protocol.SensorStreamMessage(
            protocol.MsgType.SENSOR_STREAM_COMPASS_Y,
            123,
            [1, 2, 3]
        )

        self.assertEqual(
            sm_msg.type_,
            protocol.MsgType.SENSOR_STREAM_COMPASS_Y,
        )

        self.assertEqual(
            sm_msg.seq,
            123,
        )

        self.assertEqual(
            sm_msg.data,
            [1, 2, 3]
        )

    def test_path(self):
        mapping = [
            (protocol.MsgType.SENSOR_STREAM_ACCEL_X,
             sample.LSM303DSubpart.ACCEL_X),
            (protocol.MsgType.SENSOR_STREAM_ACCEL_Y,
             sample.LSM303DSubpart.ACCEL_Y),
            (protocol.MsgType.SENSOR_STREAM_ACCEL_Z,
             sample.LSM303DSubpart.ACCEL_Z),
            (protocol.MsgType.SENSOR_STREAM_COMPASS_X,
             sample.LSM303DSubpart.COMPASS_X),
            (protocol.MsgType.SENSOR_STREAM_COMPASS_Y,
             sample.LSM303DSubpart.COMPASS_Y),
            (protocol.MsgType.SENSOR_STREAM_COMPASS_Z,
             sample.LSM303DSubpart.COMPASS_Z),
        ]

        for type_, subpart in mapping:
            msg = protocol.SensorStreamMessage(
                type_,
                1,
                []
            )

            self.assertEqual(
                msg.path,
                sample.SensorPath(
                    sample.Part.LSM303D,
                    0,
                    subpart,
                )
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
