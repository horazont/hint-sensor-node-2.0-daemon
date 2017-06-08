import contextlib
import pathlib
import subprocess
import tempfile
import unittest
import unittest.mock

from datetime import datetime, timedelta

import sn2daemon.sensor_stream as sensor_stream


class TestBuffer(unittest.TestCase):
    def setUp(self):
        self._context = contextlib.ExitStack()
        self.bufdir = pathlib.Path(self._context.enter_context(
            tempfile.TemporaryDirectory()
        ))
        self.on_emit = unittest.mock.Mock()
        self.on_emit.return_value = None
        self.buf = sensor_stream.Buffer(
            self.bufdir,
            self.on_emit,
        )
        self.t0 = datetime.utcnow()
        self.period = timedelta(milliseconds=5)
        self.buf.align(
            0,
            self.t0,
            self.period,
        )

    def tearDown(self):
        for item in self.bufdir.iterdir():
            print("TMP FILE", item)
            print(subprocess.check_output(
                ["hexdump", "-C", str(item)],
            ).decode())
        self._context.close()

    def test_batch_size(self):
        self.assertEqual(
            self.buf.batch_size,
            1024,
        )

    def test_batch_size_is_writable(self):
        self.buf.batch_size = 200
        self.assertEqual(
            self.buf.batch_size,
            200,
        )

    def test_emits_immediately_on_non_consecutive_sequence_number(self):
        self.buf.submit(
            0,
            list(range(10))
        )

        self.on_emit.assert_not_called()

        self.buf.submit(
            12,
            list(range(10))
        )

        self.on_emit.assert_called_once_with(
            self.t0,
            self.period,
            list(range(10))
        )

    def test_timestamp_is_adjusted_for_non_consecutive_samples(self):
        self.buf.batch_size = 200

        self.buf.submit(
            0,
            list(range(10))
        )

        self.buf.submit(
            12,
            list(range(190))
        )

        self.on_emit.reset_mock()

        self.buf.submit(
            202,
            list(range(190, 200))
        )

        self.on_emit.assert_called_once_with(
            self.t0 + self.period * 12,
            self.period,
            list(range(200))
        )

    def test_emits_data_after_configured_interval(self):
        self.buf.batch_size = 200
        # samples needed: 200
        self.buf.submit(
            0,
            list(range(200))
        )

        self.on_emit.assert_called_once_with(
            self.t0,
            self.period,
            list(range(200))
        )

    def test_emission_works_with_overlapping_batches(self):
        self.buf.batch_size = 200

        self.buf.submit(
            0,
            list(range(200))
        )

        self.on_emit.assert_called_once_with(
            self.t0,
            self.period,
            list(range(200))
        )
        self.on_emit.reset_mock()

        self.buf.submit(
            200,
            list(range(200, 500))
        )

        self.on_emit.assert_called_once_with(
            self.t0 + self.period * 200,
            self.period,
            list(range(200, 400))
        )
        self.on_emit.reset_mock()

        self.buf.submit(
            500,
            list(range(500, 601))
        )

        self.on_emit.assert_called_once_with(
            self.t0 + self.period * 400,
            self.period,
            list(range(400, 600))
        )
        self.on_emit.reset_mock()

        self.buf.submit(
            601,
            list(range(601, 1000))
        )

        self.assertSequenceEqual(
            self.on_emit.mock_calls,
            [
                unittest.mock.call(
                    self.t0 + self.period * 600,
                    self.period,
                    list(range(600, 800))
                ),
                unittest.mock.call(
                    self.t0 + self.period * 800,
                    self.period,
                    list(range(800, 1000))
                )
            ]
        )

    def test_emit_existing_data_from_persistent_storage_on_construction(self):
        self.buf.submit(
            0,
            list(range(190))
        )

        on_emit = unittest.mock.Mock()
        on_emit.return_value = None

        sensor_stream.Buffer(
            self.bufdir,
            on_emit,
        )

        on_emit.assert_called_once_with(
            self.t0,
            self.period,
            list(range(190))
        )

    def test_no_reemission_of_emitted_data(self):
        self.buf.batch_size = 200
        self.buf.submit(
            0,
            list(range(200))
        )
        self.on_emit.assert_called_once_with(
            self.t0,
            self.period,
            list(range(200))
        )

        on_emit = unittest.mock.Mock()
        on_emit.return_value = None

        sensor_stream.Buffer(
            self.bufdir,
            on_emit,
        )

        on_emit.assert_not_called()

    def test_sequence_number_wraparound(self):
        self.buf.batch_size = 200

        self.buf.submit(
            0,
            list(range(65400))
        )

        self.on_emit.reset_mock()

        self.buf.submit(
            65400,
            list(range(136))
        )

        self.buf.submit(
            0,
            list(range(136, 200))
        )

        self.on_emit.assert_called_once_with(
            self.t0 + self.period * 65400,
            self.period,
            list(range(200)),
        )

    def test_sequence_number_wraparound_single_large_batch(self):
        self.buf.batch_size = 200

        self.buf.submit(
            0,
            list(range(65400)) + list(range(200))
        )

        self.assertIn(
            unittest.mock.call(
                self.t0 + self.period * 65400,
                self.period,
                list(range(200)),
            ),
            self.on_emit.mock_calls
        )

    def test_sequence_number_wraparound_within_batch(self):
        self.buf.batch_size = 80000

        self.buf.submit(
            0,
            list(range(65536)) + list(range(65536))
        )

        self.on_emit.assert_called_once_with(
            self.t0,
            self.period,
            list(range(65536)) + list(range(14464)),
        )
        self.on_emit.reset_mock()

        self.buf.submit(
            0,
            list(range(65536))
        )

        self.on_emit.assert_called_once_with(
            self.t0 + timedelta(seconds=400),
            self.period,
            list(range(14464, 65536)) + list(range(28928)),
        )

    def test_sequence_number_multiple_wraparounds_within_batch(self):
        self.buf.batch_size = 160000

        self.buf.submit(
            0,
            list(range(65536)) + list(range(65536)) + list(range(65536))
        )

        self.on_emit.assert_called_once_with(
            self.t0,
            self.period,
            list(range(65536)) + list(range(65536)) + list(range(28928)),
        )
        self.on_emit.reset_mock()

        self.buf.submit(
            0,
            list(range(65536))
        )

        self.on_emit.assert_not_called()

        self.buf.submit(
            0,
            list(range(65536)) + list(range(65536))
        )

        self.on_emit.assert_called_once_with(
            self.t0 + timedelta(seconds=800),
            self.period,
            list(range(28928, 65536)) + list(range(65536)) + list(range(57856))
        )

    def test_align_adapts_t0_during_active_batch(self):
        self.buf = sensor_stream.Buffer(
            self.bufdir,
            self.on_emit,
        )

        self.buf.batch_size = 200

        t0 = datetime(2017, 6, 8, 13, 27, 0, 4900)
        t1 = datetime(2017, 6, 8, 13, 27, 0, 10003)
        t2 = datetime(2017, 6, 8, 13, 27, 0, 15100)

        self.buf.align(
            0,
            t0,
            timedelta(milliseconds=5)
        )

        self.buf.submit(
            0,
            list(range(190))
        )

        self.buf.align(
            1,
            t1,
            timedelta(milliseconds=5)
        )

        self.buf.align(
            2,
            t2,
            timedelta(milliseconds=5)
        )

        self.buf.submit(
            190,
            list(range(190, 200))
        )

        self.on_emit.assert_called_once_with(
            t0.replace(microsecond=5001),
            self.period,
            list(range(200)),
        )
