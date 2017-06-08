import contextlib
import random
import unittest
import unittest.mock

from datetime import datetime, timedelta

import sn2daemon.timeline as timeline


class TestTimeline(unittest.TestCase):
    def setUp(self):
        self.tl = timeline.Timeline(2**16, 1000)

    def tearDown(self):
        del self.tl

    def test_feed_and_transform_monotonically(self):
        for i in range(0, 2**16, 100):
            self.assertEqual(
                i,
                self.tl.feed_and_transform(i),
            )

    def test_feed_and_transform_wraparound_to_zero(self):
        self.tl.feed_and_transform(0)
        self.tl.feed_and_transform(10000)
        self.tl.feed_and_transform(20000)
        self.tl.feed_and_transform(30000)
        self.tl.feed_and_transform(40000)
        self.tl.feed_and_transform(50000)
        self.tl.feed_and_transform(60000)
        self.assertEqual(
            self.tl.feed_and_transform(0),
            2**16
        )

    def test_feed_and_transform_wraparound_above_zero(self):
        self.tl.feed_and_transform(0)
        self.tl.feed_and_transform(10000)
        self.tl.feed_and_transform(20000)
        self.tl.feed_and_transform(30000)
        self.tl.feed_and_transform(40000)
        self.tl.feed_and_transform(50000)
        self.tl.feed_and_transform(60000)
        self.assertEqual(
            self.tl.feed_and_transform(1200),
            2**16 + 1200
        )

    def test_feed_and_transform_slack(self):
        self.tl.feed_and_transform(0)
        self.tl.feed_and_transform(10000)
        self.tl.feed_and_transform(20000)
        self.tl.feed_and_transform(30000)
        self.tl.feed_and_transform(40000)
        self.tl.feed_and_transform(50000)
        self.tl.feed_and_transform(60000)
        self.assertEqual(
            self.tl.feed_and_transform(59001),
            59001,
        )

    def test_feed_and_transform_slack_after_wraparound(self):
        self.tl.feed_and_transform(0)
        self.tl.feed_and_transform(10000)
        self.tl.feed_and_transform(20000)
        self.tl.feed_and_transform(30000)
        self.tl.feed_and_transform(40000)
        self.tl.feed_and_transform(50000)
        self.tl.feed_and_transform(60000)
        self.tl.feed_and_transform(10)
        self.assertEqual(
            self.tl.feed_and_transform(65535),
            65535,
        )

    def test_feed_and_transform_slack_wraparound_slack(self):
        self.tl.feed_and_transform(0)
        self.tl.feed_and_transform(10000)
        self.tl.feed_and_transform(20000)
        self.tl.feed_and_transform(30000)
        self.tl.feed_and_transform(40000)
        self.tl.feed_and_transform(50000)
        self.tl.feed_and_transform(60000)
        self.assertEqual(
            self.tl.feed_and_transform(59001),
            59001,
        )
        self.tl.feed_and_transform(10)
        self.assertEqual(
            self.tl.feed_and_transform(65535),
            65535,
        )
        self.assertEqual(
            self.tl.feed_and_transform(10),
            2**16 + 10,
        )

    def test_reset_and_feed(self):
        self.tl.reset(1000)
        self.assertEqual(
            self.tl.feed_and_transform(1000),
            0,
        )

        for i in range(1000, 2**16, 100):
            self.assertEqual(
                i-1000,
                self.tl.feed_and_transform(i),
            )

        self.assertEqual(
            2**16-1000,
            self.tl.feed_and_transform(0)
        )

    def test_feed_and_transform_slack_after_reset(self):
        self.assertEqual(
            -999,
            self.tl.feed_and_transform(2**16-999)
        )

    def test_forward(self):
        self.tl.forward(2**16 + 5)
        self.assertEqual(
            2**16 + 10,
            self.tl.feed_and_transform(10)
        )


class TestRTCifier(unittest.TestCase):
    def setUp(self):
        self.tl = timeline.Timeline(2**16, 1000)
        self.rtcifier = timeline.RTCifier(
            self.tl
        )

    def test_align_and_map_to_rtc(self):
        dt0 = datetime.utcnow()
        t1 = random.randint(1, 1000)

        with contextlib.ExitStack() as stack:
            reset = stack.enter_context(
                unittest.mock.patch.object(
                    self.tl,
                    "reset",
                )
            )

            feed_and_transform = stack.enter_context(
                unittest.mock.patch.object(
                    self.tl,
                    "feed_and_transform",
                )
            )

            self.rtcifier.align(
                dt0,
                unittest.mock.sentinel.t0,
            )

            reset.assert_called_once_with(
                unittest.mock.sentinel.t0,
            )
            reset.reset_mock()
            feed_and_transform.assert_not_called()

            feed_and_transform.return_value = t1

            result = self.rtcifier.map_to_rtc(
                unittest.mock.sentinel.t1,
            )

        feed_and_transform.assert_called_once_with(
            unittest.mock.sentinel.t1,
        )

        self.assertEqual(
            result,
            dt0 + timedelta(milliseconds=t1),
        )
