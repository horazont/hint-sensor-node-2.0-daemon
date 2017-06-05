import unittest

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
