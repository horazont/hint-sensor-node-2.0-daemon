from datetime import timedelta


class Timeline:
    def __init__(self, wraparound_at, slack):
        super().__init__()
        self.__remote_tip = 0
        self.__local_tip = 0
        self.__wraparound_at = wraparound_at
        self.__slack = slack

    def wraparound_aware_minus(self, v1, v2):
        naive_diff_forward = (v1 - v2) % self.__wraparound_at
        naive_diff_backward = (v2 - v1) % self.__wraparound_at

        if naive_diff_backward < self.__slack:
            return -naive_diff_backward

        return naive_diff_forward

    def reset(self, timestamp):
        """
        Reset internal data structures and start a new epoch at `timestamp`.
        """
        self.__remote_tip = timestamp
        self.__local_tip = 0

    def feed_and_transform(self, timestamp):
        """
        Feed a timestamp into the timeline and return the timestamp relative to
        the epoch.
        """

        change = self.wraparound_aware_minus(timestamp, self.__remote_tip)
        if -self.__slack < change <= 0:
            # in slack region, assume late packet
            return self.__local_tip + change

        self.__remote_tip = timestamp
        self.__local_tip += change
        return self.__local_tip

    def forward(self, offset):
        """
        Advance the timeline by `offset` steps.

        Forwarding happens as if :meth:`feed_and_transform` had been called
        `offset` times, starting with the next timestamp in the timeline,
        incrementing the `timestamp` argument by one each time with proper
        wraparound.
        """
        self.__local_tip += offset
        self.__remote_tip = (self.__remote_tip + offset) % self.__wraparound_at


class RTCifier:
    def __init__(self, timeline):
        super().__init__()
        self.__timeline = timeline

    def align(self, rtc, timestamp):
        self.__timeline.reset(timestamp)
        self.__rtcbase = rtc

    def map_to_rtc(self, timestamp):
        return self.__rtcbase + timedelta(
            milliseconds=self.__timeline.feed_and_transform(timestamp)
        )
