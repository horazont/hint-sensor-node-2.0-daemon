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
        print(change)
        if -self.__slack < change <= 0:
            # in slack region, assume late packet
            return self.__local_tip + change

        self.__remote_tip = timestamp
        self.__local_tip += change
        return self.__local_tip
