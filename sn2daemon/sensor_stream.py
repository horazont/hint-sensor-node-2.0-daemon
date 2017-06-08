import io
import logging
import math
import struct

from datetime import datetime, timedelta

from . import struct_utils, timeline, utils


def decompress(average, packet):
    values = [average]

    remaining_payload_size = len(packet)

    bitmap = []
    while remaining_payload_size > 0:
        remaining_payload_size -= 1
        next_bitmap_part = packet[0]
        packet = packet[1:]

        for i in range(7, -1, -1):
            bit = (next_bitmap_part & (1 << i)) >> i
            bitmap.append(bit)
            if bit:
                remaining_payload_size -= 1
            else:
                remaining_payload_size -= 2
            if remaining_payload_size <= 0:
                if remaining_payload_size < 0:
                    print("reference:", average)
                    print("bitmap so far:", bitmap)
                    print("remaining packet:", packet)
                    raise ValueError(
                        "codec error: remaining payload is negative!"
                    )
                break

    for compressed in bitmap:
        if compressed:
            raw, = struct.unpack("<b", packet[:1])
            packet = packet[1:]
            values.append(raw + average)
        else:
            raw, = struct.unpack("<h", packet[:2])
            packet = packet[2:]
            values.append(raw + average)

    assert not packet

    return values


class Buffer:
    """
    A frontend to a persistent (restart-safe) stream sample buffer.

    :param persistent_directory: Location where samples are stored before they
                                 are emitted.
    :type persistent_directory: :class:`pathlib.Path`

    .. method:: on_emit(rtc, period, samples)

       A batch of samples is ready.

       :param rtc: The timestamp of the first sample.
       :type rtc: :class:`datetime.datetime`
       :param period: The interval between consecutive samples.
       :type period: :class:`datetime.timedelta`
       :param samples: The sample values
       :type samples: :class:`collections.abc.Sequence`

    """

    _header = struct.Struct(
        "<BQLQc",
    )

    def __init__(self, persistent_directory,
                 on_emit,
                 *,
                 emit_after=timedelta(minutes=1),
                 sample_type="H"):
        if len(sample_type) != 1 or not (32 <= ord(sample_type[0]) <= 127):
            raise ValueError("invalid sample type")
        super().__init__()
        self.logger = logging.getLogger(
            ".".join([__name__, type(self).__qualname__])
        )
        self.on_emit = on_emit
        persistent_directory.mkdir(exist_ok=True)
        self.__path = persistent_directory / "current"

        self.__sample_type = sample_type
        self.__sample_struct = struct.Struct(
            # we only allow ascii here
            "<"+sample_type
        )
        self.batch_size = 1024

        self.__batch_seq_abs0 = None
        self.__batch_data = None

        self.__timeline = timeline.Timeline(
            2**16,
            2**15,
        )
        self.__period = None
        self.__alignment_t0 = None
        self.__alignment_data = []
        self._emit_existing()

    def align(self, seq_rel, rtc, period):
        """
        Configure the mapping between sequence number and real-time clock.

        :param sequence: Sequence number of an arbitrary reference sample.
        :type sequence: :class:`int`
        :param rtc: Timestamp of the reference sample.
        :type rtc: :class:`datetime.datetime`
        :param period: Interval between two consecutive samples
        :type period: :class:`datetime.timedelta`

        The assignment of RTC times to sequence numbers is configured smoothly
        by using the most recent three calls to :meth:`align` to determine the
        mapping.

        A change to `period` causes current buffers to be emitted and the
        mapping to be reset.

        The reference sample addressed by `sequence` must be within 32768
        samples around the last submitted sample; otherwise, incorrect
        alignment will occur.
        """

        if self.__period != period:
            self._emit()
            self.__alignment_data.clear()
            self.__timeline.reset(0)
        self.__period = period

        offset = self.__timeline.feed_and_transform(seq_rel)
        self.__timeline.reset(seq_rel)
        if len(self.__alignment_data) > 2:
            del self.__alignment_data[0]

        for i in range(len(self.__alignment_data)):
            old_seq_abs, old_rtc = self.__alignment_data[i]
            self.__alignment_data[i] = (
                old_seq_abs - offset,
                old_rtc,
            )

        self.__alignment_data.append((0, rtc))

        self.__alignment_t0 = rtc + sum(
            (
                (old_rtc-old_seq_abs*self.__period)-rtc
                for old_seq_abs, old_rtc
                in self.__alignment_data
            ),
            timedelta(0)
        ) / len(self.__alignment_data)

        if self.__batch_seq_abs0 is not None:
            self.__batch_seq_abs0 -= offset

    def _buffer_samples(self, samples):
        try:
            f = self.__path.open("xb")
        except FileExistsError:
            f = self.__path.open("r+b")

        with f:
            f.seek(0)
            # we always re-write the header with current information
            f.write(self._make_header())
            f.seek(0, io.SEEK_END)
            f.writelines(
                self.__sample_struct.pack(sample)
                for sample in samples
            )
            f.flush()

        self.__timeline.forward(len(samples))
        self.__batch_data.extend(samples)

    def _get_batch_t0(self):
        return self.__alignment_t0 + self.__period * self.__batch_seq_abs0

    def _emit(self):
        if not self.__batch_data:
            return

        self.on_emit(
            self.__alignment_t0 + self.__period * self.__batch_seq_abs0,
            self.__period,
            self.__batch_data,
        )
        self.__batch_seq_abs0 += len(self.__batch_data)
        self.__batch_data = []

        try:
            self.__path.unlink()
        except OSError:
            pass

    def submit(self, first_seq_rel, samples):
        """
        Submit samples into the buffer.

        :param first_seq: The sequence number of the first sample.
        :type first_seq: :class:`int`
        :param samples: Samples to submit
        :type samples: :class:`collections.abc.Iterable`

        If the first sequence number of the samples to submit is not the
        expected next sequence number, the current buffer contents are emitted,
        since the buffer does not handle discontinuities.
        """
        first_seq_abs = self.__timeline.feed_and_transform(
            first_seq_rel
        )

        if self.__batch_seq_abs0 is None:
            self.__batch_seq_abs0 = first_seq_abs
            self.__batch_data = []

        if first_seq_abs != self.__batch_seq_abs0 + len(self.__batch_data):
            self._emit()
            self.__batch_seq_abs0 = first_seq_abs

        samples = list(samples)
        while len(samples) + len(self.__batch_data) >= self.batch_size:
            to_submit = self.batch_size - len(self.__batch_data)
            self._buffer_samples(samples[:to_submit])
            self._emit()
            del samples[:to_submit]
        if samples:
            self._buffer_samples(samples)

    def _make_header(self):
        t0 = self._get_batch_t0()
        t0_s = utils.dt_to_ts(t0)
        t0_us = t0.microsecond

        return self._header.pack(
            0x00,  # version
            t0_s, t0_us,
            round(self.__period.total_seconds() * 1e6),  # period
            self.__sample_type.encode("ascii"),  # sample type
        )

    def _parse_sample_data(self, f):
        version, t0_s, t0_us, period, sample_type = \
            struct_utils.read_single(
                f,
                self._header
            )

        self.logger.debug(
            "found file with version %d",
            version,
        )

        if version != 0x00:
            self.logger.warning(
                "discarding data due to unsupported format"
            )
            return

        sample_type = sample_type.decode("ascii", errors="replace")
        t0 = datetime.utcfromtimestamp(t0_s).replace(microsecond=t0_us)
        period = timedelta(microseconds=period)
        data = [
            value
            for value, in struct_utils.read_all(
                    f,
                    self.__sample_struct)
        ]

        self.logger.debug(
            "found %d samples starting at %r with period %s",
            len(data),
            t0,
            period,
        )

        return t0, period, data

    def _emit_existing(self):
        try:
            f = self.__path.open("rb")
        except FileNotFoundError:
            self.logger.debug(
                "no existing data at %r",
                str(self.__path),
            )
            return
        except OSError:
            self.logger.error(
                "failed to load existing data at %r",
                str(self.__path),
            )
            self.__path.unlink()
            return

        with f:
            data = self._parse_sample_data(f)

        self.__path.unlink()

        if data is not None:
            t0, period, data = data
            self.on_emit(
                t0,
                period,
                data,
            )
