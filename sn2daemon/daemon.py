import array
import asyncio
import bz2
import functools
import logging
import pathlib

from . import protocol, timeline, sensor_stream, sample, utils


class SensorNode2Daemon:
    def __init__(self, args, config, loop):
        super().__init__()
        self.logger = logging.getLogger("sn2d")
        self.__loop = loop
        self.__args = args
        self.__config = config

        self._timeline = timeline.Timeline(
            2**16,  # wraparound
            30000,  # 30s slack
        )
        self._rtcifier = timeline.RTCifier(
            self._timeline,
        )
        self._had_status = False

        self._protocol = None

        imu_datadir = pathlib.Path(
            config["buffer"]["datadir"]
        )
        imu_datadir.mkdir(exist_ok=True)

        # configure all the stream buffers
        self._stream_buffers = {
            path: sensor_stream.Buffer(
                imu_datadir / utils.str_to_path_part(str(path)),
                functools.partial(
                    self._on_stream_emit,
                    path
                ),
                sample_type="h",
            )
            for path in (
                    sample.SensorPath(
                        sample.Part.LSM303D,
                        0,
                        sample.LSM303DSubpart(
                            "{}-{}".format(subpart, axis)
                        )
                    )
                    for subpart in ["accel", "compass"]
                    for axis in ["x", "y", "z"]
            )
        }

        self.__pre_status_buffer = []

        for buf in self._stream_buffers.values():
            buf.batch_size = 1024

    def _on_stream_emit(self, path, t0, period, data):
        bin_data = array.array("h", data).tobytes()
        bz2_data = bz2.compress(bin_data)
        print(path, t0, period, data, "compression:", 1-(len(bz2_data) / len(bin_data)))

    def _make_protocol(self):
        if self._protocol is not None:
            self.logger.warning("protocol already initialised!")
        self._protocol = protocol.SensorNode2Protocol()
        self._protocol.on_message_received.connect(self._on_message)
        return self._protocol

    def _print_status(self, obj):
        print("--- BEGIN STATUS MESSAGE ---")
        print("rtc = {}".format(obj.rtc))
        print("uptime = {}".format(obj.uptime))
        for type_, stream in zip(["accel", "compass"],
                                 [obj.v1_accel_stream_state,
                                  obj.v1_compass_stream_state]):
            print("{} stream:".format(type_))
            if stream is not None:
                print("  seq       = ", stream[0])
                print("  timestamp = ", stream[1])
                print("  period    = ", stream[2])
            else:
                print("  absent")

        for i, bus_metrics in enumerate((obj.v2_i2c_metrics or []), 1):
            print("I2C{}:".format(i))
            print("  transaction overruns: {}".format(
                bus_metrics.transaction_overruns
            ))

        if obj.v2_bme280_metrics:
            print("BME280:")
            print("  timeouts: {}".format(obj.v2_bme280_metrics.timeouts))

        print("--- END STATUS MESSAGE ---")

    def _process_non_status_message(self, obj):
        if hasattr(obj, "get_samples"):
            for sample_obj in obj.get_samples():
                print("{} sensor={} value={}".format(
                    self._rtcifier.map_to_rtc(
                        sample_obj.timestamp
                    ).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    sample_obj.sensor,
                    sample_obj.value,
                ))
        elif isinstance(obj, protocol.SensorStreamMessage):
            stream_buffer = self._stream_buffers[
                obj.path
            ]
            stream_buffer.submit(
                obj.seq,
                obj.data,
            )

    def _on_message(self, obj):
        # print(obj)
        if obj.type_ == protocol.MsgType.STATUS:
            self._had_status = True
            self._rtcifier.align(
                obj.rtc,
                obj.uptime
            )

            for subpart, state in zip(["accel", "compass"],
                                      [obj.v1_accel_stream_state,
                                       obj.v1_compass_stream_state]):
                seq = state.sequence_number
                rtc = self._rtcifier.map_to_rtc(state.timestamp)
                period = state.period
                for axis in "xyz":
                    stream_buffer = self._stream_buffers[
                        sample.SensorPath(
                            sample.Part.LSM303D,
                            0,
                            sample.LSM303DSubpart("{}-{}".format(subpart, axis))
                        )
                    ]
                    stream_buffer.align(
                        seq,
                        rtc,
                        period,
                    )

            self._print_status(obj)

        if self._had_status:
            if self.__pre_status_buffer:
                for msg in self.__pre_status_buffer:
                    self._process_non_status_message(msg)
                self.__pre_status_buffer.clear()
            self._process_non_status_message(obj)
        else:
            self.__pre_status_buffer.append(obj)

    async def run(self):
        _, protocol = await self.__loop.create_datagram_endpoint(
            self._make_protocol,
            remote_addr=(
                self.__config['net']['remote_address'],
                self.__config['net'].get('port', 7284),
            ),
            local_addr=(
                "0.0.0.0",
                self.__config['net'].get('port', 7284),
            ),
        )

        while True:
            await asyncio.sleep(1)
