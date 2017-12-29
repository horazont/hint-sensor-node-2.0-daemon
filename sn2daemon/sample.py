import collections

from enum import Enum


class Part(Enum):
    CUSTOM_NOISE = 'noise'
    DS18B20 = 'ds18b20'  # temperature
    BME280 = 'bme280'  # environmental sensor
    TCS3200 = 'tcs3200'  # light sensor
    LSM303D = 'lsm303d'  # accelerometer + compass


class Quantity(Enum):
    NOISE_LEVEL = 'noise-level'  # a.u.
    TEMPERATURE = 'temperature'  # °C
    RELATIVE_HUMIDITY = 'relative-humidity'  # %RH
    PRESSURE = 'pressure'  # Pa
    LIGHT_INTENSITY = 'light-intensity'
    ACCELERATION = 'acceleration'  # g
    MAGNETIC_FIELD = 'magnetic-field'  # gauss


class BME280Subpart(Enum):
    TEMPERATURE = 'temp'
    HUMIDITY = 'hum'
    PRESSURE = 'pres'


class TCS3200Subpart(Enum):
    RED = 'r'
    GREEN = 'g'
    BLUE = 'b'
    CLEAR = 'c'


class LSM303DSubpart(Enum):
    ACCEL_X = 'accel-x'
    ACCEL_Y = 'accel-y'
    ACCEL_Z = 'accel-z'
    COMPASS_X = 'compass-x'
    COMPASS_Y = 'compass-y'
    COMPASS_Z = 'compass-z'


PART_SUBPARTS = {
    Part.BME280: BME280Subpart,
    Part.TCS3200: TCS3200Subpart,
    Part.LSM303D: LSM303DSubpart,
}


_SensorPath = collections.namedtuple(
    "_SensorPath",
    ["part", "instance", "subpart"]
)


class SensorPath(_SensorPath):
    def __new__(cls, part, instance, subpart=None):
        return super().__new__(cls, part, instance, subpart)

    def replace(self, *args, **kwargs):
        return self._replace(*args, **kwargs)

    def __str__(self):
        parts = [self.part.value, self.instance]
        if self.subpart is not None:
            parts.append(self.subpart.value)
        return "/".join(map(str, parts))


_Sample = collections.namedtuple(
    "_Sample",
    ["timestamp", "sensor", "value"]
)


class Sample(_Sample):
    def replace(self, *args, **kwargs):
        return self._replace(*args, **kwargs)
