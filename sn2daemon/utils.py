import calendar
import urllib.parse


def str_to_path_part(s):
    return urllib.parse.quote(s, safe=" ")


def dt_to_ts(dt):
    return calendar.timegm(dt.utctimetuple())


def decompose_dt(dt):
    return dt_to_ts(dt), dt.microsecond
