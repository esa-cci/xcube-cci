# The MIT License (MIT)
# Copyright (c) 2021 by the ESA CCI Toolbox development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import re
from typing import Optional, Tuple, Union

from dateutil.relativedelta import relativedelta
from xcube.core.timecoord import get_timestamps_from_string

from xcube_cci.constants import MONTHS


_RE_TO_DATETIME_FORMATS = \
    [(re.compile(14 * '\\d'), '%Y%m%d%H%M%S', relativedelta()),
     (re.compile(12 * '\\d'), '%Y%m%d%H%M',
      relativedelta(minutes=1, seconds=-1)),
     (re.compile(8 * '\\d'), '%Y%m%d', relativedelta(days=1, seconds=-1)),
     (re.compile(4 * '\\d' + '-' + 2 * '\\d' + '-' + 2 * '\\d'), '%Y-%m-%d',
      relativedelta(days=1, seconds=-1)),
     (re.compile(6 * '\\d'), '%Y%m', relativedelta(months=1, seconds=-1)),
     (re.compile(4 * '\\d'), '%Y', relativedelta(years=1, seconds=-1))]

_FILE_DATE_PATTERN = re.compile(
    r"(?<![0-9])(?:[0-9]{8}T(?:[0-9]{6}|[0-9]{4}|[0-9]{2})|"
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{14}|[0-9]{12}|[0-9]{10}|"
    r"[0-9]{8}|[0-9]{6}|[0-9]{4})(?![0-9])"
)


def get_time_period(
        time_frequency: Optional[str]
) -> Union[Tuple[relativedelta, int], Tuple[None, None]]:
    frequency = (time_frequency or "").replace("-", " ").lower()
    if frequency in ("daily", "day"):
        return relativedelta(days=1), 8
    if frequency in ("mon", "monthly", "month"):
        return relativedelta(months=1), 6
    if frequency in ("yr", "year"):
        return relativedelta(years=1), 4
    match = re.fullmatch(r"([1-9][0-9]*) (hours|days|yrs|years)", frequency)
    if match:
        count, unit = match.groups()
        if unit == "yrs":
            unit = "years"
        return relativedelta(**{unit: int(count)}), {"hours": 10, "days": 8, "years": 4}[unit]
    return None, None


def find_datetime_format(
        filename: str
) -> Tuple[Optional[str], int, int, relativedelta]:
    for regex, time_format, timedelta in _RE_TO_DATETIME_FORMATS:
        searcher = regex.search(filename)
        if searcher:
            p1, p2 = searcher.span()
            return time_format, p1, p2, timedelta
    return None, -1, -1, relativedelta()


def get_timestrings_from_string(
        string: str, minimum_digits: Optional[int] = None
) -> Tuple[Optional[Union[str, int]], Optional[str]]:
    """Extract up to two times, optionally enforcing a minimum precision."""
    if minimum_digits is not None:
        dates = [
            value.replace("T", "").replace("-", "")
            for value in _FILE_DATE_PATTERN.findall(string)
        ]
        dates = [value for value in dates if len(value) >= minimum_digits]
        if not 1 <= len(dates) <= 2:
            return None, None
        times = []
        for value in dates:
            if len(value) == 10:
                value += "0000"
            parsed, _ = get_timestamps_from_string(value)
            if parsed is None:
                return None, None
            times.append(parsed.isoformat())
        return times[0], times[1] if len(times) == 2 else None

    first_time, second_time = get_timestamps_from_string(string)
    if first_time:
        first_time = first_time.isoformat()
    if second_time:
        second_time = second_time.isoformat()
    if not first_time:
        for i, month in enumerate(MONTHS):
            if month in string:
                first_time = i + 1
                break
    return first_time, second_time
