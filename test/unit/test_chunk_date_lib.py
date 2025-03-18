# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

from datetime import datetime

import pytest
from bscearth.utils.date import (
    add_time,
    add_years,
    add_months,
    add_days,
    add_hours,
    subs_dates,
    sub_days,
    chunk_start_date,
    chunk_end_date,
    previous_day,
    parse_date,
    date2str,
    sum_str_hours,
    split_str_hours,
)


def test_add_time():
    assert add_time(datetime(2000, 1, 1), 1, 'year', 'standard') == datetime(2001, 1, 1)
    assert add_time(datetime(2000, 1, 30), 1, 'month', 'standard') == datetime(2000, 2, 29)
    assert add_time(datetime(2000, 2, 28), 1, 'day', 'standard') == datetime(2000, 2, 29)
    assert add_time(datetime(2000, 2, 28, 23), 1, 'hour', 'standard') == datetime(2000, 2, 29)

    assert add_time(datetime(2000, 1, 1), 1, 'year', 'noleap') == datetime(2001, 1, 1)
    assert add_time(datetime(2000, 1, 30), 1, 'month', 'noleap') == datetime(2000, 2, 28)
    assert add_time(datetime(2000, 2, 28), 1, 'day', 'noleap') == datetime(2000, 3, 1)
    assert add_time(datetime(2000, 2, 28, 23), 1, 'hour', 'noleap') == datetime(2000, 3, 1)

    # Theoretically tests that Log is called
    assert add_time(datetime(2000, 2, 28, 23), 1, 'other', 'noleap') is None


def test_add_years():
    assert add_years(datetime(2000, 1, 1), 1) == datetime(2001, 1, 1)


def test_add_months():
    assert add_months(datetime(2000, 1, 1), 1, 'standard') == datetime(2000, 2, 1)
    assert add_months(datetime(2000, 1, 29), 1, 'standard') == datetime(2000, 2, 29)
    assert add_months(datetime(2000, 1, 1), 1, 'noleap') == datetime(2000, 2, 1)
    assert add_months(datetime(2000, 1, 29), 1, 'noleap') == datetime(2000, 2, 28)


def test_add_days():
    assert add_days(datetime(2000, 1, 1), 1, 'standard') == datetime(2000, 1, 2)
    assert add_days(datetime(2000, 2, 28), 1, 'standard') == datetime(2000, 2, 29)
    assert add_days(datetime(2000, 1, 1), 1, 'noleap') == datetime(2000, 1, 2)
    assert add_days(datetime(2000, 2, 28), 1, 'noleap') == datetime(2000, 3, 1)
    assert add_days(datetime(2000, 3, 1), 1, 'noleap') == datetime(2000, 3, 2)


def test_add_hours():
    assert add_hours(datetime(2000, 1, 1), 24, 'standard') == datetime(2000, 1, 2)
    assert add_hours(datetime(2000, 1, 1, 23), 1, 'standard') == datetime(2000, 1, 2)
    assert add_hours(datetime(2000, 2, 28), 24, 'standard') == datetime(2000, 2, 29)
    assert add_hours(datetime(2000, 2, 28), 24, 'noleap') == datetime(2000, 3, 1)


def test_subs_dates():
    assert subs_dates(datetime(2000, 1, 1), datetime(2001, 1, 1), 'standard') == 366
    assert subs_dates(datetime(2000, 2, 1), datetime(2000, 3, 1), 'standard') == 29
    assert subs_dates(datetime(2000, 2, 28), datetime(2000, 3, 1), 'standard') == 2
    assert subs_dates(datetime(2000, 2, 28, 23), datetime(2000, 3, 1), 'standard') == 1

    assert subs_dates(datetime(2000, 1, 1), datetime(2001, 1, 1), 'noleap') == 365
    assert subs_dates(datetime(2000, 2, 1), datetime(2000, 3, 1), 'noleap') == 28
    assert subs_dates(datetime(2000, 2, 28), datetime(2000, 3, 1), 'noleap') == 1
    assert subs_dates(datetime(2000, 2, 28, 23), datetime(2000, 3, 1), 'noleap') == 0
    assert subs_dates(datetime(2000, 3, 28), datetime(2000, 3, 29), 'noleap') == 1
    assert subs_dates(datetime(1999, 3, 28), datetime(2000, 2, 28), 'noleap') == 337


def test_subs_days():
    assert sub_days(datetime(2000, 1, 2), 1, 'standard') == datetime(2000, 1, 1)
    assert sub_days(datetime(2000, 1, 2), -1, 'standard') == datetime(2000, 1, 3)
    assert sub_days(datetime(2000, 3, 1), 1, 'standard') == datetime(2000, 2, 29)
    assert sub_days(datetime(2000, 2, 28), -1, 'standard') == datetime(2000, 2, 29)
    assert sub_days(datetime(2000, 1, 1), 365, 'standard') == datetime(1999, 1, 1)
    assert sub_days(datetime(1999, 1, 1), -365, 'standard') == datetime(2000, 1, 1)
    assert sub_days(datetime(2000, 12, 31), 365, 'standard') == datetime(2000, 1, 1)
    assert sub_days(datetime(2000, 1, 1), -365, 'standard') == datetime(2000, 12, 31)
    assert sub_days(datetime(2000, 2, 28), -2920, 'standard') == datetime(2008, 2, 26)
    assert sub_days(datetime(2008, 2, 26), 2920, 'standard') == datetime(2000, 2, 28)
    assert sub_days(datetime(2015, 12, 31), -61, 'standard') == datetime(2016, 3, 1)
    assert sub_days(datetime(2016, 3, 1), 61, 'standard') == datetime(2015, 12, 31)
    assert sub_days(datetime(2001, 1, 1), 1, 'standard') == datetime(2000, 12, 31)
    assert sub_days(datetime(1999, 12, 31), -1, 'standard') == datetime(2000, 1, 1)

    assert sub_days(datetime(2000, 1, 2), 1, 'noleap') == datetime(2000, 1, 1)
    assert sub_days(datetime(2000, 1, 2), -1, 'noleap') == datetime(2000, 1, 3)
    assert sub_days(datetime(2000, 3, 1), 1, 'noleap') == datetime(2000, 2, 28)
    assert sub_days(datetime(2000, 2, 28), -1, 'noleap') == datetime(2000, 3, 1)
    assert sub_days(datetime(2000, 1, 1), 365, 'noleap') == datetime(1999, 1, 1)
    assert sub_days(datetime(1999, 1, 1), -365, 'noleap') == datetime(2000, 1, 1)
    assert sub_days(datetime(2001, 1, 1), 365, 'noleap') == datetime(2000, 1, 1)
    assert sub_days(datetime(2000, 1, 1), -365, 'noleap') == datetime(2001, 1, 1)
    assert sub_days(datetime(2000, 2, 28), -2920, 'noleap') == datetime(2008, 2, 28)
    assert sub_days(datetime(2008, 2, 26), 2920, 'noleap') == datetime(2000, 2, 26)
    assert sub_days(datetime(2015, 12, 31), -61, 'noleap') == datetime(2016, 3, 2)
    assert sub_days(datetime(2016, 3, 2), 61, 'noleap') == datetime(2015, 12, 31)
    assert sub_days(datetime(2001, 1, 1), 1, 'noleap') == datetime(2000, 12, 31)
    assert sub_days(datetime(1999, 12, 31), -1, 'noleap') == datetime(2000, 1, 1)


def test_chunk_start_date():
    assert chunk_start_date(datetime(2000, 1, 1), 2, 1, 'year', 'standard') == \
           datetime(2001, 1, 1)
    assert chunk_start_date(datetime(2000, 1, 1), 2, 2, 'year', 'standard') == \
           datetime(2002, 1, 1)

    assert chunk_start_date(datetime(2000, 1, 1), 2, 1, 'year', 'noleap') == \
           datetime(2001, 1, 1)
    assert chunk_start_date(datetime(2000, 1, 1), 2, 2, 'year', 'noleap') == \
           datetime(2002, 1, 1)

    assert chunk_start_date(datetime(2000, 1, 1), 2, 1, 'month', 'standard') == \
           datetime(2000, 2, 1)
    assert chunk_start_date(datetime(2000, 1, 1), 2, 2, 'month', 'standard') == \
           datetime(2000, 3, 1)
    assert chunk_start_date(datetime(2000, 1, 31), 2, 1, 'month', 'standard') == \
           datetime(2000, 2, 29)

    assert chunk_start_date(datetime(2000, 1, 1), 2, 1, 'month', 'noleap') == \
           datetime(2000, 2, 1)
    assert chunk_start_date(datetime(2000, 1, 1), 2, 2, 'month', 'noleap') == \
           datetime(2000, 3, 1)
    assert chunk_start_date(datetime(2000, 1, 31), 2, 1, 'month', 'noleap') == \
           datetime(2000, 2, 28)

    assert chunk_start_date(datetime(2000, 1, 1), 2, 1, 'day', 'standard') == \
           datetime(2000, 1, 2)
    assert chunk_start_date(datetime(2000, 1, 1), 2, 2, 'day', 'standard') == \
           datetime(2000, 1, 3)
    assert chunk_start_date(datetime(2000, 2, 28), 2, 1, 'day', 'standard') == \
           datetime(2000, 2, 29)

    assert chunk_start_date(datetime(2000, 1, 1), 2, 1, 'day', 'noleap') == \
           datetime(2000, 1, 2)
    assert chunk_start_date(datetime(2000, 1, 1), 2, 2, 'day', 'noleap') == \
           datetime(2000, 1, 3)
    assert chunk_start_date(datetime(2000, 2, 28), 2, 1, 'day', 'noleap') == \
           datetime(2000, 3, 1)

    assert chunk_start_date(datetime(2000, 1, 1), 2, 1, 'hour', 'standard') == \
           datetime(2000, 1, 1, 1)
    assert chunk_start_date(datetime(2000, 1, 1), 2, 2, 'hour', 'standard') == \
           datetime(2000, 1, 1, 2)
    assert chunk_start_date(datetime(2000, 2, 28, 23), 2, 1, 'hour', 'standard') == \
           datetime(2000, 2, 29)

    assert chunk_start_date(datetime(2000, 1, 1), 2, 1, 'hour', 'noleap') == \
           datetime(2000, 1, 1, 1)
    assert chunk_start_date(datetime(2000, 1, 1), 2, 2, 'hour', 'noleap') == \
           datetime(2000, 1, 1, 2)
    assert chunk_start_date(datetime(2000, 2, 28, 23), 2, 1, 'hour', 'noleap') == \
           datetime(2000, 3, 1)


def test_chunk_end_date():
    assert chunk_end_date(datetime(2000, 1, 1), 1, 'year', 'standard') == datetime(2001, 1, 1)
    assert chunk_end_date(datetime(2000, 1, 30), 1, 'month', 'standard') == datetime(2000, 2, 29)
    assert chunk_end_date(datetime(2000, 2, 28), 1, 'day', 'standard') == datetime(2000, 2, 29)
    assert chunk_end_date(datetime(2000, 2, 28, 23), 1, 'hour', 'standard') == datetime(2000, 2, 29)

    assert chunk_end_date(datetime(2000, 1, 1), 1, 'year', 'noleap') == datetime(2001, 1, 1)
    assert chunk_end_date(datetime(2000, 1, 30), 1, 'month', 'noleap') == datetime(2000, 2, 28)
    assert chunk_end_date(datetime(2000, 2, 28), 1, 'day', 'noleap') == datetime(2000, 3, 1)
    assert chunk_end_date(datetime(2000, 2, 28, 23), 1, 'hour', 'noleap') == datetime(2000, 3, 1)


def test_previous_date():
    assert previous_day(datetime(2000, 1, 2), 'standard') == datetime(2000, 1, 1)
    assert previous_day(datetime(2000, 3, 1), 'standard') == datetime(2000, 2, 29)

    assert previous_day(datetime(2000, 1, 2), 'noleap') == datetime(2000, 1, 1)
    assert previous_day(datetime(2000, 3, 1), 'noleap') == datetime(2000, 2, 28)

    assert previous_day(datetime(2000, 1, 1), 'noleap') == datetime(1999, 12, 31)
    assert previous_day(datetime(2001, 1, 1), 'noleap') == datetime(2000, 12, 31)


def test_parse_date():
    assert parse_date('') is None
    assert parse_date('2000') == datetime(2000, 1, 1)
    assert parse_date('200001') == datetime(2000, 1, 1)
    assert parse_date('20000101') == datetime(2000, 1, 1)
    assert parse_date('2000010100') == datetime(2000, 1, 1)
    assert parse_date('200001010000') == datetime(2000, 1, 1)
    assert parse_date('20000101000000') == datetime(2000, 1, 1)
    assert parse_date('2000-01-01 00:00:00') == datetime(2000, 1, 1)

    with pytest.raises(ValueError):
        parse_date('200')
    with pytest.raises(ValueError):
        parse_date('20001')
    with pytest.raises(ValueError):
        parse_date('200014')
    with pytest.raises(ValueError):
        parse_date('2000011')
    with pytest.raises(ValueError):
        parse_date('20000230')
    with pytest.raises(ValueError):
        parse_date('200002281')
    with pytest.raises(ValueError):
        parse_date('2000022825')
    with pytest.raises(ValueError):
        parse_date('20000228121')
    with pytest.raises(ValueError):
        parse_date('200002281299')
    with pytest.raises(ValueError):
        parse_date('2000022812591')
    with pytest.raises(ValueError):
        parse_date('20000228125999')


def test_date2str():
    # noinspection PyTypeChecker
    assert date2str(None) == ''
    assert date2str(datetime(2000, 1, 1)) == '20000101'
    assert date2str(datetime(2000, 1, 1), 'H') == '2000010100'
    assert date2str(datetime(2000, 1, 1), 'M') == '200001010000'
    assert date2str(datetime(2000, 1, 1), 'S') == '20000101000000'


def test_sum_str_hours():
    assert sum_str_hours('00:30', '00:30') == '01:00'
    assert sum_str_hours('14:30', '14:30') == '29:00'
    assert sum_str_hours('50:45', '50:30') == '101:15'


def test_split_str_hours():
    assert split_str_hours('00:30') == (0, 30)
    assert split_str_hours('12:55') == (12, 55)
    with pytest.raises(Exception):
        parse_date('30')
