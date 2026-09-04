# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

"""Unit tests for ``autosubmit.helpers.datetime_utils``."""

from datetime import datetime, timedelta

from autosubmit.helpers.datetime_utils import (
    to_utc_iso,
    utc_now_iso,
)


def test_utc_now_iso_is_canonical_utc():
    """Test that utc_now_iso returns an ISO string, UTC, second precision."""
    value = utc_now_iso()
    assert isinstance(value, str)
    parsed = datetime.fromisoformat(value)
    assert parsed.tzinfo is not None
    assert parsed.utcoffset() == timedelta(0)
    assert parsed.microsecond == 0


def test_to_utc_iso_converts_aware_local_to_utc():
    """Test that to_utc_iso converts a timezone-aware local datetime to UTC."""
    aware = datetime.fromisoformat("2026-09-03T16:00:00+02:00")
    assert to_utc_iso(aware) == "2026-09-03T14:00:00+00:00"


def test_to_utc_iso_assumes_naive_is_utc():
    """Test that to_utc_iso assumes a naive datetime is UTC."""
    naive = datetime(2026, 9, 3, 16, 0, 0)
    assert to_utc_iso(naive) == "2026-09-03T16:00:00+00:00"
