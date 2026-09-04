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

"""Timestamp helpers.

Every timestamp autosubmit persists should be UTC, ISO-8601, second
precision, with explicit offset: e.g. 2026-09-04T09:00:00+00:00

Converting to a local timezone for display is responsibility of the API/GUI.
"""

from datetime import datetime, timezone


def utc_now_iso() -> str:
    """Return the current UTC time as a canonical ISO string (seconds precision)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def to_utc_iso(value: datetime) -> str:
    """Serialize any datetime to the canonical UTC ISO format."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")
