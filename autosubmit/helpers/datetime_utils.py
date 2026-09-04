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

# Canonical format for every timestamp autosubmit stores or serves
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
# Legacy format written by the jobs DB
LEGACY_NAIVE_FORMAT = "%Y-%m-%d %H:%M:%S"


def utc_now_iso() -> str:
    """Return the current UTC time as a canonical ISO string (seconds precision)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def to_utc_iso(value: datetime) -> str:
    """Serialize any datetime to the canonical UTC ISO format."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def parse_utc_iso(value: str) -> datetime:
    """Parse a stored timestamp into a UTC-aware ``datetime``.

    Accepts the canonical form (``+00:00``), the legacy local forms
    (``+0200``/``+02:00``) and the old jobs-DB form
    (``%Y-%m-%d %H:%M:%S``). Use it when normalizing rows
    written before this standard was adopted (e.g. at the API).
    """
    value = value.strip()
    try:
        parsed = datetime.fromisoformat(value)  # +02:00 (and +0200 on 3.11+)
    except ValueError:
        try:
            parsed = datetime.strptime(value, DATETIME_FORMAT)  # +0200 no colon
        except ValueError:
            parsed = datetime.strptime(value, LEGACY_NAIVE_FORMAT)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
