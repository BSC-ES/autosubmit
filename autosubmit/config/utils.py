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

"""Configuration utility helpers.

This module contains the small value-normalisation helpers used to parse the
``RUNTIME`` configuration section. They live here (instead of in
:mod:`autosubmit.config.runtime`) so they can be reused by other configuration
code without importing the runtime dataclasses.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from autosubmit.log.log import AutosubmitCritical, Log

__all__ = ["as_bool", "as_float", "as_int", "as_mapping"]

_TRUTHY = frozenset({"true", "1", "yes", "on"})
_FALSY = frozenset({"false", "0", "no", "off"})


def as_mapping(value: Any, name: str) -> Mapping[Any, Any]:
    """Return the value when it is a mapping.

    ``None`` is treated as an empty mapping so optional sections can be omitted.

    :param value: Value to normalize.
    :param name: Configuration key used in the error message.
    :return: The original mapping, or an empty mapping.
    :raises AutosubmitCritical: If the value exists but is not a mapping.
    """
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return value
    raise AutosubmitCritical(
        f"Expected a mapping for {name}, got {type(value).__name__}: {value!r}",
        7014,
    )


def as_bool(value: Any, default: bool, name: str) -> bool:
    """Coerce ``value`` to a boolean, falling back to ``default``.

    :param value: Raw configuration value.
    :param default: Value used when ``value`` is missing or invalid.
    :param name: Configuration key used in the warning message.
    :return: The parsed boolean.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _TRUTHY:
            return True
        if normalized in _FALSY:
            return False
    Log.warning(f"Invalid value for {name}: {value!r}. Using default {default}.")
    return default


def as_int(value: Any, default: int, name: str) -> int:
    """Coerce ``value`` to an integer, falling back to ``default``.

    :param value: Raw configuration value.
    :param default: Value used when ``value`` is missing or invalid.
    :param name: Configuration key used in the warning message.
    :return: The parsed integer.
    """
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        Log.warning(f"Invalid value for {name}: {value!r}. Using default {default}.")
        return default


def as_float(value: Any, default: float, name: str) -> float:
    """Coerce ``value`` to a float, falling back to ``default``.

    :param value: Raw configuration value.
    :param default: Value used when ``value`` is missing or invalid.
    :param name: Configuration key used in the warning message.
    :return: The parsed float.
    """
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        Log.warning(f"Invalid value for {name}: {value!r}. Using default {default}.")
        return default
