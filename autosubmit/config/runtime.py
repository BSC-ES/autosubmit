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

"""Parsed runtime configuration.

The ``RUNTIME`` section of the experiment configuration holds options that
affect the behaviour of an Autosubmit run (for example, how log recovery is
retried). They are parsed and normalized here so the rest of the code receives
typed values instead of raw dictionaries.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from autosubmit.log.log import Log

__all__ = ["LogRecoveryConfig", "RuntimeConfig", "as_mapping", "resolve_log_recovery_queue_size"]

_TRUTHY = frozenset({"true", "1", "yes", "on"})
_FALSY = frozenset({"false", "0", "no", "off"})


# TODO: move these  helpers to ``autosubmit/config/utils.py`` so other
# configuration modules can reuse them. It is not possible yet because
# ``config/utils.py`` imports ``upgrade_scripts``, which imports ``configcommon``,
# which imports this module, creating an import cycle.
def as_mapping(value: Any) -> Mapping[Any, Any]:
    """Return the value when it is a mapping, otherwise an empty mapping.

    :param value: Value to normalize.
    :return: The original mapping, or an empty mapping.
    """
    return value if isinstance(value, Mapping) else {}


def _as_bool(value: Any, default: bool, name: str) -> bool:
    if value is None:
        return default
    elif isinstance(value, bool):
        return value
    elif isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _TRUTHY:
            return True
        elif normalized in _FALSY:
            return False
    Log.warning(f"Invalid value for {name}: {value!r}. Using default {default}.")
    return default


def _as_int(value: Any, default: int, name: str) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        Log.warning(f"Invalid value for {name}: {value!r}. Using default {default}.")
        return default


def _as_float(value: Any, default: float, name: str) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        Log.warning(f"Invalid value for {name}: {value!r}. Using default {default}.")
        return default


@dataclass(frozen=True)
class LogRecoveryConfig:
    """Options configured under ``RUNTIME.LOG_RECOVERY``."""

    consecutive_max_restarts: int = 2
    retry_delay: float = 600.0
    inline_fallback: bool = True


@dataclass(frozen=True)
class RuntimeConfig:
    """Parsed ``RUNTIME`` section of the experiment configuration.

    Holds the options that affect how an Autosubmit run behaves.
    """

    log_recovery: LogRecoveryConfig = field(default_factory=LogRecoveryConfig)

    @classmethod
    def from_experiment_data(cls, experiment_data: Mapping[str, Any] | None) -> "RuntimeConfig":
        """Build the runtime configuration from the experiment data.

        Missing values fall back to the defaults defined by the dataclasses, and values with
        the wrong type are coerced or replaced by their default.

        :param experiment_data: Experiment configuration dictionary.
        :return: The parsed runtime configuration.
        """
        runtime = as_mapping(as_mapping(experiment_data).get("RUNTIME"))
        log_recovery = as_mapping(runtime.get("LOG_RECOVERY"))

        defaults = LogRecoveryConfig()
        return cls(
            log_recovery=LogRecoveryConfig(
                consecutive_max_restarts=_as_int(
                    log_recovery.get("CONSECUTIVE_MAX_RESTARTS"),
                    defaults.consecutive_max_restarts,
                    "RUNTIME.LOG_RECOVERY.CONSECUTIVE_MAX_RESTARTS",
                ),
                retry_delay=_as_float(
                    log_recovery.get("RETRY_DELAY"),
                    defaults.retry_delay,
                    "RUNTIME.LOG_RECOVERY.RETRY_DELAY",
                ),
                inline_fallback=_as_bool(
                    log_recovery.get("INLINE_FALLBACK"),
                    defaults.inline_fallback,
                    "RUNTIME.LOG_RECOVERY.INLINE_FALLBACK",
                ),
            )
        )


def resolve_log_recovery_queue_size(experiment_data: Mapping[str, Any] | None, platform_name: str) -> int:
    """Resolve the recovery queue size for a platform.

    Keeps the legacy ``CONFIG``/``PLATFORMS`` precedence used by the platform constructor.
    ``TOTAL_JOBS`` takes precedence over ``LOG_RECOVERY_QUEUE_SIZE`` and the result is doubled.

    :param experiment_data: Experiment configuration dictionary.
    :param platform_name: Name of the platform (case-insensitive).
    :return: The recovery queue size.
    """
    config = as_mapping(experiment_data)
    platforms = as_mapping(config.get("PLATFORMS"))
    platforms_by_name = {name.upper(): data for name, data in platforms.items()}
    platform_config = as_mapping(platforms_by_name.get(platform_name.upper()))
    global_config = as_mapping(config.get("CONFIG"))

    default_queue_size = global_config.get("LOG_RECOVERY_QUEUE_SIZE", 100)
    platform_default_queue_size = platform_config.get("LOG_RECOVERY_QUEUE_SIZE", default_queue_size)
    config_total_jobs = global_config.get("TOTAL_JOBS", platform_default_queue_size)
    # TODO: this reads ``PLATFORMS.TOTAL_JOBS`` (global) instead of the per-platform value. Kept as
    #       is to preserve the existing behaviour.
    platform_total_jobs = platforms.get("TOTAL_JOBS", config_total_jobs)

    try:
        return int(platform_total_jobs) * 2
    except (TypeError, ValueError):
        Log.warning(
            f"Invalid value for LOG_RECOVERY_QUEUE_SIZE/TOTAL_JOBS on {platform_name}: "
            f"{platform_total_jobs!r}. Using default 200."
        )
        return 200
