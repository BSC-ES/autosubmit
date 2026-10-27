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

from autosubmit.config.utils import as_bool, as_float, as_int, as_mapping

__all__ = ["LogRecoveryConfig", "RuntimeConfig", "resolve_log_recovery_queue_size"]


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
        runtime = as_mapping(as_mapping(experiment_data, "experiment_data").get("RUNTIME"), "RUNTIME")
        log_recovery = as_mapping(runtime.get("LOG_RECOVERY"), "RUNTIME.LOG_RECOVERY")

        defaults = LogRecoveryConfig()
        return cls(
            log_recovery=LogRecoveryConfig(
                consecutive_max_restarts=as_int(
                    log_recovery.get("CONSECUTIVE_MAX_RESTARTS"),
                    defaults.consecutive_max_restarts,
                    "RUNTIME.LOG_RECOVERY.CONSECUTIVE_MAX_RESTARTS",
                ),
                retry_delay=as_float(
                    log_recovery.get("RETRY_DELAY"),
                    defaults.retry_delay,
                    "RUNTIME.LOG_RECOVERY.RETRY_DELAY",
                ),
                inline_fallback=as_bool(
                    log_recovery.get("INLINE_FALLBACK"),
                    defaults.inline_fallback,
                    "RUNTIME.LOG_RECOVERY.INLINE_FALLBACK",
                ),
            )
        )


def resolve_log_recovery_queue_size(experiment_data: Mapping[str, Any] | None, platform_name: str) -> int:
    """Resolve the pending log recovery tracking size for a platform.

    Precedence, from highest to lowest: ``PLATFORMS.<platform>.TOTAL_JOBS``,
    ``CONFIG.TOTAL_JOBS``, ``PLATFORMS.<platform>.LOG_RECOVERY_QUEUE_SIZE``,
    ``CONFIG.LOG_RECOVERY_QUEUE_SIZE`` and the default ``100``. The result is doubled,
    so the default is ``200``.

    The result bounds how many pending recovery entries the platform keeps in memory, not
    the capacity of the multiprocessing queue.

    :param experiment_data: Experiment configuration dictionary.
    :param platform_name: Name of the platform (case-insensitive).
    :return: The pending log recovery tracking size.
    """
    config = as_mapping(experiment_data, "experiment_data")
    platforms = as_mapping(config.get("PLATFORMS"), "PLATFORMS")
    platforms_by_name = {name.upper(): data for name, data in platforms.items()}
    platform_config = as_mapping(
        platforms_by_name.get(platform_name.upper()), f"PLATFORMS.{platform_name}"
    )
    global_config = as_mapping(config.get("CONFIG"), "CONFIG")

    global_queue_size = as_int(
        global_config.get("LOG_RECOVERY_QUEUE_SIZE"),
        100,
        "CONFIG.LOG_RECOVERY_QUEUE_SIZE",
    )
    platform_queue_size = as_int(
        platform_config.get("LOG_RECOVERY_QUEUE_SIZE"),
        global_queue_size,
        f"PLATFORMS.{platform_name}.LOG_RECOVERY_QUEUE_SIZE",
    )
    global_total_jobs = as_int(
        global_config.get("TOTAL_JOBS"),
        platform_queue_size,
        "CONFIG.TOTAL_JOBS",
    )
    platform_total_jobs = as_int(
        platform_config.get("TOTAL_JOBS"),
        global_total_jobs,
        f"PLATFORMS.{platform_name}.TOTAL_JOBS",
    )
    return platform_total_jobs * 2
