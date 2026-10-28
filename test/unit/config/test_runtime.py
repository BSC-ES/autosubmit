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

"""Tests for the parsed runtime configuration."""

from typing import TYPE_CHECKING

import pytest

from autosubmit.config.runtime import (
    LogRecoveryConfig,
    RuntimeConfig,
    resolve_log_recovery_queue_size,
)
from autosubmit.config.utils import as_mapping
from autosubmit.log.log import AutosubmitCritical

if TYPE_CHECKING:
    from autosubmit.config.configcommon import AutosubmitConfig


def test_runtime_defaults() -> None:
    runtime = RuntimeConfig.from_experiment_data({})
    assert runtime.log_recovery == LogRecoveryConfig()
    assert runtime.log_recovery.consecutive_max_restarts == 2
    assert runtime.log_recovery.retry_delay == 600.0
    assert runtime.log_recovery.inline_fallback is True
    assert runtime.log_recovery.min_alive_seconds == 5.0


def test_runtime_none_experiment_data() -> None:
    assert RuntimeConfig.from_experiment_data(None) == RuntimeConfig()


@pytest.mark.parametrize(
    "value,name,expected",
    [
        (None, "value", {}),
        ({"a": 1}, "value", {"a": 1}),
    ],
    ids=["none", "mapping"],
)
def test_as_mapping(value, name, expected) -> None:
    assert as_mapping(value, name) == expected


@pytest.mark.parametrize("value", ["not-a-mapping", 1, ["a"]], ids=["str", "int", "list"])
def test_as_mapping_raises_for_non_mapping(value) -> None:
    with pytest.raises(AutosubmitCritical) as exc_info:
        as_mapping(value, "PLATFORMS")

    assert exc_info.value.code == 7014
    assert "PLATFORMS" in exc_info.value.message


@pytest.mark.parametrize(
    "log_recovery,expected",
    [
        (
            {
                "CONSECUTIVE_MAX_RESTARTS": 5,
                "RETRY_DELAY": 30,
                "INLINE_FALLBACK": False,
                "MIN_ALIVE_SECONDS": 1.5,
            },
            LogRecoveryConfig(5, 30.0, False, 1.5),
        ),
        (
            {
                "CONSECUTIVE_MAX_RESTARTS": "5",
                "RETRY_DELAY": "30.5",
                "INLINE_FALLBACK": "false",
                "MIN_ALIVE_SECONDS": "2.5",
            },
            LogRecoveryConfig(5, 30.5, False, 2.5),
        ),
        ({"INLINE_FALLBACK": "true"}, LogRecoveryConfig(2, 600.0, True)),
        (
            {"CONSECUTIVE_MAX_RESTARTS": "nope", "RETRY_DELAY": "nope", "MIN_ALIVE_SECONDS": "nope"},
            LogRecoveryConfig(2, 600.0, True),
        ),
        ({"INLINE_FALLBACK": "nope"}, LogRecoveryConfig(2, 600.0, True)),
    ],
    ids=["native-types", "string-types", "string-bool", "invalid-numbers", "invalid-bool"],
)
def test_runtime_log_recovery_parsing(log_recovery: dict, expected: LogRecoveryConfig) -> None:
    runtime = RuntimeConfig.from_experiment_data({"RUNTIME": {"LOG_RECOVERY": log_recovery}})
    assert runtime.log_recovery == expected


@pytest.mark.parametrize(
    "runtime,name",
    [
        ("not-a-dict", "RUNTIME"),
        ({"LOG_RECOVERY": "not-a-dict"}, "RUNTIME.LOG_RECOVERY"),
    ],
    ids=["runtime-not-a-dict", "log-recovery-not-a-dict"],
)
def test_runtime_invalid_sections_raise(runtime, name) -> None:
    with pytest.raises(AutosubmitCritical) as exc_info:
        RuntimeConfig.from_experiment_data({"RUNTIME": runtime})

    assert name in exc_info.value.message


@pytest.mark.parametrize(
    "log_recovery,option_name",
    [
        ({"CONSECUTIVE_MAX_RESTARTS": "nope"}, "RUNTIME.LOG_RECOVERY.CONSECUTIVE_MAX_RESTARTS"),
        ({"RETRY_DELAY": "nope"}, "RUNTIME.LOG_RECOVERY.RETRY_DELAY"),
        ({"INLINE_FALLBACK": "nope"}, "RUNTIME.LOG_RECOVERY.INLINE_FALLBACK"),
        ({"INLINE_FALLBACK": 1}, "RUNTIME.LOG_RECOVERY.INLINE_FALLBACK"),
        ({"MIN_ALIVE_SECONDS": "nope"}, "RUNTIME.LOG_RECOVERY.MIN_ALIVE_SECONDS"),
    ],
    ids=["invalid-restarts", "invalid-delay", "invalid-fallback", "invalid-fallback-type", "invalid-min-alive"],
)
def test_runtime_invalid_values_log_warning(mocker, log_recovery: dict, option_name: str) -> None:
    warning = mocker.patch("autosubmit.config.utils.Log.warning")

    RuntimeConfig.from_experiment_data({"RUNTIME": {"LOG_RECOVERY": log_recovery}})

    warning.assert_called_once()
    assert option_name in warning.call_args.args[0]


@pytest.mark.parametrize("value", [-5, "-10.5"], ids=["negative-number", "negative-string"])
def test_runtime_negative_min_alive_seconds(mocker, value) -> None:
    warning = mocker.patch("autosubmit.config.runtime.Log.warning")

    runtime = RuntimeConfig.from_experiment_data(
        {"RUNTIME": {"LOG_RECOVERY": {"MIN_ALIVE_SECONDS": value}}}
    )

    assert runtime.log_recovery.min_alive_seconds == 0.0
    warning.assert_called_once()
    assert "MIN_ALIVE_SECONDS" in warning.call_args.args[0]


def test_runtime_missing_values_do_not_log_warning(mocker) -> None:
    warning = mocker.patch("autosubmit.config.utils.Log.warning")

    RuntimeConfig.from_experiment_data({"RUNTIME": {"LOG_RECOVERY": {}}})

    warning.assert_not_called()


def test_autosubmit_config_runtime_property(autosubmit_config) -> None:
    as_conf: "AutosubmitConfig" = autosubmit_config(
        "a000",
        {"RUNTIME": {"LOG_RECOVERY": {"CONSECUTIVE_MAX_RESTARTS": 4, "INLINE_FALLBACK": "false"}}},
    )

    assert as_conf.runtime.log_recovery.consecutive_max_restarts == 4
    assert as_conf.runtime.log_recovery.inline_fallback is False
    assert as_conf.runtime.log_recovery.retry_delay == 600.0
    assert as_conf.runtime.log_recovery.min_alive_seconds == 5.0


@pytest.mark.parametrize(
    "platform_name,expected",
    [
        ("MyPlatform", True),
        ("myplatform", True),
        ("MYPLATFORM", True),
        ("other", False),
        (None, False),
        ("", False),
    ],
    ids=["exact", "lower", "upper", "unknown", "none", "empty"],
)
def test_is_recovery_threads_disabled(autosubmit_config, platform_name, expected: bool) -> None:
    as_conf: "AutosubmitConfig" = autosubmit_config(
        "a000",
        {"PLATFORMS": {"MyPlatform": {"DISABLE_RECOVERY_THREADS": True}}},
    )

    assert as_conf.is_recovery_threads_disabled(platform_name) is expected


def test_is_recovery_threads_disabled_missing_platforms(autosubmit_config) -> None:
    as_conf: "AutosubmitConfig" = autosubmit_config("a000", {"PLATFORMS": {}})

    assert as_conf.is_recovery_threads_disabled("MyPlatform") is False


@pytest.mark.parametrize(
    "experiment_data,expected",
    [
        ({}, 200),
        ({"CONFIG": {"LOG_RECOVERY_QUEUE_SIZE": 50}}, 100),
        ({"CONFIG": {"TOTAL_JOBS": 30}}, 60),
        ({"PLATFORMS": {"MyPlatform": {"TOTAL_JOBS": 40}}}, 80),
        ({"PLATFORMS": {"MyPlatform": {"LOG_RECOVERY_QUEUE_SIZE": 20}}}, 40),
    ],
    ids=["defaults", "config-queue-size", "config-total-jobs", "platform-total-jobs", "platform-queue-size"],
)
def test_resolve_log_recovery_queue_size(experiment_data: dict, expected: int) -> None:
    assert resolve_log_recovery_queue_size(experiment_data, "MyPlatform") == expected


def test_resolve_log_recovery_queue_size_invalid_logs_warning(mocker) -> None:
    warning = mocker.patch("autosubmit.config.utils.Log.warning")

    assert resolve_log_recovery_queue_size({"CONFIG": {"TOTAL_JOBS": "nope"}}, "MyPlatform") == 200
    warning.assert_called_once()
