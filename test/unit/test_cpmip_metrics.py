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
# along with Autosubmit. If not, see <http://www.gnu.org/licenses/>.

from typing import Any, cast

import pytest

from autosubmit.metrics.cpmip_metrics import CPMIPMetrics, CPMIPMetricsData


@pytest.fixture
def metrics_data() -> CPMIPMetricsData:
    return CPMIPMetricsData(
        start_time=1000,
        end_time=2000,
        run_years=1.0,
        ncpus=64,
    )


def test_evaluate_returns_empty_when_thresholds_is_empty(metrics_data):
    """Test that evaluation returns no violations when no thresholds are provided."""

    violations = CPMIPMetrics.evaluate(metrics_data, {})

    assert violations == {}


@pytest.mark.parametrize(
    "metric_name,threshold,comparison,accepted_error,real_value,expected_bound",
    [
        ("SYPD", 5.0, "greater_than", 10, 3.9, 4.5),
        ("CORE_HOURS", 10.0, "less_than", 5, 10.6, 10.5),
    ],
    ids=["sypd_greater_than_violation", "core_hours_less_than_violation"],
)
def test_evaluate_violation_cases(
    mocker,
    metrics_data,
    metric_name,
    threshold,
    comparison,
    accepted_error,
    real_value,
    expected_bound,
):
    """Test that evaluation detects violations outside the accepted tolerance."""

    thresholds = {
        metric_name: {
            "THRESHOLD": threshold,
            "COMPARISON": comparison,
            "%_ACCEPTED_ERROR": accepted_error,
        }
    }
    mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._fetch_metrics",
        return_value={metric_name: real_value},
    )

    violations = CPMIPMetrics.evaluate(metrics_data, thresholds)

    assert metric_name in violations
    assert violations[metric_name]["threshold"] == threshold
    assert violations[metric_name]["accepted_error"] == accepted_error
    assert violations[metric_name]["comparison"] == comparison
    assert violations[metric_name]["bound"] == pytest.approx(expected_bound)
    assert violations[metric_name]["real_value"] == real_value


@pytest.mark.parametrize(
    "metric_name,threshold,comparison,accepted_error,real_value",
    [
        # lower bound = 5.0 * (1 - 0.10) = 4.5
        ("SYPD", 5.0, "greater_than", 10, 4.5),
        # upper bound = 10.0 * (1 + 0.10) = 11.0
        ("CORE_HOURS", 10.0, "less_than", 10, 11.0),
    ],
    ids=["sypd_on_lower_bound", "core_hours_on_upper_bound"],
)
def test_evaluate_boundary_is_not_violation(
    mocker,
    metrics_data,
    metric_name,
    threshold,
    comparison,
    accepted_error,
    real_value,
):
    """Test that values exactly on the tolerance boundary are accepted."""

    thresholds = {
        metric_name: {
            "THRESHOLD": threshold,
            "COMPARISON": comparison,
            "%_ACCEPTED_ERROR": accepted_error,
        }
    }
    mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._fetch_metrics",
        return_value={metric_name: real_value},
    )

    violations = CPMIPMetrics.evaluate(metrics_data, thresholds)

    assert violations == {}


@pytest.mark.parametrize(
    "sypd_name,core_hours_name,chsy_name",
    [
        ("simulated_years_per_day", "core_hours", "core_hours_per_simulated_year"),
        ("SYPD", "CORE_HOURS", "CHSY"),
        ("SIMULATED_YEARS_PER_DAY", "CORE_HOURS", "CORE_HOURS_PER_SIMULATED_YEAR"),
        ("sypd", "core_hours", "chsy"),
        ("SiMuLaTeD_YeArS_pEr_DaY", "CoRe_HoUrS", "CoRe_HoUrS_pEr_SiMuLaTeD_yEaR"),
        ("SyPd", "CoRe_HoUrS", "ChSy"),
    ],
    ids=[
        "snake_case_threshold_names",
        "abbreviation_threshold_names",
        "snake_case_upper_threshold_names",
        "abbreviation_lower_threshold_names",
        "snake_case_mixed_threshold_names",
        "abbreviation_mixed_threshold_names",
    ],
)
def test_evaluate_accepts_metric_alias_threshold_names(
    mocker,
    metrics_data,
    sypd_name,
    core_hours_name,
    chsy_name,
):
    """Test that threshold metric aliases resolve to canonical metric names."""

    thresholds = {
        sypd_name: {
            "THRESHOLD": 5.0,
            "COMPARISON": "greater_than",
            "%_ACCEPTED_ERROR": 10,
        },
        core_hours_name: {
            "THRESHOLD": 100.0,
            "COMPARISON": "greater_than",
            "%_ACCEPTED_ERROR": 10,
        },
        chsy_name: {
            "THRESHOLD": 1000.0,
            "COMPARISON": "greater_than",
            "%_ACCEPTED_ERROR": 10,
        },
    }
    mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._fetch_metrics",
        return_value={"SYPD": 3.9, "CORE_HOURS": 2880.0, "CHSY": 5760.0},
    )

    violations = CPMIPMetrics.evaluate(metrics_data, thresholds)

    assert list(violations.keys()) == ["SYPD"]


def test_evaluate_mixed_metrics_only_returns_violations(mocker, metrics_data):
    """Test that only the violating metric is returned when inputs are mixed."""

    thresholds = {
        "SYPD": {
            "THRESHOLD": 5.0,
            "COMPARISON": "greater_than",
            "%_ACCEPTED_ERROR": 10,
        },
        "LATENCY": {
            "THRESHOLD": 10.0,
            "COMPARISON": "less_than",
            "%_ACCEPTED_ERROR": 5,
        },
    }
    mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._fetch_metrics",
        return_value={"SYPD": 3.9, "LATENCY": 10.4},
    )
    log_mock = mocker.patch("autosubmit.metrics.cpmip_metrics.Log.warning")

    violations = CPMIPMetrics.evaluate(metrics_data, thresholds)

    assert list(violations.keys()) == ["SYPD"]
    log_mock.assert_called_once()
    assert "Ignoring unknown CPMIP metric alias: LATENCY" in str(log_mock.call_args)


@pytest.mark.parametrize(
    "threshold_key,comparison_key,accepted_error_key",
    [
        ("threshold", "comparison", "%_accepted_error"),
        ("ThReShOlD", "CoMpArIsOn", "%_AcCePtEd_ErRoR"),
    ],
    ids=["lowercase_threshold_config_keys", "mixed_case_threshold_config_keys"],
)
def test_evaluate_accepts_threshold_config_keys_case_insensitive(
    mocker,
    metrics_data,
    threshold_key,
    comparison_key,
    accepted_error_key,
):
    """Test that threshold config variable names are case-insensitive."""

    thresholds = {
        "SYPD": {
            threshold_key: 5.0,
            comparison_key: "greater_than",
            accepted_error_key: 10,
        }
    }
    mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._fetch_metrics",
        return_value={"SYPD": 3.9},
    )

    violations = CPMIPMetrics.evaluate(metrics_data, thresholds)

    assert "SYPD" in violations
    assert violations["SYPD"]["threshold"] == 5.0
    assert violations["SYPD"]["accepted_error"] == 10
    assert violations["SYPD"]["comparison"] == "greater_than"


@pytest.mark.parametrize(
    "thresholds,fetched_metrics,expected_log_calls",
    [
        (
            {
                "SYPD": {
                    "THRESHOLD": 5.0,
                    "COMPARISON": "greater_than",
                    "%_ACCEPTED_ERROR": 10,
                }
            },
            {},
            [],
        ),
        (
            {
                "CHSY": {
                    "THRESHOLD": 1000,
                    "COMPARISON": "less_than",
                    "%_ACCEPTED_ERROR": 10,
                }
            },
            {"SYPD": 1.5},
            ["Skipping CPMIP metric 'CHSY' (CHSY) because it was not computed"],
        ),
        (
            {
                "SYPD": {
                    "THRESHOLD": 5.0,
                    "COMPARISON": "greater_than",
                }
            },
            {"SYPD": 3.0},
            ["Skipping CPMIP metric 'SYPD' due to invalid threshold config"],
        ),
        (
            {
                "SYPD": {
                    "THRESHOLD": 5.0,
                    "COMPARISON": "equal_to",
                    "%_ACCEPTED_ERROR": 0,
                }
            },
            {"SYPD": 3.0},
            ["Skipping CPMIP metric 'SYPD' due to invalid threshold config"],
        ),
        (
            {
                "SYPD": {
                    "THRESHOLD": 5.0,
                    "COMPARISON": "greater_than",
                    "%_ACCEPTED_ERROR": 0,
                }
            },
            {"SYPD": 5.0},
            [],
        ),
        (
            {
                "SYPD": {
                    "THRESHOLD": "not-a-number",
                    "COMPARISON": "greater_than",
                    "%_ACCEPTED_ERROR": 10,
                }
            },
            {"SYPD": 3.9},
            ["Skipping CPMIP metric 'SYPD' due to invalid threshold config"],
        ),
        (
            {
                "SYPD": {
                    "THRESHOLD": 0,
                    "COMPARISON": "greater_than",
                    "%_ACCEPTED_ERROR": 10,
                }
            },
            {"SYPD": 3.9},
            ["Skipping CPMIP metric 'SYPD' due to invalid threshold config"],
        ),
        (
            {
                "SYPD": {
                    "THRESHOLD": -1.0,
                    "COMPARISON": "less_than",
                    "%_ACCEPTED_ERROR": 10,
                }
            },
            {"SYPD": 3.9},
            ["Skipping CPMIP metric 'SYPD' due to invalid threshold config"],
        ),
        (
            {
                "SYPD": {
                    "THRESHOLD": 5.0,
                    "COMPARISON": "greater_than",
                    "%_ACCEPTED_ERROR": "invalid",
                }
            },
            {"SYPD": 3.9},
            ["Skipping CPMIP metric 'SYPD' due to invalid threshold config"],
        ),
        (
            {
                "SYPD": {
                    "THRESHOLD": 5.0,
                    "COMPARISON": "greater_than",
                    "%_ACCEPTED_ERROR": -10,
                }
            },
            {"SYPD": 3.9},
            ["Skipping CPMIP metric 'SYPD' due to invalid threshold config"],
        ),
        (
            {
                "SYPD": {
                    "THRESHOLD": 5.0,
                    "COMPARISON": "less_than",
                    "%_ACCEPTED_ERROR": 0,
                }
            },
            {"SYPD": 3.9},
            [],
        ),
        (
            {
                1: {
                    "THRESHOLD": 5.0,
                    "COMPARISON": "greater_than",
                    "%_ACCEPTED_ERROR": 10,
                }
            },
            {"SYPD": 3.9},
            [],
        ),
    ],
    ids=[
        "metrics_fetch_returns_empty",
        "metric_not_present_in_results",
        "malformed_threshold_config",
        "unsupported_comparison",
        "zero_accepted_error_valid",
        "invalid_threshold_type",
        "zero_threshold",
        "negative_threshold",
        "invalid_accepted_error_type",
        "negative_accepted_error",
        "accepted_error_zero",
        "non_string_metric_name",
    ],
)
def test_evaluate_non_violation_paths(mocker, metrics_data, thresholds, fetched_metrics, expected_log_calls):
    """Test that malformed or non-matching inputs do not produce violations and generate expected logs."""

    mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._fetch_metrics",
        return_value=fetched_metrics,
    )
    log_mock = mocker.patch("autosubmit.metrics.cpmip_metrics.Log.warning")

    violations = CPMIPMetrics.evaluate(metrics_data, thresholds)

    assert violations == {}
    
    assert log_mock.call_count == len(expected_log_calls)
    
    if expected_log_calls:
        for expected_log in expected_log_calls:
            calls_made = [str(call) for call in log_mock.call_args_list]
            assert any(expected_log in call for call in calls_made), f"Expected log message not found: {expected_log}. Actual calls: {calls_made}"


def test_evaluate_end_to_end_computes_values_and_returns_expected_violation(mocker):
    """End-to-end evaluation should compute metrics from raw data and report only real violations."""

    data = CPMIPMetricsData(
        start_time=0,
        end_time=43200,
        run_years=1.0,
        ncpus=64,
    )
    thresholds = {
        "SYPD": {
            "THRESHOLD": 3.0,
            "COMPARISON": "greater_than",
            "%_ACCEPTED_ERROR": 0,
        },
        "CORE_HOURS": {
            "THRESHOLD": 800.0,
            "COMPARISON": "less_than",
            "%_ACCEPTED_ERROR": 0,
        },
        "CHSY": {
            "THRESHOLD": 5000.0,
            "COMPARISON": "less_than",
            "%_ACCEPTED_ERROR": 0,
        },
    }
    log_mock = mocker.patch("autosubmit.metrics.cpmip_metrics.Log.warning")

    violations = CPMIPMetrics.evaluate(data, thresholds)

    assert list(violations.keys()) == ["SYPD"]
    assert violations["SYPD"]["comparison"] == "greater_than"
    assert violations["SYPD"]["threshold"] == pytest.approx(3.0)
    assert violations["SYPD"]["accepted_error"] == pytest.approx(0.0)
    assert violations["SYPD"]["bound"] == pytest.approx(3.0)
    assert violations["SYPD"]["real_value"] == pytest.approx(2.0)
    log_mock.assert_not_called()




@pytest.mark.parametrize(
    "start_time,end_time",
    [(44200, 1000), (2000, 2000)],
    ids=["runtime_end_before_start", "runtime_end_equal_start"],
)
def test_fetch_metrics_returns_empty_when_runtime_validation_fails(mocker, start_time, end_time):
    """Test that invalid runtime timestamps stop metric computation before runtime extraction."""

    data = CPMIPMetricsData(
        start_time=start_time,
        end_time=end_time,
        run_years=1.0,
        ncpus=64,
    )

    runtime_mock = mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._get_runtime_hours",
    )
    log_mock = mocker.patch("autosubmit.metrics.cpmip_metrics.Log.warning")

    metrics = CPMIPMetrics._fetch_metrics(data)

    runtime_mock.assert_not_called()
    assert metrics == {}
    log_mock.assert_called_once()
    assert "Unable to compute CPMIP metrics due to invalid runtime metadata" in str(log_mock.call_args)


@pytest.mark.parametrize(
    "ncpus,chsy_return_value,expected_metrics,expected_chsy_call,expected_log_calls",
    [
        (240, 5760.0, {"SYPD": 1.0, "CORE_HOURS": 2880.0, "CHSY": 5760.0}, (12.0, 1.0, 240), []),
        (None, 2880.0, {"SYPD": 1.0}, None, ["Skipping CORE_HOURS and CHSY metric computation because ncpus is missing"]),
    ],
    ids=["with_ncpus_computes_ch_and_chsy", "without_ncpus_returns_only_sypd"],
)
def test_fetch_metrics_with_and_without_ncpus(
    mocker,
    ncpus,
    chsy_return_value,
    expected_metrics,
    expected_chsy_call,
    expected_log_calls,
):
    """Test that fetch_metrics includes CHSY only when ncpus is available and logs appropriately."""

    data = CPMIPMetricsData(
        start_time=1000,
        end_time=44200,
        run_years=1.0,
        ncpus=ncpus,
    )

    sypd_mock = mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._simulated_years_per_day",
        return_value=1.0,
    )
    core_hours_mock = mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._core_hours",
        return_value=2880.0,
    )
    chsy_mock = mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._core_hours_per_simulated_year",
        return_value=chsy_return_value,
    )
    log_mock = mocker.patch("autosubmit.metrics.cpmip_metrics.Log.warning")

    metrics = CPMIPMetrics._fetch_metrics(data)

    sypd_mock.assert_called_once_with(12.0, 1.0)

    if expected_chsy_call is None:
        core_hours_mock.assert_not_called()
        chsy_mock.assert_not_called()
    else:
        core_hours_mock.assert_called_once_with(12.0, 240)
        chsy_mock.assert_called_once_with(*expected_chsy_call)

    assert metrics == expected_metrics
    
    assert log_mock.call_count == len(expected_log_calls)
    if expected_log_calls:
        for expected_log in expected_log_calls:
            calls_made = [str(call) for call in log_mock.call_args_list]
            assert any(expected_log in call for call in calls_made), f"Expected log message not found: {expected_log}"

def test_fetch_metrics_continues_when_run_years_is_missing(mocker):
    """Missing run_years should skip simulated-years-based metrics and still compute CORE_HOURS."""

    data = CPMIPMetricsData(
        start_time=1000,
        end_time=44200,
        run_years=None,
        ncpus=64,
    )

    log_mock = mocker.patch("autosubmit.metrics.cpmip_metrics.Log.warning")

    metrics = CPMIPMetrics._fetch_metrics(data)

    assert metrics == {"CORE_HOURS": 768.0}
    log_mock.assert_not_called()


@pytest.mark.parametrize(
    "chsy_error",
    [ValueError("invalid chsy"), TypeError("invalid chsy type")],
    ids=["chsy_value_error", "chsy_type_error"],
)
def test_fetch_metrics_returns_only_sypd_and_ch_when_chsy_computation_fails(mocker, chsy_error):
    """Test that CHSY failures still return SYPD and CORE_HOURS and log warning."""

    data = CPMIPMetricsData(
        start_time=1000,
        end_time=44200,
        run_years=1.0,
        ncpus=64,
    )

    runtime_mock = mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._get_runtime_hours",
        return_value=12.0,
    )
    sypd_mock = mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._simulated_years_per_day",
        return_value=1.0,
    )
    core_hours_mock = mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._core_hours",
        return_value=2880.0,
    )
    chsy_mock = mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._core_hours_per_simulated_year",
        side_effect=chsy_error,
    )
    log_mock = mocker.patch("autosubmit.metrics.cpmip_metrics.Log.warning")

    metrics = CPMIPMetrics._fetch_metrics(data)

    runtime_mock.assert_called_once_with(data)
    sypd_mock.assert_called_once_with(12.0, 1.0)
    core_hours_mock.assert_called_once_with(12.0, 64)
    chsy_mock.assert_called_once_with(12.0, 1.0, 64)
    assert metrics == {"SYPD": 1.0, "CORE_HOURS": 2880.0}
    
    log_mock.assert_called_once()
    assert "Unable to compute CHSY metric" in str(log_mock.call_args)


@pytest.mark.parametrize(
    "core_hours_error",
    [ValueError("invalid core hours"), TypeError("invalid core hours type")],
    ids=["core_hours_value_error", "core_hours_type_error"],
)
def test_fetch_metrics_returns_only_sypd_when_core_hours_computation_fails(mocker, core_hours_error):
    """Test that CORE_HOURS failures still return SYPD and log warning."""

    data = CPMIPMetricsData(
        start_time=1000,
        end_time=44200,
        run_years=1.0,
        ncpus=64,
    )

    runtime_mock = mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._get_runtime_hours",
        return_value=12.0,
    )
    sypd_mock = mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._simulated_years_per_day",
        return_value=1.0,
    )
    core_hours_mock = mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._core_hours",
        side_effect=core_hours_error,
    )
    chsy_mock = mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._core_hours_per_simulated_year",
    )
    log_mock = mocker.patch("autosubmit.metrics.cpmip_metrics.Log.warning")

    metrics = CPMIPMetrics._fetch_metrics(data)

    runtime_mock.assert_called_once_with(data)
    sypd_mock.assert_called_once_with(12.0, 1.0)
    core_hours_mock.assert_called_once_with(12.0, 64)
    chsy_mock.assert_not_called()
    assert metrics == {"SYPD": 1.0}
    
    log_mock.assert_called_once()
    assert "Unable to compute CORE_HOURS metric" in str(log_mock.call_args)




@pytest.mark.parametrize(
    "data",
    [
        CPMIPMetricsData(start_time=1000, end_time=2000, run_years=1.0, ncpus=64),
        CPMIPMetricsData(start_time=1000, end_time=2000, run_years=1.0, ncpus=None),
        CPMIPMetricsData(start_time=1000, end_time=2000, run_years=None, ncpus=64),
    ],
    ids=[
        "all_fields_valid",
        "optional_ncpus_none",
        "optional_run_years_none",
    ],
)
def test_validate_metrics_data_success_paths(data):
    """Validate metrics data success paths across supported field combinations."""

    CPMIPMetrics.validate_metrics_data(data)


@pytest.mark.parametrize(
    "data,expected_exception,error_message",
    [
        (
            CPMIPMetricsData(start_time=None, end_time=2000, run_years=1.0, ncpus=64),
            ValueError,
            "start_time is required",
        ),
        (
            CPMIPMetricsData(start_time=1000, end_time=None, run_years=1.0, ncpus=64),
            ValueError,
            "end_time is required",
        ),
        (
            CPMIPMetricsData(start_time=cast(Any, "1000"), end_time=2000, run_years=1.0, ncpus=64),
            TypeError,
            "start_time must be an integer value",
        ),
        (
            CPMIPMetricsData(start_time=1000, end_time=cast(Any, 2000.5), run_years=1.0, ncpus=64),
            TypeError,
            "end_time must be an integer value",
        ),
    ],
    ids=[
        "missing_required_start_time",
        "missing_required_end_time",
        "invalid_start_time_type",
        "invalid_end_time_type",
    ],
)
def test_validate_metrics_data_unsuccessful_paths(data, expected_exception, error_message):
    """Validate metrics data failure paths across all validated input fields."""

    with pytest.raises(expected_exception, match=error_message):
        CPMIPMetrics.validate_metrics_data(data)

@pytest.mark.parametrize(
    "data,expected_field",
    [
        (
            CPMIPMetricsData(start_time=1000, end_time=2000, run_years=cast(Any, "1"), ncpus=64),
            "run_years",
        ),
        (
            CPMIPMetricsData(start_time=1000, end_time=2000, run_years=0, ncpus=64),
            "run_years",
        ),
        (
            CPMIPMetricsData(start_time=1000, end_time=2000, run_years=1, ncpus=64),
            "run_years",
        ),
        (
            CPMIPMetricsData(start_time=1000, end_time=2000, run_years=1.0, ncpus=cast(Any, "64")),
            "ncpus",
        ),
        (
            CPMIPMetricsData(start_time=1000, end_time=2000, run_years=1.0, ncpus=0),
            "ncpus",
        ),
    ],
    ids=[
        "invalid_run_years_type_degrades",
        "invalid_run_years_non_positive_degrades",
        "invalid_run_years_integer_degrades",
        "invalid_ncpus_type_degrades",
        "invalid_ncpus_non_positive_degrades",
    ],
)
def test_validate_metrics_data_optional_invalid_fields_degrade_to_none(data, expected_field):
    """Optional invalid fields are normalized to None instead of raising."""

    CPMIPMetrics.validate_metrics_data(data)

    assert getattr(data, expected_field) is None


def test_validate_against_schema_allowed_values_case_sensitive_error_path():
    """Non-case-insensitive allowed_values should reject values outside the literal list."""

    schema_rules = {
        "type": str,
        "type_label": "a string",
        "required": False,
        "allowed_values": ["A", "B"],
        "case_insensitive": False,
    }

    with pytest.raises(ValueError, match="must be one of"):
        CPMIPMetrics._validate_against_schema("C", "dummy_field", schema_rules)


@pytest.mark.parametrize(
    "metric_name,threshold_config",
    [
        ("SYPD", {"threshold": 5.0, "comparison": "greater_than", "%_accepted_error": 10}),
        ("CORE_HOURS", {"threshold": 100.0, "comparison": "less_than", "%_accepted_error": 5}),
        ("CHSY", {"threshold": 500.0, "comparison": "greater_than", "%_accepted_error": 0}),
    ],
    ids=["sypd_valid_threshold", "core_hours_valid_threshold", "chsy_valid_threshold"],
)
def test_validate_threshold_config_success_paths_for_all_metrics(metric_name, threshold_config):
    """Validate threshold-config success path for all supported metric names."""

    config = CPMIPMetrics._validate_threshold_config(metric_name, threshold_config)

    assert config["threshold"] == float(threshold_config["threshold"])
    assert config["comparison"] == threshold_config["comparison"]
    assert config["accepted_error"] == float(threshold_config["%_accepted_error"])


@pytest.mark.parametrize(
    "metric_name,threshold_config,error_message",
    [
        ("SYPD", {"threshold": 5.0, "comparison": "equal_to", "%_accepted_error": 10}, "Unsupported comparison operator"),
        ("CORE_HOURS", {"threshold": "invalid", "comparison": "greater_than", "%_accepted_error": 10}, "Invalid threshold configuration"),
        ("CHSY", {"threshold": 5.0, "comparison": "greater_than"}, "Invalid threshold configuration"),
    ],
    ids=["sypd_invalid_comparison", "core_hours_invalid_threshold_type", "chsy_missing_accepted_error"],
)
def test_validate_threshold_config_unsuccessful_paths_for_all_metrics(metric_name, threshold_config, error_message):
    """Validate threshold-config failure path for all supported metric names."""

    with pytest.raises(ValueError, match=error_message):
        CPMIPMetrics._validate_threshold_config(metric_name, threshold_config)

@pytest.mark.parametrize(
    "start_time,end_time,error_message",
    [
        (5, 4, "end_time must be greater than start_time"),
        (5, 5, "end_time must be greater than start_time"),
    ],
    ids=[
        "timestamp_order_invalid_less",
        "timestamp_order_invalid",
    ],
)
def test_validate_timestamp_pair_error_paths(mocker, start_time, end_time, error_message):
    """Cover timestamp validator error branches with one parametrized test."""

    log_mock = mocker.patch("autosubmit.metrics.cpmip_metrics.Log.warning")

    assert CPMIPMetrics._validate_timestamp_pair(start_time, end_time) is False
    log_mock.assert_called_once()
    assert error_message in str(log_mock.call_args)


def test_validate_timestamp_pair_success_path():
    """Valid timestamp pair should return True."""

    assert CPMIPMetrics._validate_timestamp_pair(1, 2) is True


@pytest.mark.parametrize(
    "metric_function,args,expected",
    [
        (CPMIPMetrics._simulated_years_per_day, (12.0, 1.0), 2.0),
        (CPMIPMetrics._core_hours, (12.0, 120), 1440.0),
        (CPMIPMetrics._core_hours_per_simulated_year, (12.0, 1.0, 120), 1440.0),
    ],
    ids=[
        "sypd_formula",
        "core_hours_formula",
        "chsy_formula",
    ],
)
def test_private_formula_helpers(metric_function, args, expected):
    """Validate private metric formula helpers in a compact parametrized test."""

    assert metric_function(*args) == pytest.approx(expected)


def test_fetch_metrics_returns_empty_when_data_object_invalid(mocker):
    """Invalid data objects are rejected at validation and produce an empty metrics dict."""

    log_mock = mocker.patch("autosubmit.metrics.cpmip_metrics.Log.warning")

    metrics = CPMIPMetrics._fetch_metrics(cast(Any, {"invalid": "data"}))

    assert metrics == {}
    log_mock.assert_called_once()
    assert "Unable to compute CPMIP metrics due to invalid data" in str(log_mock.call_args)
    assert "data must be a CPMIPMetricsData object" in str(log_mock.call_args)


@pytest.mark.parametrize("thresholds", [None, [], "invalid", 1], ids=["none", "list", "string", "int"])
def test_evaluate_raises_when_thresholds_is_not_dict(metrics_data, thresholds):
    """Evaluate must reject non-dict thresholds."""

    with pytest.raises(TypeError, match="thresholds must be a dictionary"):
        CPMIPMetrics.evaluate(metrics_data, thresholds)
