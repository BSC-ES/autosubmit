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

from dataclasses import dataclass
from typing import Any, Optional, Union

from autosubmit.log.log import Log


@dataclass
class CPMIPMetricsData:
    """
    Container for CPMIP metrics computation inputs.
    
    Holds the essential runtime and simulation metadata needed to compute CPMIP metrics.
    This decouples metrics computation from job object structure.
    
    :param start_time: Unix timestamp (seconds) when the job started.
    :type start_time: Optional[int]
    :param end_time: Unix timestamp (seconds) when the job ended.
    :type end_time: Optional[int]
    :param run_years: Length of the simulation in years. Can be None.
    :type run_years: Optional[float]
    :param ncpus: Number of CPUs used by the job. Can be None if unavailable.
    :type ncpus: Optional[int]
    """
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    run_years: Optional[float] = None
    ncpus: Optional[int] = None


class CPMIPMetrics:
    """Evaluates CPMIP performance metrics against configured thresholds."""

    METRIC_NAME_ALIASES = {
        "SYPD": ("SYPD", "simulated_years_per_day"),
        "CORE_HOURS": ("CORE_HOURS", "core_hours"),
        "CHSY": ("CHSY", "core_hours_per_simulated_year"),
    }
    _METRIC_NAME_LOOKUP = {
        alias.strip().lower(): metric_name
        for metric_name, aliases in METRIC_NAME_ALIASES.items()
        for alias in aliases
    }

    _VALIDATION_SCHEMA = {
        "start_time": {
            "type": int,
            "type_label": "an integer value",
            "required": True,
            "positive": False,
        },
        "end_time": {
            "type": int,
            "type_label": "an integer value",
            "required": True,
            "positive": False,
        },
        "run_years": {
            "type": float,
            "type_label": "a float value",
            "required": False,
            "positive": True,
        },
        "ncpus": {
            "type": int,
            "type_label": "an integer value",
            "required": False,
            "positive": True,
        },
    }

    @staticmethod
    def _validate_positive(
        value: Union[int, float],
        var_name: str,
        error_message: Optional[str] = None,
        allow_zero: bool = False,
    ) -> None:
        """
        Validate that a value is positive, optionally allowing zero.

        :param value: Value to validate.
        :type value: Union[int, float]
        :param var_name: Variable name for error messages.
        :type var_name: str
        :param error_message: Optional custom message for invalid values.
        :type error_message: Optional[str]
        :param allow_zero: Whether value can be equal to zero.
        :type allow_zero: bool
        :raises ValueError: If value is invalid for the configured positivity rule.
        """
        if allow_zero:
            if value < 0:
                raise ValueError(error_message or f"{var_name} must be >= 0")
            return

        if value <= 0:
            raise ValueError(error_message or f"{var_name} must be > 0")

    @staticmethod
    def _validate_against_schema(value: Any, field_name: str, schema_rules: dict[str, Any]) -> Any:
        """
        Validate a value against schema rules and return normalized value.

        :param value: Value to validate (can be None).
        :type value: any
        :param field_name: Field name for error messages.
        :type field_name: str
        :param schema_rules: Dictionary with validation rules (type, allowed_values, positive, etc.).
        :type schema_rules: dict
        :return: Normalized value (e.g., lowercased strings if case_insensitive=True).
        :rtype: any
        :raises TypeError: If type is invalid.
        :raises ValueError: If value violates constraints.
        """

        if value is None:
            if schema_rules.get("required", False):
                raise ValueError(f"{field_name} is required")
            return None


        expected_type = schema_rules.get("type")
        if expected_type and not isinstance(value, expected_type):
            type_label = schema_rules.get("type_label", f"{expected_type.__name__}")
            raise TypeError(f"{field_name} must be {type_label}")


        if schema_rules.get("positive", False):
            CPMIPMetrics._validate_positive(
                value,
                field_name,
                error_message=f"{field_name} must be > 0",
            )


        allowed_values = schema_rules.get("allowed_values")
        if allowed_values:
            case_insensitive = schema_rules.get("case_insensitive", False)
            check_value = value.strip().lower() if case_insensitive and isinstance(value, str) else value

            if case_insensitive and isinstance(value, str):
                allowed_lower = [v.lower() for v in allowed_values]
                if check_value not in allowed_lower:
                    raise ValueError(
                        f"{field_name} must be one of {allowed_values}, got: {value}"
                    )
            else:
                if value not in allowed_values:
                    raise ValueError(
                        f"{field_name} must be one of {allowed_values}, got: {value}"
                    )

        if schema_rules.get("case_insensitive", False) and isinstance(value, str):
            return value.strip().lower()

        return value

    @staticmethod
    def validate_metrics_data(data: CPMIPMetricsData) -> None:
        """
        Validate all fields of CPMIPMetricsData using the schema.

        :param data: CPMIPMetricsData object to validate.
        :type data: CPMIPMetricsData
        :raises TypeError: If data is not a CPMIPMetricsData instance.
        :raises ValueError: If any field violates schema constraints.
        """
        if not isinstance(data, CPMIPMetricsData):
            raise TypeError("data must be a CPMIPMetricsData object")

        for field_name, schema_rules in CPMIPMetrics._VALIDATION_SCHEMA.items():
            field_value = getattr(data, field_name)
            try:
                normalized_value = CPMIPMetrics._validate_against_schema(field_value, field_name, schema_rules)
                setattr(data, field_name, normalized_value)
            except (TypeError, ValueError):
                if schema_rules.get("required", False):
                    raise
                # Optional fields degrade to None so non-required metric paths can continue.
                setattr(data, field_name, None)

    @staticmethod
    def _validate_timestamp_pair(start_time: Optional[int], end_time: Optional[int]) -> bool:
        """
        Validate start_time and end_time, ensuring end_time > start_time.

        :param start_time: Unix timestamp (seconds) when job started.
        :type start_time: Optional[int]
        :param end_time: Unix timestamp (seconds) when job ended.
        :type end_time: Optional[int]
        :return: True when timestamps are valid for runtime computation, otherwise False.
        :rtype: bool
        """
        if start_time is None or end_time is None:
            Log.warning(
                "Unable to compute CPMIP metrics due to invalid runtime metadata: "
                "start_time and end_time are required"
            )
            return False

        if end_time <= start_time:
            Log.warning(
                "Unable to compute CPMIP metrics due to invalid runtime metadata: "
                "end_time must be greater than start_time"
            )
            return False

        return True

    @staticmethod
    def _validate_threshold_config(metric_name: str, threshold_config: dict) -> dict:
        """
        Validate and normalize threshold configuration for a metric.

        :param metric_name: Metric name from thresholds dict.
        :type metric_name: str
        :param threshold_config: Configuration dict with threshold, comparison, %_accepted_error.
        :type threshold_config: dict
        :return: Normalized config dict with parsed values.
        :rtype: dict
        :raises ValueError: If config is invalid or missing required keys.
        :raises TypeError: If values have invalid types.
        """
        try:
            normalized_config = {
                str(key).strip().lower(): value
                for key, value in threshold_config.items()
            }

            threshold = float(normalized_config["threshold"])
            CPMIPMetrics._validate_positive(
                threshold,
                f"{metric_name} threshold",
                error_message=f"{metric_name} threshold must be > 0",
            )

            comparison = str(normalized_config["comparison"]).strip().lower()

            accepted_error = float(normalized_config["%_accepted_error"])
            CPMIPMetrics._validate_positive(
                accepted_error,
                f"{metric_name} accepted error",
                error_message=f"{metric_name} accepted error must be >= 0",
                allow_zero=True,
            )

            if comparison not in ["greater_than", "less_than"]:
                raise ValueError(
                    f"Unsupported comparison operator: {comparison}"
                )

            return {
                "threshold": threshold,
                "comparison": comparison,
                "accepted_error": accepted_error,
            }
        except (ValueError, TypeError, KeyError) as error:
            raise ValueError(
                f"Invalid threshold configuration for '{metric_name}': {error}"
            ) from error

    @staticmethod
    def _simulated_years_per_day(runtime: float, simulated_years: float) -> float:
        """
        Compute Simulated Years Per Day from runtime and simulated years.

        Assumes inputs are pre-validated. Called from _fetch_metrics() only.

        :param runtime: Runtime of a Job in hours.
        :type runtime: float
        :param simulated_years: Simulated years.
        :type simulated_years: float
        :return: Simulated years per day.
        :rtype: float
        """
        return simulated_years * 24.0 / runtime

    @staticmethod
    def _core_hours(runtime: float, ncpus: int) -> float:
        """
        Compute core-hours from runtime and CPU count.

        Assumes inputs are pre-validated. Called from _fetch_metrics() and _core_hours_per_simulated_year() only.

        :param runtime: Runtime of a Job in hours.
        :type runtime: float
        :param ncpus: Number of CPUs used by the job.
        :type ncpus: int
        :return: Core-hours.
        :rtype: float
        """
        return ncpus * runtime

    @staticmethod
    def _core_hours_per_simulated_year(runtime: float, simulated_years: float, ncpus: int) -> float:
        """
        Compute Core-Hours per Simulated Year.

        Assumes inputs are pre-validated. Called from _fetch_metrics() only.

        :param runtime: Runtime of a Job in hours.
        :type runtime: float
        :param simulated_years: Simulated years.
        :type simulated_years: float
        :param ncpus: Number of CPUs used by the job.
        :type ncpus: int
        :return: Core-hours per simulated year.
        :rtype: float
        """
        return CPMIPMetrics._core_hours(runtime, ncpus) / simulated_years

    @staticmethod
    def _get_runtime_hours(data: CPMIPMetricsData) -> float:
        """
        Extract runtime in hours from metrics data timestamps.

        :param data: CPMIPMetricsData object with start/end times.
        :type data: CPMIPMetricsData
        :return: Runtime in hours.
        :rtype: float
        """
        if data.start_time is None or data.end_time is None:
            raise ValueError("start_time and end_time are required to compute runtime")

        runtime_seconds = data.end_time - data.start_time
        return runtime_seconds / 3600

    @staticmethod
    def _fetch_metrics(data: CPMIPMetricsData) -> dict[str, float]:
        """
        Build CPMIP metrics from runtime and simulation metadata.

        :param data: CPMIPMetricsData object.
        :type data: CPMIPMetricsData
        :return: Dictionary with computed metrics. Empty dict when required metadata is missing or invalid.
               If run_years metadata is missing/invalid, run_years-dependent metrics can fail while
             runtime-dependent metrics (for example CORE_HOURS) can still be returned.
        :rtype: dict[str, float]
           :raises ValueError: If timestamp or run_years metadata is invalid.
        :raises TypeError: If data types are invalid.
        """

        try:
            CPMIPMetrics.validate_metrics_data(data)
        except (ValueError, TypeError) as error:
            Log.warning(f"Unable to compute CPMIP metrics due to invalid data: {error}")
            return {}

        if not CPMIPMetrics._validate_timestamp_pair(data.start_time, data.end_time):
            return {}

        runtime_hours = CPMIPMetrics._get_runtime_hours(data)
        simulated_years = data.run_years

        metrics: dict[str, float] = {}
        ncpus = data.ncpus

        if ncpus is None:
            Log.warning("Skipping CORE_HOURS and CHSY metric computation because ncpus is missing")

        if simulated_years is not None:
            try:
                metrics["SYPD"] = CPMIPMetrics._simulated_years_per_day(runtime_hours, simulated_years)
            except (ValueError, TypeError) as error:
                Log.warning(f"Unable to compute SYPD metric: {error}")

        if ncpus is not None:
            try:
                metrics["CORE_HOURS"] = CPMIPMetrics._core_hours(runtime_hours, ncpus)
            except (ValueError, TypeError) as error:
                Log.warning(f"Unable to compute CORE_HOURS metric: {error}")

        if ncpus is not None and simulated_years is not None and "CORE_HOURS" in metrics:
            try:
                metrics["CHSY"] = CPMIPMetrics._core_hours_per_simulated_year(
                    runtime_hours,
                    simulated_years,
                    ncpus,
                )
            except (ValueError, TypeError) as error:
                Log.warning(f"Unable to compute CHSY metric: {error}")

        return metrics

    @staticmethod
    def _resolve_metric_for_evaluation(metric_name: str, metrics: dict) -> Optional[str]:
        """
        Resolve a metric alias to its canonical name and ensure it was computed.

        :param metric_name: Metric alias provided by the threshold configuration.
        :type metric_name: str
        :param metrics: Dictionary of already computed metrics.
        :type metrics: dict
        :return: Canonical metric name if resolvable and present in computed metrics, otherwise None.
        :rtype: Optional[str]
        """
        canonical_metric_name = CPMIPMetrics._METRIC_NAME_LOOKUP.get(metric_name.strip().lower())
        if canonical_metric_name is None:
            Log.warning(f"Ignoring unknown CPMIP metric alias: {metric_name}")
            return None

        if canonical_metric_name not in metrics:
            Log.warning(
                f"Skipping CPMIP metric '{metric_name}' ({canonical_metric_name}) because it was not computed"
            )
            return None

        return canonical_metric_name

    @staticmethod
    def _extract_threshold_for_evaluation(metric_name: str, threshold_config: dict) -> Optional[dict]:
        """
        Validate threshold configuration and normalize it for evaluation.

        :param metric_name: Metric name from the thresholds configuration.
        :type metric_name: str
        :param threshold_config: Raw threshold configuration dictionary.
        :type threshold_config: dict
        :return: Normalized threshold configuration dictionary, or None if invalid.
        :rtype: Optional[dict]
        """
        try:
            return CPMIPMetrics._validate_threshold_config(metric_name, threshold_config)
        except ValueError as error:
            Log.warning(f"Skipping CPMIP metric '{metric_name}' due to invalid threshold config: {error}")
            return None

    @staticmethod
    def _compute_violation(
        real_value: float,
        threshold: float,
        comparison: str,
        accepted_error: float,
    ) -> Optional[dict]:
        """
        Compute violation payload for a single metric.

        Assumes comparison has already been validated by _validate_threshold_config.

        :param real_value: Computed metric value.
        :type real_value: float
        :param threshold: Threshold value to compare against.
        :type threshold: float
        :param comparison: Comparison operator, either greater_than or less_than.
        :type comparison: str
        :param accepted_error: Accepted error percentage.
        :type accepted_error: float
        :return: Violation payload when out of bounds, otherwise None.
        :rtype: Optional[dict]
        """
        tolerance_factor = accepted_error / 100.0
        is_violation = False

        if comparison == "greater_than":
            lower_bound = threshold * (1 - tolerance_factor)
            bound = lower_bound
            if real_value < lower_bound:
                is_violation = True
        else:  # comparison == "less_than"
            upper_bound = threshold * (1 + tolerance_factor)
            bound = upper_bound
            if real_value > upper_bound:
                is_violation = True

        if not is_violation:
            return None

        return {
            "threshold": threshold,
            "accepted_error": accepted_error,
            "comparison": comparison,
            "bound": bound,
            "real_value": real_value,
        }

    @staticmethod
    def evaluate(data: CPMIPMetricsData, thresholds: dict[str, dict]) -> dict:
        """
        Evaluate metrics against threshold definitions.

        :param data: CPMIPMetricsData object used to fetch computed metrics.
        :type data: CPMIPMetricsData
        :param thresholds: Threshold configuration by metric name.
        :type thresholds: dict[str, dict]
        :return: Violations by metric name.
        :rtype: dict[str, dict]
        """
        if not isinstance(thresholds, dict):
            raise TypeError("thresholds must be a dictionary")

        if not thresholds:
            return {}

        metrics = CPMIPMetrics._fetch_metrics(data)
        if not metrics:
            return {}

        violations = {}

        for metric_name, threshold_config in thresholds.items():
            if not isinstance(metric_name, str):
                continue

            canonical_metric_name = CPMIPMetrics._resolve_metric_for_evaluation(metric_name, metrics)
            if canonical_metric_name is None:
                continue

            config = CPMIPMetrics._extract_threshold_for_evaluation(metric_name, threshold_config)
            if config is None:
                continue

            violation = CPMIPMetrics._compute_violation(
                real_value=metrics[canonical_metric_name],
                threshold=config["threshold"],
                comparison=config["comparison"],
                accepted_error=config["accepted_error"],
            )
            if violation is not None:
                violations[canonical_metric_name] = violation

        return violations
