# Copyright 2015-2026 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

"""Tests for the benchmark comparison script (.benchmarks/compare_results.py).

These tests validate the logic that compares pytest-benchmark runs against a
baseline and flags regressions, using synthetic runs built in-memory.
"""

import importlib.util
from pathlib import Path

import pytest

_BENCH_DIR = Path(__file__).parents[2] / ".benchmarks"
_spec = importlib.util.spec_from_file_location("compare_results", _BENCH_DIR / "compare_results.py")
assert _spec is not None and _spec.loader is not None
compare = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(compare)

_THRESHOLDS_PATH = _BENCH_DIR / "thresholds.yml"


def _make_entry(name: str, test_type: str, run_id: str, median: float, **extra) -> dict:
    return {
        "name": name,
        "fullname": name,
        "stats": {"median": median},
        "extra_info": {"test type": test_type, "ID": run_id, **extra},
    }


def _make_run(*entries) -> dict:
    return {
        "machine_info": {"cpu": {"brand_raw": "Test CPU"}, "python_version": "3.11.14"},
        "benchmarks": list(entries),
    }


def _thresholds() -> dict:
    return compare._load_thresholds(_THRESHOLDS_PATH)


def _time_threshold() -> float:
    return float(_thresholds()["metrics"]["Time Taken(Seconds)"]["threshold"])


def _time_floor() -> float:
    return float(_thresholds()["metrics"]["Time Taken(Seconds)"]["floor"])


def _baseline_entry(**overrides):
    extra = {
        "Memory consumption(MiB)": 543.73,
        "Historical DB Disk Usage(MiB)": 0.02,
        "Job list DB Usage": 0.03,
        "Total Jobs": 7,
        "Total Dependencies": 7,
        "FD GROW": None,
        "MEM GROW(MIB)": 0,
        "OBJ GROW": None,
    }
    extra.update(overrides)
    return extra


def test_no_baseline_renders_current_only():
    run = _make_run(_make_entry("create", "create", "fc0_1_1", 0.7, **_baseline_entry()))
    frame = compare.build_frame([run])
    report = compare.evaluate(frame, None, _thresholds())

    assert not report.empty
    assert (report["verdict"] == "N/A").all()

    markdown = compare.render_markdown(report, "4.2.0", "Current", None, None)
    assert "**Baseline:** None" in markdown
    assert "No regressions detected" in markdown


def test_time_regression_warns():
    thr = _time_threshold()
    baseline = _make_run(_make_entry("run", "run", "fc0_1_1", 10.0, **_baseline_entry()))
    current = _make_run(_make_entry("run", "run", "fc0_1_1", 10.0 * (1 + (thr + 10) / 100),
                                     **_baseline_entry()))

    report = compare.evaluate(
        compare.build_frame([current]), compare.build_frame([baseline]), _thresholds()
    )
    row = report[report["metric"] == "Time Taken(Seconds)"].iloc[0]
    assert row["verdict"] == "WARN"
    assert row["delta %"] == pytest.approx(thr + 10)


def test_floor_suppresses_small_values():
    floor = _time_floor()
    baseline = _make_run(_make_entry("create", "create", "fc0_1_1", 0.01, **_baseline_entry()))
    current = _make_run(_make_entry("create", "create", "fc0_1_1", floor * 0.5, **_baseline_entry()))

    report = compare.evaluate(
        compare.build_frame([current]), compare.build_frame([baseline]), _thresholds()
    )
    # Huge relative increase, but the current value stays below the configured
    # floor, so the regression is suppressed.
    row = report[report["metric"] == "Time Taken(Seconds)"].iloc[0]
    assert row["verdict"] == "PASS"


def test_exact_metric_change_warns():
    baseline = _make_run(_make_entry("create", "create", "fc0_1_1", 0.7, **_baseline_entry()))
    current = _make_run(_make_entry("create", "create", "fc0_1_1", 0.7,
                                    **{**_baseline_entry(), "Total Jobs": 8}))

    report = compare.evaluate(
        compare.build_frame([current]), compare.build_frame([baseline]), _thresholds()
    )
    row = report[report["metric"] == "Total Jobs"].iloc[0]
    assert row["verdict"] == "WARN"


def test_baseline_uses_median_over_multiple_runs():
    baseline = [
        _make_run(_make_entry("run", "run", "fc0_1_1", 10.0, **_baseline_entry())),
        _make_run(_make_entry("run", "run", "fc0_1_1", 14.0, **_baseline_entry())),
    ]
    current = _make_run(_make_entry("run", "run", "fc0_1_1", 10.0, **_baseline_entry()))

    report = compare.evaluate(
        compare.build_frame([current]), compare.build_frame(baseline), _thresholds()
    )
    row = report[report["metric"] == "Time Taken(Seconds)"].iloc[0]
    # median baseline is 12.0; current 10.0 is an improvement, so no warning
    assert row["verdict"] == "PASS"


def test_environment_warning_detects_different_cpu():
    current = [_make_run(_make_entry("run", "run", "fc0_1_1", 1.0, **_baseline_entry()))]
    previous = [_make_run(_make_entry("run", "run", "fc0_1_1", 1.0, **_baseline_entry()))]
    previous[0]["machine_info"]["cpu"]["brand_raw"] = "Other CPU"

    warning = compare.environment_warning(current, previous)
    assert warning is not None
    assert "Environment differs" in warning


def test_render_markdown_flags_regressions():
    thr = _time_threshold()
    baseline = _make_run(_make_entry("run", "run", "fc0_1_1", 10.0, **_baseline_entry()))
    current = _make_run(_make_entry("run", "run", "fc0_1_1", 10.0 * (1 + (thr + 10) / 100),
                                     **_baseline_entry()))
    report = compare.evaluate(
        compare.build_frame([current]), compare.build_frame([baseline]), _thresholds()
    )
    markdown = compare.render_markdown(report, "4.2.0", "Current", "Baseline", None)
    assert "## ⚠️ Regressions detected" in markdown
    assert "run/fc0_1_1" in markdown
    assert "Time Taken(Seconds)" in markdown


def test_current_directory_uses_newest_run_only(tmp_path: Path):
    import os
    import time

    older = tmp_path / "0001_old.json"
    newer = tmp_path / "0002_new.json"
    older.write_text("{}", encoding="UTF-8")
    newer.write_text("{}", encoding="UTF-8")
    now = time.time()
    os.utime(older, (now - 10, now - 10))
    os.utime(newer, (now, now))

    files = compare._iter_run_files(str(tmp_path), latest_only=True)
    assert files == [newer]
