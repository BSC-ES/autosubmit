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
import os
import time
from pathlib import Path

import pandas as pd
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
        "extra_info": {"test type": test_type, "ID": run_id, "base": run_id.split("·", 1)[0], **extra},
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
        "FD GROWTH": None,
        "MEM GROWTH(MIB)": 0,
    }
    extra.update(overrides)
    return extra


def _evaluate_pair(baseline_time: float | None, current_time: float,
                   test_type: str = "run", run_id: str = "fc0_1_1") -> pd.DataFrame:
    """Build a current/baseline frame pair and evaluate them."""
    current = _make_run(_make_entry(test_type, test_type, run_id, current_time, **_baseline_entry()))
    if baseline_time is None:
        return compare.evaluate(compare.build_frame(current), None, _thresholds())
    baseline = _make_run(_make_entry(test_type, test_type, run_id, baseline_time, **_baseline_entry()))
    return compare.evaluate(
        compare.build_frame(current), compare.build_frame(baseline), _thresholds()
    )


@pytest.mark.parametrize("baseline_time, current_time, previous_label, env_warning, expected", [
    pytest.param(None, 0.7, None, None,
                 ["**Baseline:** None", "No regressions detected"], id="no-baseline-current-only"),
    pytest.param(10.0, 10.0 * (1 + (_time_threshold() + 10) / 100), "Baseline", None,
                 ["Regressions detected", "run/fc0_1_1", "Time Taken(Seconds)"],
                 id="flags-regressions"),
    pytest.param(10.0, 10.0, "Baseline",
                 "Environment differs from baseline: current ran on `A` "
                 "while the baseline ran on `B`.",
                 ["Environment differs from baseline"], id="includes-environment-warning"),
])
def test_render_markdown_variants(baseline_time, current_time, previous_label, env_warning, expected):
    report = _evaluate_pair(baseline_time, current_time)

    markdown = compare.render_markdown(report, "4.2.0", "Current",
                                       previous_label=previous_label, env_warning=env_warning)
    for fragment in expected:
        assert fragment in markdown


@pytest.mark.parametrize("baseline_time, current_time, expected, expected_delta", [
    pytest.param(10.0, 10.0 * (1 + (_time_threshold() + 10) / 100), "WARN",
                 pytest.approx(_time_threshold() + 10), id="over-threshold-warns"),
    pytest.param(0.01, _time_floor() * 0.5, "PASS", None, id="under-floor-suppressed"),
])
def test_time_verdicts(baseline_time, current_time, expected, expected_delta):
    baseline = _make_run(_make_entry("run", "run", "fc0_1_1", baseline_time, **_baseline_entry()))
    current = _make_run(_make_entry("run", "run", "fc0_1_1", current_time, **_baseline_entry()))

    report = compare.evaluate(
        compare.build_frame(current), compare.build_frame(baseline), _thresholds()
    )
    row = report[report["metric"] == "Time Taken(Seconds)"].iloc[0]
    assert row["verdict"] == expected
    if expected_delta is not None:
        assert row["delta %"] == expected_delta


@pytest.mark.parametrize("metric, baseline_val, current_val, expected", [
    pytest.param("Total Jobs", 7, 8, "WARN", id="total-jobs-change-warns"),
    pytest.param("Total Dependencies", 7, 8, "WARN", id="total-deps-change-warns"),
    pytest.param("Total Jobs", 7, 7, "PASS", id="total-jobs-unchanged-passes"),
    pytest.param("Total Dependencies", 7, 7, "PASS", id="total-deps-unchanged-passes"),
    pytest.param("Total Jobs", 7.9, 7.1, "WARN", id="total-jobs-fractional-change-warns"),
])
def test_exact_metric_change_warns(metric, baseline_val, current_val, expected):
    baseline = _make_run(_make_entry("create", "create", "fc0_1_1", 0.7,
                                     **{**_baseline_entry(), metric: baseline_val}))
    current = _make_run(_make_entry("create", "create", "fc0_1_1", 0.7,
                                    **{**_baseline_entry(), metric: current_val}))

    report = compare.evaluate(
        compare.build_frame(current), compare.build_frame(baseline), _thresholds()
    )
    row = report[report["metric"] == metric].iloc[0]
    assert row["verdict"] == expected


@pytest.mark.parametrize("current_cpu, previous_cpu, expected", [
    pytest.param("Test CPU", "Other CPU", True, id="different-cpu-warns"),
    pytest.param("Test CPU", "Test CPU", False, id="same-cpu-no-warning"),
])
def test_environment_warning(current_cpu, previous_cpu, expected):
    current = [_make_run(_make_entry("run", "run", "fc0_1_1", 1.0, **_baseline_entry()))]
    previous = [_make_run(_make_entry("run", "run", "fc0_1_1", 1.0, **_baseline_entry()))]
    current[0]["machine_info"]["cpu"]["brand_raw"] = current_cpu
    previous[0]["machine_info"]["cpu"]["brand_raw"] = previous_cpu

    warning = compare.environment_warning(current, previous)
    assert (warning is not None) == expected


def test_current_directory_uses_newest_run_only(tmp_path: Path):
    older = tmp_path / "0001_old.json"
    newer = tmp_path / "0002_new.json"
    older.write_text("{}", encoding="UTF-8")
    newer.write_text("{}", encoding="UTF-8")
    now = time.time()
    os.utime(older, (now - 10, now - 10))
    os.utime(newer, (now, now))

    files = compare._iter_run_files(str(tmp_path))
    assert files == [newer]


_NAN = float("nan")


@pytest.mark.parametrize("metric, baseline, current, expected, expected_delta", [
    pytest.param("MEM GROWTH(MIB)", -15.0, 215.0, "WARN",
                 pytest.approx((215.0 - 15.0) / 15.0 * 100.0), id="flip-to-leak-warns"),
    pytest.param("MEM GROWTH(MIB)", 215.0, -15.0, "PASS", None, id="flip-to-release-passes"),
    pytest.param("MEM GROWTH(MIB)", -215.62, -256.48, "WARN",
                 pytest.approx((256.48 - 215.62) / 215.62 * 100.0), id="negative-more-negative-warns"),
    pytest.param("MEM GROWTH(MIB)", -215.0, -200.0, "PASS", None, id="negative-less-negative-passes"),
    pytest.param("MEM GROWTH(MIB)", 0.18, -0.8, "PASS", None, id="sub-floor-excluded"),
    pytest.param("FD GROWTH", 0, 1, "WARN", _NAN, id="zero-baseline-change-warns"),
    pytest.param("FD GROWTH", 1, 0, "PASS", None, id="zero-baseline-improvement-passes"),
    pytest.param("MEM GROWTH(MIB)", 0, 0.5, "PASS", _NAN, id="zero-baseline-sub-floor-passes"),
])
def test_growth_verdicts(metric, baseline, current, expected, expected_delta):
    baseline_run = _make_run(_make_entry("run", "run", "fc0_1_1", 10.0,
                                         **{**_baseline_entry(), metric: baseline}))
    current_run = _make_run(_make_entry("run", "run", "fc0_1_1", 10.0,
                                        **{**_baseline_entry(), metric: current}))

    report = compare.evaluate(
        compare.build_frame(current_run), compare.build_frame(baseline_run), _thresholds()
    )
    row = report[report["metric"] == metric].iloc[0]
    assert row["verdict"] == expected
    if expected_delta is _NAN:
        assert row["delta %"] != row["delta %"]  # NaN
    elif expected_delta is not None:
        assert row["delta %"] == expected_delta


@pytest.mark.parametrize("slug_dir, cpu, expected_file", [
    pytest.param("intel-xeon", "Intel Xeon", "4.2.0-aaaaaaa.json", id="slug-found"),
    pytest.param("amd-epyc", "Intel Xeon", None, id="slug-missing"),
])
def test_select_previous(slug_dir, cpu, expected_file, tmp_path):
    ref = tmp_path / "reference"
    (ref / slug_dir).mkdir(parents=True)
    if expected_file:
        (ref / slug_dir / expected_file).write_text("{}", encoding="UTF-8")

    files = compare._select_previous(str(ref), cpu)
    if expected_file:
        # the selected file lives under the current CPU's slug directory
        assert files[0].parent.name == "intel-xeon"
        assert files[0].name == expected_file
    else:
        assert files == []


def test_main_errors_on_unreadable_current_file(tmp_path: Path, monkeypatch):
    data = tmp_path / "data"
    data.mkdir()
    (data / "0001_corrupt.json").write_text("{not json", encoding="UTF-8")

    monkeypatch.setattr("sys.argv", ["compare_results", "--current", str(data), "--version", "9.9.9"])
    assert compare.main() == 1


def test_missing_baseline_scenario_renders_no_baseline():
    baseline = _make_run(_make_entry("run", "run", "fc0_1_1", 10.0, **_baseline_entry()))
    current = _make_run(
        _make_entry("run", "run", "fc0_1_1", 10.0, **_baseline_entry()),
        _make_entry("create", "create", "fc0_2_2", 1.0, **_baseline_entry()),
    )

    report = compare.evaluate(
        compare.build_frame(current), compare.build_frame(baseline), _thresholds()
    )
    rows = report[report["metric"] == "(no baseline)"]
    assert len(rows) == 1
    assert rows.iloc[0]["test type"] == "create"
    assert rows.iloc[0]["verdict"] == "N/A"


@pytest.mark.parametrize("has_baseline, out_name", [
    pytest.param(True, "test_run.png", id="with-baseline"),
    pytest.param(False, "test_abs.png", id="absolute-mode-no-baseline"),
])
def test_render_heatmap_produces_png(tmp_path: Path, has_baseline, out_name):
    current = _make_run(_make_entry("run", "run", "fc0_1_1", 11.0, **_baseline_entry()))
    baseline = _make_run(_make_entry("run", "run", "fc0_1_1", 10.0, **_baseline_entry()))
    report = compare.evaluate(
        compare.build_frame(current), compare.build_frame(baseline), _thresholds()
    )
    previous = compare.build_frame(baseline) if has_baseline else None

    out = compare.render_heatmap(
        compare.build_frame(current), previous, report,
        "4.2.0", tmp_path, thresholds=_thresholds(),
        test_types={"run"}, metrics=["Time Taken(Seconds)"], out_name=out_name,
    )
    assert out is not None and out.exists() and out.stat().st_size > 0


def _make_cross_run(run_mem: float, recovery_mem: float | None = None,
                    setstatus_mems: dict | None = None) -> dict:
    """Build a run file with run/recovery/setstatus scenarios of the same experiment."""
    setstatus_mems = setstatus_mems or {}
    entries = [_make_entry("run", "run", "4m/2c/2s", 10.0, **{**_baseline_entry(), "Memory consumption(MiB)": run_mem})]
    if recovery_mem is not None:
        entries.append(_make_entry("recovery", "recovery", "4m/2c/2s", 10.0,
                                   **{**_baseline_entry(), "Memory consumption(MiB)": recovery_mem}))
    for flt, mem in setstatus_mems.items():
        entries.append(_make_entry(f"setstatus_{flt}", "setstatus", f"4m/2c/2s·{flt}", 10.0,
                                   **{**_baseline_entry(), "Memory consumption(MiB)": mem}))
    return _make_run(*entries)


@pytest.mark.parametrize("run_mem, recovery_mem, setstatus_mems, expected", [
    pytest.param(100.0, 120.0, {"ftcs": 110.0, "ft": 130.0, "fs": 105.0, "fl": 115.0},
                 {"recovery": "PASS", "4m/2c/2s·ftcs": "PASS", "4m/2c/2s·ft": "PASS",
                  "4m/2c/2s·fs": "PASS", "4m/2c/2s·fl": "PASS"}, id="all-pass"),
    pytest.param(130.0, 120.0, {"ftcs": 110.0, "ft": 130.0, "fs": 105.0, "fl": 115.0},
                 {"recovery": "WARN"}, id="recovery-warns"),
    pytest.param(100.0, 120.0, {"ftcs": 110.0, "ft": 90.0, "fs": 105.0, "fl": 115.0},
                 {"4m/2c/2s·ft": "WARN"}, id="setstatus-variant-warns"),
    pytest.param(100.0, 120.0, {}, {"setstatus": "N/A"}, id="missing-setstatus"),
])
def test_cross_scenario_verdicts(run_mem, recovery_mem, setstatus_mems, expected):
    run = _make_cross_run(run_mem=run_mem, recovery_mem=recovery_mem, setstatus_mems=setstatus_mems)
    cross = compare.evaluate_cross_scenarios(compare.build_frame(run))

    for counterpart, verdict in expected.items():
        row = cross[cross["counterpart"] == counterpart].iloc[0]
        assert row["verdict"] == verdict


def test_cross_scenarios_no_run_type_returns_empty():
    run = _make_run(_make_entry("create", "create", "4m/2c/2s", 10.0, **_baseline_entry()))
    cross = compare.evaluate_cross_scenarios(compare.build_frame(run))

    assert cross.empty


def test_render_markdown_reports_cross_checks(tmp_path: Path):
    run = _make_cross_run(run_mem=130.0, recovery_mem=120.0)
    current = compare.build_frame(run)
    report = compare.evaluate(current, None, _thresholds())
    cross = compare.evaluate_cross_scenarios(current)

    markdown = compare.render_markdown(report, "4.2.0", "Current", previous_label=None,
                                       env_warning=None, cross_report=cross)
    assert "Cross-scenario checks" in markdown
    assert "Memory consumption(MiB)" in markdown
    assert "No regressions detected" in markdown


def test_render_heatmap_scenario_filter(tmp_path: Path):
    entries = []
    for tt, rid, mem in [("run", "4m/2c/2s", 100.0), ("run", "4m/2c/6s", 200.0),
                         ("setstatus", "4m/2c/2s·ftcs", 110.0)]:
        entries.append(_make_entry(tt, tt, rid, 10.0, **{**_baseline_entry(), "Memory consumption(MiB)": mem}))
    current = compare.build_frame(_make_run(*entries))
    report = compare.evaluate(current, None, _thresholds())

    out = compare.render_heatmap(
        current, None, report, "4.2.0", tmp_path, thresholds=_thresholds(),
        test_types={"run", "setstatus"}, metrics=["Memory consumption(MiB)"],
        scenario_ids={"4m/2c/2s"}, out_name="test_filtered.png",
    )
    assert out is not None and out.exists() and out.stat().st_size > 0


def test_heaviest_scenario_picks_max_total_jobs():
    entries = [
        _make_entry("run", "run", "4m/2c/2s", 10.0, **{**_baseline_entry(), "Total Jobs": 7}),
        _make_entry("run", "run", "10m/2c/150s", 10.0, **{**_baseline_entry(), "Total Jobs": 3000}),
        _make_entry("setstatus", "setstatus", "10m/2c/150s·ftcs", 10.0,
                    **{**_baseline_entry(), "Total Jobs": 3000}),
    ]
    current = compare.build_frame(_make_run(*entries))
    assert compare._heaviest_scenario(current) == "10m/2c/150s"


def test_render_heatmaps_produces_memory_plot(tmp_path: Path):
    entries = []
    for tt, rid, mem in [("run", "10m/2c/150s", 100.0), ("recovery", "10m/2c/150s", 120.0),
                         ("create", "10m/2c/150s", 90.0),
                         ("setstatus", "10m/2c/150s·ftcs", 110.0),
                         ("setstatus", "10m/2c/150s·ft", 115.0)]:
        entries.append(_make_entry(tt, tt, rid, 10.0,
                                   **{**_baseline_entry(), "Memory consumption(MiB)": mem, "Total Jobs": 3000}))
    current = compare.build_frame(_make_run(*entries))
    report = compare.evaluate(current, None, _thresholds())

    paths = compare.render_heatmaps(current, None, report, "4.2.0", tmp_path, thresholds=_thresholds())
    names = {p.name for p in paths}
    assert "summary_4.2.0_run.png" in names
    assert "summary_4.2.0_create_recovery_setstatus.png" in names
    assert "summary_4.2.0_memory.png" in names


def test_heavy_scenario_in_report_but_excluded_from_heatmaps():
    entries = [
        _make_entry("run", "run", "4m/2c/2s", 10.0, **{**_baseline_entry(), "Total Jobs": 7}),
        _make_entry("run", "run", "10m/2c/75s", 10.0, **{**_baseline_entry(), "Total Jobs": 3000}),
        _make_entry("setstatus", "setstatus", "10m/2c/75s·ftcs", 10.0,
                    **{**_baseline_entry(), "Total Jobs": 3000}),
    ]
    current = compare.build_frame(_make_run(*entries))

    report = compare.evaluate(current, None, _thresholds())
    assert any("10m/2c/75s" in rid for rid in report["ID"])
    assert not report[report["ID"] == "10m/2c/75s"].empty
    assert not report[report["ID"] == "10m/2c/75s·ftcs"].empty

    cross = compare.evaluate_cross_scenarios(current)
    assert any("10m/2c/75s" in row["ID"] or row["ID"] == "10m/2c/75s" for row in cross.to_dict("records"))
    assert compare._heaviest_scenario(current) == "10m/2c/75s"

    run_order = compare._plot_order(current, compare._RUN_TEST_TYPES,
                                    excluded_scenarios=compare._EXCLUDED_SCENARIOS)
    assert [rid for _, rid in run_order] == ["4m/2c/2s"]
    memory_order = compare._plot_order(current, compare._MEMORY_TEST_TYPES,
                                       scenario_ids={"10m/2c/75s"})
    assert any(rid == "10m/2c/75s" for _, rid in memory_order)


@pytest.mark.parametrize("test_types,excluded,scenario_ids,expected_bases", [
    (compare._RUN_TEST_TYPES, compare._EXCLUDED_SCENARIOS, None, {"4m/2c/2s", "4m/2c/6s"}),
    (compare._OTHER_TEST_TYPES, compare._EXCLUDED_SCENARIOS, None, {"4m/2c/2s"}),
    (compare._MEMORY_TEST_TYPES, None, {"10m/2c/75s"}, {"10m/2c/75s"}),
    (compare._RUN_TEST_TYPES, None, None, {"4m/2c/2s", "4m/2c/6s", "10m/2c/75s"}),
])
def test_plot_order_includes_or_excludes_heavy_scenario(test_types, excluded, scenario_ids, expected_bases):
    entries = [
        _make_entry("run", "run", "4m/2c/2s", 2.0, **_baseline_entry()),
        _make_entry("run", "run", "4m/2c/6s", 6.0, **_baseline_entry()),
        _make_entry("run", "run", "10m/2c/75s", 75.0,
                    **{**_baseline_entry(), "Total Jobs": 3000}),
        _make_entry("setstatus", "setstatus", "4m/2c/2s·ftcs", 1.0, **_baseline_entry()),
        _make_entry("setstatus", "setstatus", "10m/2c/75s·ftcs", 1.0,
                    **{**_baseline_entry(), "Total Jobs": 3000}),
        _make_entry("create", "create", "4m/2c/2s", 1.0, **_baseline_entry()),
    ]
    current = compare.build_frame(_make_run(*entries))

    order = compare._plot_order(current, test_types, scenario_ids=scenario_ids,
                                excluded_scenarios=excluded)
    assert {current.loc[(tt, rid), "base"] for tt, rid in order} == expected_bases
