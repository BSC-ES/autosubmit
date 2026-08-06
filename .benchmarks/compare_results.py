#!/usr/bin/env python3
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

"""Compare two pytest-benchmark runs and produce a performance report.

Loads pytest-benchmark JSON runs (as saved by ``--benchmark-save``) and
produces:

- ``summary_{version}.md``: a comparison table between the current run and a
  baseline, flagging regressions that exceed the configured thresholds.
- ``summary_{version}_run.png`` and ``summary_{version}_create_recovery_setstatus.png``:
  heatmaps of the run and the create/recovery/setstatus scenarios, current vs
  baseline.

Each side of the comparison is a single pytest-benchmark file: the workflow
merges the BENCHMARK_RUNS sessions of a run into one file with per-scenario
medians (see merge_runs.py), and the baseline reference keeps one file per
CPU: ``.benchmarks/reference/<cpu-slug>/``, in which case the baseline
matching the current run's CPU is selected automatically (a run on a CPU
without a baseline yet is shown without comparison and seeds it).

Usage::

    python .benchmarks/compare_results.py \\
        --current .benchmarks/data \\
        --previous .benchmarks/reference \\
        --thresholds .benchmarks/thresholds.yml \\
        --version 4.2.0 \\
        --output-dir .benchmarks/artifacts

When no ``--previous`` is provided the report is rendered current-only (used
for the first run after the baseline branch is created).
"""


import argparse
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Deterministic metrics that must not change between runs.
EXACT_METRICS = ["Total Jobs", "Total Dependencies"]

# Metric names as stored in ``benchmark.extra_info``, plus the wall-clock time
# that pytest-benchmark records in ``stats.median``.
METRIC_COLUMNS = [
    "Time Taken(Seconds)",
    "Memory consumption(MiB)",
    "Historical DB Disk Usage(MiB)",
    "Job list DB Usage",
    "FD GROWTH",
    "MEM GROWTH(MIB)",
    "OBJ GROWTH",
] + EXACT_METRICS

# Profiler growth metrics are only meaningful for the `run` scenarios;
# elsewhere they are absent or dominated by noise, so they are excluded.
_GROWTH_METRICS = {"FD GROWTH", "MEM GROWTH(MIB)", "OBJ GROWTH"}
_NO_GROWTH_TEST_TYPES = {"create", "recovery", "setstatus"}

# The performance plots are split in two: the `run` scenarios carry the
# profiler growth metrics, the create/recovery/setstatus scenarios only carry
# time/memory/db.
_RUN_TEST_TYPES = {"run"}
_OTHER_TEST_TYPES = {"create", "recovery", "setstatus"}
_RUN_PLOT_METRICS = ["Time Taken(Seconds)", "Memory consumption(MiB)", "MEM GROWTH(MIB)", "FD GROWTH", "OBJ GROWTH"]
_OTHER_PLOT_METRICS = ["Time Taken(Seconds)", "Memory consumption(MiB)",
                       "Historical DB Disk Usage(MiB)", "Job list DB Usage"]
_SHORT_METRICS = {
    "Time Taken(Seconds)": "Time (s)",
    "Memory consumption(MiB)": "Memory (MiB)",
    "MEM GROWTH(MIB)": "MEM growth (MiB)",
    "FD GROWTH": "FD growth",
    "OBJ GROWTH": "Obj growth",
    "Historical DB Disk Usage(MiB)": "Hist DB (KiB)",
    "Job list DB Usage": "Job DB (KiB)",
}


def _allowed_metrics(test_type: str) -> set[str]:
    """Return the metric names to report for the given test type."""
    if test_type in _NO_GROWTH_TEST_TYPES:
        return set(METRIC_COLUMNS) - _GROWTH_METRICS
    return set(METRIC_COLUMNS)


_TABLE_COLUMNS = ["test type", "ID", "metric", "baseline", "current", "delta %", "verdict"]


def _load_thresholds(path: Path) -> dict:
    """Load the thresholds YAML file using the project's YAML parser."""
    from ruamel.yaml import YAML

    yaml = YAML(typ="safe")
    with open(path, encoding="UTF-8") as file:
        data = yaml.load(file) or {}
    metrics = data.get("metrics", {})
    exact = data.get("exact_metrics", [])
    plot_cfg = data.get("plot", {})
    return {"metrics": metrics, "exact_metrics": exact, "plot": plot_cfg}


def _iter_run_files(path: str | None, latest_only: bool = False) -> list[Path]:
    """Return the pytest-benchmark JSON files under ``path`` (file or dir).

    When ``latest_only`` is set and ``path`` is a directory, only the most
    recently modified run file is returned. This is used for the ``--current``
    argument so the merged file of the latest run is the one compared.
    """
    if not path:
        return []
    p = Path(path)
    if p.is_dir():
        files = sorted(p.rglob("*.json"), key=lambda f: f.stat().st_mtime)
        if latest_only and files:
            return [files[-1]]
        return files
    if p.is_file():
        return [p]
    return []


def _load_runs(files: list[Path]) -> list[dict]:
    """Load the raw JSON content of the given pytest-benchmark run files."""
    runs = []
    for file in files:
        try:
            with open(file, encoding="UTF-8") as fh:
                runs.append(json.load(fh))
        except (OSError, json.JSONDecodeError) as exc:
            print(f"[WARNING] Skipping unreadable benchmark file {file}: {exc}")
    return runs


def _current_cpu(runs: list[dict]) -> str:
    """Return the CPU ``brand_raw`` of the first run, or ``''`` when unknown."""
    if not runs:
        return ""
    return (runs[0].get("machine_info", {}).get("cpu") or {}).get("brand_raw") or ""


def _cpu_slug(brand_raw: str) -> str:
    """Turn a CPU brand string into a directory-safe slug."""
    slug = re.sub(r"[^a-z0-9]+", "-", brand_raw.lower()).strip("-")
    return slug or "unknown-cpu"


def _select_previous(previous: str | None, current_cpu: str) -> list[Path]:
    """Return the baseline run file for the current CPU.

    ``--previous`` may be a single file or the per-CPU reference directory
    (``.benchmarks/reference/<slug>/``). The subdirectory matching
    ``current_cpu`` is used; if none matches, no baseline is available for
    this CPU.
    """
    if not previous:
        return []
    p = Path(previous)
    if p.is_file():
        return [p]
    if p.is_dir():
        slug = _cpu_slug(current_cpu) if current_cpu else ""
        target = p / slug if slug else None
        if target is not None and target.is_dir():
            return sorted(target.rglob("*.json"))
    return []


def build_frame(run: dict) -> pd.DataFrame:
    """Build a DataFrame indexed by (test type, ID) from a single run file.

    Each side of the comparison is one pytest-benchmark file: the workflow
    merges the BENCHMARK_RUNS sessions with merge_runs.py (per-scenario
    medians) and the baseline reference keeps one file per CPU.
    """
    records = []
    for entry in run.get("benchmarks", []):
        extra = entry.get("extra_info", {})
        test_type = extra.get("test type")
        run_id = extra.get("ID")
        if not test_type or not run_id:
            continue
        row = {
            "test type": test_type,
            "ID": run_id,
            "Time Taken(Seconds)": entry.get("stats", {}).get("median"),
        }
        for metric in METRIC_COLUMNS[1:]:
            row[metric] = extra.get(metric)
        records.append(row)

    if not records:
        return pd.DataFrame(columns=_TABLE_COLUMNS)

    frame = pd.DataFrame(records)
    numeric = [c for c in METRIC_COLUMNS if c not in EXACT_METRICS]
    for col in numeric:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    for col in EXACT_METRICS:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")

    return frame.set_index(["test type", "ID"])


def _safe_pct(current: float | None, previous: float | None) -> float | None:
    """Return the percentage change current vs previous, or None when unknown."""
    if previous is None or current is None or pd.isna(previous) or previous == 0 or pd.isna(current):
        return None
    return (float(current) - float(previous)) / float(previous) * 100.0


def evaluate(current: pd.DataFrame, previous: pd.DataFrame | None, thresholds: dict) -> pd.DataFrame:
    """Compare current vs previous applying the configured thresholds.

    Deltas are always computed on the absolute value of the measurement
    (``|current|`` vs ``|previous|``): a regression means the magnitude grew,
    which is well-defined even when values can be negative or cross zero (a
    growth metric going from -15 to +215 MiB is a regression even though the
    signed delta would read as an "improvement").

    Returns a DataFrame with one row per (scenario, metric) pair.
    """
    metrics_cfg = thresholds.get("metrics", {})
    exact = set(thresholds.get("exact_metrics", [])) | set(EXACT_METRICS)

    rows = []
    for (test_type, run_id) in current.index:
        cur = current.loc[(test_type, run_id)]
        if previous is None:
            prev = pd.Series(dtype=float)
            baseline_ok = False
        elif (test_type, run_id) in previous.index:
            prev = previous.loc[(test_type, run_id)]
            baseline_ok = True
        else:
            rows.append({"test type": test_type, "ID": run_id, "metric": "(no baseline)",
                         "baseline": None, "current": None, "delta %": None, "verdict": "N/A"})
            continue

        for metric in METRIC_COLUMNS:
            if metric not in _allowed_metrics(test_type):
                continue
            cur_val = cur.get(metric)
            prev_val = prev.get(metric) if baseline_ok else None
            if cur_val is None or pd.isna(cur_val):
                continue

            if metric in exact:
                if baseline_ok and prev_val is not None and not pd.isna(prev_val):
                    cur_int = int(float(cur_val))
                    prev_int = int(float(prev_val))
                    verdict = "WARN" if cur_int != prev_int else "PASS"
                    rows.append({"test type": test_type, "ID": run_id, "metric": metric,
                                 "baseline": prev_val, "current": cur_val, "delta %": None, "verdict": verdict})
                elif not baseline_ok:
                    rows.append({"test type": test_type, "ID": run_id, "metric": metric,
                                 "baseline": None, "current": cur_val, "delta %": None, "verdict": "N/A"})
                continue

            cfg = metrics_cfg.get(metric, {})
            threshold = float(cfg.get("threshold", 15.0))
            floor = float(cfg.get("floor", 0.0))
            abs_prev = abs(prev_val) if prev_val is not None else None
            pct = _safe_pct(abs(cur_val), abs_prev)

            if not baseline_ok or prev_val is None or pd.isna(prev_val):
                rows.append({"test type": test_type, "ID": run_id, "metric": metric,
                             "baseline": prev_val, "current": cur_val, "delta %": pct, "verdict": "N/A"})
                continue

            verdict = "PASS"
            if pct is not None and pct > threshold and abs(float(cur_val)) >= floor:
                verdict = "WARN"
            elif prev_val == 0 and cur_val != 0 and abs(float(cur_val)) >= floor:
                # A measurable change from an exact zero baseline (e.g. FD
                # GROWTH going 0 -> 1) has no finite delta, but it is a change:
                # any magnitude above the floor warns.
                verdict = "WARN"
            rows.append({"test type": test_type, "ID": run_id, "metric": metric,
                         "baseline": prev_val, "current": cur_val, "delta %": pct, "verdict": verdict})

    return pd.DataFrame(rows, columns=_TABLE_COLUMNS)


def environment_warning(current_runs: list[dict], previous_runs: list[dict]) -> str | None:
    """Compare machine/environment info between current and baseline runs."""
    if not current_runs or not previous_runs:
        return None
    cur = current_runs[0].get("machine_info", {})
    prev = previous_runs[0].get("machine_info", {})
    cur_cpu = (cur.get("cpu") or {}).get("brand_raw")
    prev_cpu = (prev.get("cpu") or {}).get("brand_raw")
    if cur_cpu and prev_cpu and cur_cpu != prev_cpu:
        return (f"Environment differs from baseline: current ran on `{cur_cpu}` "
                f"while the baseline ran on `{prev_cpu}`. Results may not be comparable.")
    return None


def render_markdown(report: pd.DataFrame, version: str, current_label: str,
                    previous_label: str | None, env_warning: str | None) -> str:
    """Render the report as a GitHub-flavored markdown summary."""
    lines = [f"# Autosubmit Performance Metrics - Version {version}",
             "", f"- **Current:** {current_label}", f"- **Baseline:** {previous_label or 'None'}"]
    if env_warning:
        lines += ["", f"> ⚠️ {env_warning}"]
    lines.append("")

    warnings = report[report["verdict"] == "WARN"]
    if not warnings.empty:
        lines.append("## ⚠️ Regressions detected")
        lines.append("")
        for _, row in warnings.iterrows():
            delta = f"{row['delta %']:+.1f}%" if pd.notna(row["delta %"]) else "changed"
            lines.append(f"- `{row['test type']}/{row['ID']}` **{row['metric']}**: "
                         f"{row['baseline']:.2f} -> {row['current']:.2f} ({delta})")
        lines.append("")
    else:
        lines.append("## ✔️ No regressions detected")
        lines.append("")

    for test_type, group in report.groupby("test type", sort=False):
        table = group.drop(columns=["test type"]).copy()
        for col in ["baseline", "current"]:
            table[col] = table[col].map(lambda v: f"{v:.2f}" if pd.notna(v) else "-")
        table["delta %"] = table["delta %"].map(lambda v: f"{v:+.1f}%" if pd.notna(v) else "-")
        lines.append(f"### {test_type}")
        lines.append("")
        lines.append(table.to_markdown(index=False))
        lines.append("")
    return "\n".join(lines)



def _metric_threshold(thresholds: dict, metric: str) -> float:
    """Return the regression threshold (%) for a metric, with a sane fallback."""
    cfg = thresholds.get("metrics", {}).get(metric, {})
    thr = float(cfg.get("threshold", 15.0))
    return thr if thr > 0 else 15.0


def _text_color(rgba: np.ndarray, r: int, c: int) -> str:
    """Return 'white' or 'black' for text on a cell, based on its rendered luminance.

    The cell color is composited over white at its alpha, then the luminance
    decides the text color so dark cells get white text and light cells black.
    """
    rgb = rgba[r, c, 0:3]
    alpha = float(rgba[r, c, 3])
    eff = alpha * rgb + (1.0 - alpha)
    lum = 0.299 * float(eff[0]) + 0.587 * float(eff[1]) + 0.114 * float(eff[2])
    return "white" if lum < 0.55 else "black"


def _format_abs(metric: str, value: float) -> str:
    """Format an absolute metric value for a heatmap cell (numbers only)."""
    if metric == "Time Taken(Seconds)":
        return f"{value:.1f}"
    if metric in ("Historical DB Disk Usage(MiB)", "Job list DB Usage"):
        return f"{value * 1024:.0f}"
    return f"{value:.0f}"


def render_heatmap(current: pd.DataFrame, previous: pd.DataFrame | None, report: pd.DataFrame,
                   version: str, output_dir: Path, cpu_label: str | None = None,
                   thresholds: dict | None = None, test_types: set[str] | None = None,
                   metrics: list[str] | None = None,
                   out_name: str | None = None) -> Path | None:
    """Render a filled-cell grid for a subset of scenarios x metrics.

    Each scenario x metric intersection is a cell: its color encodes the
    direction (``coolwarm`` diverging, red = regression, blue = improvement)
    with a central dead zone (|delta| below ``plot.delta_tolerance`` renders
    neutral) and opacity by threshold-relative severity. Cells are annotated
    with the current absolute value (numbers only, units in the column headers);
    rows are grouped by test type via a left gutter. When there is no baseline,
    cells are uniformly neutral. Metrics excluded from a test type are left
    blank. ``test_types`` and ``metrics`` restrict which scenarios and metrics
    are drawn, so the run-only profiler metrics can live in their own plot.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize, TwoSlopeNorm
    from matplotlib.patches import Rectangle

    class _DeadZoneNorm(Normalize):
        """Diverging norm with a central neutral 'dead zone'."""

        def __init__(self, vmin: float, vmax: float, vcenter: float = 0.0, tolerance: float = 3.0):
            super().__init__(vmin=vmin, vmax=vmax)
            self.vcenter = float(vcenter)
            self.tolerance = float(tolerance)

        def _span(self) -> float:
            return max(self.vmax - self.vcenter, self.vcenter - self.vmin)

        def __call__(self, value, clip=None):
            v = np.clip(np.asarray(value, dtype=float), self.vmin, self.vmax)
            half = max((self._span() - self.tolerance) / 2.0, 1e-9)
            x = np.zeros_like(v)
            above = v >= self.vcenter + self.tolerance
            below = v <= self.vcenter - self.tolerance
            x[above] = (v[above] - (self.vcenter + self.tolerance)) / (2.0 * half)
            x[below] = (v[below] - (self.vcenter - self.tolerance)) / (2.0 * half)
            return np.clip((x + 1.0) / 2.0, 0.0, 1.0)

        def inverse(self, value):
            x = np.asarray(value, dtype=float) * 2.0 - 1.0
            half = max((self._span() - self.tolerance) / 2.0, 1e-9)
            return np.where(x >= 0,
                            self.vcenter + self.tolerance + x * 2.0 * half,
                            self.vcenter - self.tolerance + x * 2.0 * half)

    thresholds = thresholds or {}
    test_types = test_types or set(current.index.get_level_values("test type").unique())
    metrics = [c for c in (metrics or []) if current[c].notna().any()]
    order = []
    for test_type in current.index.get_level_values("test type").unique():
        if test_type not in test_types:
            continue
        mask = current.index.get_level_values("test type") == test_type
        rows = current.loc[mask].sort_values("Time Taken(Seconds)", ascending=True)
        order.extend((test_type, run_id) for run_id in rows.index.get_level_values("ID"))

    if not metrics or not order:
        return None

    pivot = report.pivot_table(index=["test type", "ID"], columns="metric",
                               values="delta %", aggfunc="first")
    delta = pivot.reindex(index=order, columns=metrics).to_numpy(dtype=float)

    abs_pivot = current[metrics].copy()
    abs_pivot["test type"] = current.index.get_level_values("test type")
    abs_pivot["ID"] = current.index.get_level_values("ID")
    abs_pivot = abs_pivot.set_index(["test type", "ID"])
    absval = abs_pivot.reindex(index=order, columns=metrics).to_numpy(dtype=float)

    excluded = np.zeros((len(order), len(metrics)), dtype=bool)
    for r, (test_type, _) in enumerate(order):
        for c, metric in enumerate(metrics):
            excluded[r, c] = metric not in _allowed_metrics(test_type)

    absolute_mode = previous is None or previous.empty
    plot_cfg = thresholds.get("plot", {})
    clip = float(plot_cfg.get("delta_clip", 15.0))
    tolerance = float(plot_cfg.get("delta_tolerance", 3.0))
    cmap = plt.get_cmap("coolwarm")
    if absolute_mode:
        # No baseline: uniform neutral cells (the "0%" color), values only.
        color_values = np.zeros_like(absval)
        norm = TwoSlopeNorm(vmin=-1.0, vcenter=0.0, vmax=1.0)
    else:
        norm = _DeadZoneNorm(vmin=-clip, vmax=clip, vcenter=0.0, tolerance=tolerance)
        color_values = np.where(np.isnan(delta), 0.0, delta)

    rgba = cmap(norm(color_values))
    rgba[excluded, 3] = 0.0
    if not absolute_mode:
        no_baseline = np.isnan(delta) & ~excluded
        dead_zone = (np.abs(delta) <= tolerance) & ~excluded
        neutral = (no_baseline | dead_zone) & ~excluded
        rgba[neutral, 0:3] = [0.88, 0.88, 0.88]
        rgba[neutral, 3] = 1.0
        significant = ~excluded & ~np.isnan(delta) & ~dead_zone
        if significant.any():
            sig_rows, sig_cols = np.nonzero(significant)
            thresholds_by_col = np.array([_metric_threshold(thresholds, m) for m in metrics], dtype=float)
            severity = np.abs(delta[sig_rows, sig_cols]) / thresholds_by_col[sig_cols]
            rgba[sig_rows, sig_cols, 3] = 0.25 + 0.75 * np.minimum(1.0, severity)
    else:
        rgba[np.isnan(absval) & ~excluded, 3] = 0.0

    fig, ax = plt.subplots(figsize=(1.7 * len(metrics) + 2.4, max(3.5, 0.5 * len(order) + 1.5)))
    ax.set_frame_on(False)
    ax.imshow(rgba, aspect="auto", interpolation="nearest")

    ax.set_xticks([c + 0.5 for c in range(len(metrics))], minor=True)
    ax.set_yticks([r + 0.5 for r in range(len(order))], minor=True)
    ax.grid(which="minor", color="#eeeeee", lw=0.8)
    ax.set_xlim(-1.9, len(metrics) - 0.5)
    ax.set_ylim(-0.5, len(order) - 0.5)

    for r in range(len(order)):
        for c, metric in enumerate(metrics):
            if excluded[r, c]:
                continue
            abs_value = absval[r, c]
            if np.isnan(abs_value):
                continue
            ax.text(c, r, _format_abs(metric, abs_value), ha="center", va="center",
                    fontsize=8, color=_text_color(rgba, r, c))

    ax.set_yticks(range(len(order)))
    ax.set_yticklabels([run_id for (_, run_id) in order], fontsize=7)
    ax.set_xticks(range(len(metrics)))
    ax.set_xticklabels([_SHORT_METRICS.get(m, m) for m in metrics], fontsize=9)

    groups: dict[str, list[int]] = {}
    prev_type = None
    for r, (test_type, _) in enumerate(order):
        if prev_type is not None and test_type != prev_type:
            ax.axhline(r - 0.5, color="gray", lw=0.9, zorder=1)
        groups.setdefault(test_type, []).append(r)
        prev_type = test_type

    for test_type, rows in groups.items():
        ymid = (rows[0] + rows[-1]) / 2.0
        group_rows = rows[-1] - rows[0] + 1
        fontsize = min(12, max(7, 8 * group_rows))
        ax.text(-1.15, ymid, test_type, rotation=90, ha="center", va="center",
                fontsize=fontsize, fontweight="bold", color="#1f1f1f")
        ax.add_patch(Rectangle((-0.5, rows[0] - 0.5), len(metrics), group_rows,
                               fill=False, edgecolor="black", linewidth=1.5, zorder=2))

    title = f"Autosubmit Performance Metrics - Version {version}"
    if cpu_label:
        title += f" · {cpu_label}"
    if absolute_mode:
        title += " · no baseline - absolute values"
    ax.set_title(title, fontsize=13)

    sm = plt.cm.ScalarMappable(norm=norm, cmap=cmap)
    sm.set_array([])
    if not absolute_mode:
        cb = fig.colorbar(sm, ax=ax, fraction=0.03, pad=0.02)
        cb.set_label("Delta % vs Baseline")
        ticks = [-clip, -clip / 2, 0, clip / 2, clip]
        cb.set_ticks(ticks)
        cb.set_ticklabels([f"{v:g}" for v in ticks])

    fig.tight_layout()
    out = output_dir / (out_name or f"summary_{version}.png")
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=110, bbox_inches="tight")
    plt.close(fig)
    return out


def render_heatmaps(current: pd.DataFrame, previous: pd.DataFrame | None, report: pd.DataFrame,
                    version: str, output_dir: Path, cpu_label: str | None = None,
                    thresholds: dict | None = None) -> list[Path]:
    """Render the two performance plots: `run` and the create/recovery/setstatus ones."""
    paths = []
    for name, test_types, metrics in (
        (f"summary_{version}_run.png", _RUN_TEST_TYPES, _RUN_PLOT_METRICS),
        (f"summary_{version}_create_recovery_setstatus.png", _OTHER_TEST_TYPES, _OTHER_PLOT_METRICS),
    ):
        out = render_heatmap(current, previous, report, version, output_dir,
                             cpu_label=cpu_label, thresholds=thresholds,
                             test_types=test_types, metrics=metrics, out_name=name)
        if out is not None:
            paths.append(out)
    return paths


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--current", required=True, help="Current run file or directory of JSON runs.")
    parser.add_argument("--previous", default=None, help="Baseline run file or directory of JSON runs.")
    parser.add_argument("--thresholds", default=str(Path(__file__).parent / "thresholds.yml"),
                        help="Path to the thresholds YAML file.")
    parser.add_argument("--version", default=None, help="Autosubmit version, used to name the output files.")
    parser.add_argument("--output-dir", default=str(Path(__file__).parent / "artifacts"),
                        help="Directory where the report files are written.")
    parser.add_argument("--current-label", default="Current run", help="Label for the current run.")
    parser.add_argument("--previous-label", default="Baseline", help="Label for the baseline.")
    args = parser.parse_args()

    if not args.version:
        from importlib.metadata import PackageNotFoundError
        from importlib.metadata import version as pkg_version

        try:
            args.version = pkg_version("autosubmit")
        except PackageNotFoundError:
            args.version = (Path(__file__).parent.parent / "VERSION").read_text().strip()

    current_files = _iter_run_files(args.current, latest_only=True)
    if not current_files:
        print(f"[ERROR] No benchmark runs found under --current {args.current}", file=sys.stderr)
        return 1

    current_runs = _load_runs(current_files)
    if not current_runs:
        print(f"[ERROR] No readable benchmark runs found under --current {args.current}", file=sys.stderr)
        return 1
    current_cpu = _current_cpu(current_runs)

    previous_files = _select_previous(args.previous, current_cpu)
    previous_runs = _load_runs(previous_files) if previous_files else []
    if previous_runs and current_cpu:
        baseline_cpu = _current_cpu(previous_runs)
        if baseline_cpu and baseline_cpu != current_cpu:
            print(f"[WARNING] Baseline CPU `{baseline_cpu}` differs from current `{current_cpu}`; "
                  f"ignoring baseline.")
            previous_runs = []

    current_frame = build_frame(current_runs[0])
    previous_frame = build_frame(previous_runs[0]) if previous_runs else None

    thresholds = _load_thresholds(Path(args.thresholds))
    report = evaluate(current_frame, previous_frame, thresholds)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    prev_label = args.previous_label if previous_runs else None
    baseline_cpu = _current_cpu(previous_runs) if previous_runs else ""
    if prev_label and baseline_cpu:
        prev_label = f"{prev_label} · {baseline_cpu}"

    env_warning = environment_warning(current_runs, previous_runs)
    if args.previous and not previous_runs and current_cpu:
        note = f"No baseline yet for CPU `{current_cpu}` - this run establishes it (shown without comparison)."
        env_warning = f"{env_warning}\n\n{note}" if env_warning else note

    markdown = render_markdown(report, args.version, args.current_label, prev_label, env_warning)
    markdown_path = output_dir / f"summary_{args.version}.md"
    markdown_path.write_text(markdown, encoding="UTF-8")
    print(f"Saved performance comparison markdown to {markdown_path}")

    plot_paths = render_heatmaps(current_frame, previous_frame, report, args.version, output_dir,
                                 cpu_label=current_cpu or None, thresholds=thresholds)
    for plot_path in plot_paths:
        print(f"Saved performance comparison plot to {plot_path}")

    n_warnings = int((report["verdict"] == "WARN").sum())
    verdict = {
        "version": args.version,
        "regressions_detected": n_warnings > 0,
        "n_regressions": n_warnings,
        "n_scenarios": len(current_frame),
        "cpu": current_cpu,
        "cpu_slug": _cpu_slug(current_cpu) if current_cpu else "",
    }
    verdict_path = output_dir / f"report_{args.version}.json"
    verdict_path.write_text(json.dumps(verdict), encoding="UTF-8")
    print(f"Saved comparison verdict to {verdict_path}")
    print(f"Detected {n_warnings} regression warning(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
