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

r"""Merge the pytest-benchmark session files of one run into a single JSON.

The metrics workflow runs the suite BENCHMARK_RUNS times and each session
writes its own pytest-benchmark JSON. This script merges them into ONE
pytest-benchmark-shaped JSON whose per-scenario values are the medians across
sessions (both ``stats.median`` and the ``extra_info`` metrics).

Usage::

    python .benchmarks/merge_runs.py \\
        --input .benchmarks/data \\
        --output .benchmarks/reference/<cpu-slug>/4.2.0-abc1234.json
"""


import argparse
import json
import statistics
import sys
from pathlib import Path

# extra_info keys that identify the scenario and are never averaged.
_STRING_KEYS = {"test type", "ID"}


def load_runs(input_dir: Path) -> list[dict]:
    """Load all pytest-benchmark JSON files under ``input_dir``.

    :param input_dir: Directory holding the pytest-benchmark JSON files.
    :type input_dir: Path
    :return: List of parsed benchmark run dictionaries.
    :rtype: list
    """
    files = sorted(input_dir.rglob("*.json"))
    if not files:
        print(f"[ERROR] No benchmark run files found under {input_dir}", file=sys.stderr)
        sys.exit(1)
    runs = []
    for file in files:
        try:
            with open(file, encoding="UTF-8") as fh:
                runs.append(json.load(fh))
        except (OSError, json.JSONDecodeError) as exc:
            print(f"[WARNING] Skipping unreadable benchmark file {file}: {exc}")
    if not runs:
        print("[ERROR] No readable benchmark run files found", file=sys.stderr)
        sys.exit(1)
    return runs


def _median(values: list[float | None]) -> float | None:
    """Median of the non-None values, or None when there are none.

    :param values: Values to compute the median of.
    :type values: list
    :return: Median of the non-None values, or None when there are none.
    :rtype: float | None
    """
    nums = [v for v in values if v is not None]
    return statistics.median(nums) if nums else None


def _to_float(value) -> float | None:
    """Convert a value to float, or None when it cannot be parsed.

    :param value: Value to convert.
    :return: Float representation of the value, or None when unparsable.
    :rtype: float | None
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _merge_extra_info(entries: list[dict]) -> dict:
    """Median the numeric extra_info metrics across sessions.

    String identifiers (``test type``, ``ID``) are kept from the first
    session. A metric that is not numeric (or missing) in a session is left
    out of the median for that session; when a metric is numeric in only some
    sessions the median of the available ones is used with a warning.

    :param entries: Benchmark entries to merge, one per session.
    :type entries: list
    :return: Merged extra_info dictionary with the per-metric medians.
    :rtype: dict
    """
    keys: set[str] = set()
    for entry in entries:
        keys.update((entry.get("extra_info") or {}).keys())

    merged: dict = {}
    for key in sorted(keys):
        values = [(entry.get("extra_info") or {}).get(key) for entry in entries]
        if key in _STRING_KEYS:
            merged[key] = values[0]
            continue
        numeric = [_to_float(v) for v in values]
        parsed = [v for v in numeric if v is not None]
        if not parsed:
            merged[key] = values[0]
        elif len(parsed) < len(values):
            print(f"[WARNING] Metric {key!r} missing in {len(values) - len(parsed)}/{len(values)} sessions; "
                  f"using median of the available ones")
            merged[key] = statistics.median(parsed)
        else:
            merged[key] = statistics.median(parsed)
    return merged


def merge_runs(runs: list[dict]) -> dict:
    """Merge several pytest-benchmark run dicts into a single one.

    Scenarios are grouped by their pytest-benchmark ``fullname`` and the
    merged file holds one entry per scenario with the median values across
    sessions. The ``machine_info`` of the first run is kept so CPU detection
    keeps working on the merged file.

    :param runs: Benchmark run dictionaries to merge.
    :type runs: list
    :return: Merged benchmark dictionary with per-scenario medians.
    :rtype: dict
    """
    machine_info = runs[0].get("machine_info") or {}
    by_name: dict[str, list[dict]] = {}
    for run in runs:
        for entry in run.get("benchmarks", []):
            key = entry.get("fullname") or entry.get("name")
            if key:
                by_name.setdefault(key, []).append(entry)

    benchmarks = []
    for key, entries in by_name.items():
        if len(entries) < len(runs):
            print(f"[WARNING] Scenario {key!r} present in {len(entries)}/{len(runs)} sessions; "
                  f"using median of the available ones")
        first = entries[0]
        time_values = [_to_float(entry.get("stats", {}).get("median")) for entry in entries]
        benchmarks.append({
            "name": first.get("name"),
            "fullname": first.get("fullname"),
            "stats": {"median": _median(time_values)},
            "extra_info": _merge_extra_info(entries),
        })

    return {"machine_info": machine_info, "benchmarks": benchmarks}


def main() -> int:
    """Merge benchmark runs from ``--input`` into a single JSON at ``--output``.

    Exits with code 1 and an error message on stderr when there are no
    readable runs or no scenarios to merge. Returns 0 on success.

    :return: Exit code, 0 on success and 1 on error.
    :rtype: int
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Directory holding the session JSON files.")
    parser.add_argument("--output", required=True, help="Path of the merged output JSON.")
    args = parser.parse_args()

    runs = load_runs(Path(args.input))
    merged = merge_runs(runs)
    if not merged["benchmarks"]:
        print("[ERROR] No benchmark scenarios found in the input runs", file=sys.stderr)
        return 1

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(merged, indent=2), encoding="UTF-8")
    print(f"Merged {len(runs)} run(s) ({len(merged['benchmarks'])} scenarios) into {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
