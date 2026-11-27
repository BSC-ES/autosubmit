#!/usr/bin/env bash

set -Eeuo pipefail

# ============================================================
# Autosubmit PostgreSQL benchmark - Results summarisation
#
# Usage:
#   ./scripts/summarise.sh
#   cd scripts && ./summarise.sh
#
# Directory structure:
#   postgres_schemas_perf/
#   ├── results/
#   └── scripts/
#       └── summarise.sh
#
# Output:
#   results/summary/
#       tables/
#       plots/
#       report/
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

RESULTS_DIR="${PROJECT_DIR}/results"
SUMMARY_DIR="${RESULTS_DIR}/summary"

TABLES_DIR="${SUMMARY_DIR}/tables"
PLOTS_DIR="${SUMMARY_DIR}/plots"
REPORT_DIR="${SUMMARY_DIR}/report"

if [[ ! -d "$RESULTS_DIR" ]]; then
    echo "ERROR: Results directory not found: $RESULTS_DIR"
    exit 1
fi

mkdir -p "$TABLES_DIR" "$PLOTS_DIR" "$REPORT_DIR"

echo "================================================"
echo "Autosubmit PostgreSQL benchmark"
echo "Results summarisation"
echo "================================================"
echo
echo "Project:  $PROJECT_DIR"
echo "Results:  $RESULTS_DIR"
echo "Output:   $SUMMARY_DIR"
echo

if ! command -v python3 >/dev/null 2>&1; then
    echo "ERROR: Python 3 is required."
    exit 1
fi

if ! python3 -c "import matplotlib" 2>/dev/null; then
    echo "ERROR: matplotlib is required."
    echo "Install with: python3 -m pip install matplotlib"
    exit 1
fi

python3 - "$RESULTS_DIR" "$TABLES_DIR" "$PLOTS_DIR" "$REPORT_DIR" <<'PYTHON'

import csv
import re
import sys

from pathlib import Path

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt


# ============================================================
# Configuration
# ============================================================

RESULTS = Path(sys.argv[1])
TABLES = Path(sys.argv[2])
PLOTS = Path(sys.argv[3])
REPORT = Path(sys.argv[4])

TESTS = [
    ("T0-PG", 0),
    ("T0-AS", 0),
    ("T1", 1),
    ("T2", 10),
    ("T3", 100),
    ("T4", 1000),
    ("T5", 5000),
    ("T6", 10000),
]

COLORS = [
    "#64748b",
    "#0d9488",
    "#2563eb",
    "#2563eb",
    "#2563eb",
    "#2563eb",
    "#2563eb",
    "#dc2626",
]

QUERY_NAMES = [
    "Schemas",
    "Relations",
    "Attributes",
    "Dependencies",
    "Indexes",
    "Schema listing",
]

CATALOG_METRICS = [
    "schemas",
    "relations",
    "attributes",
    "indexes",
    "constraints",
    "types",
    "dependencies",
]


# ============================================================
# Helpers
# ============================================================

def latest_file(test, suffix):

    directory = RESULTS / test

    if not directory.is_dir():
        return None

    files = sorted(directory.glob(f"*_{suffix}"))

    return files[-1] if files else None


def read_csv(test, metric):

    filename = latest_file(test, f"{metric}.csv")

    if filename is None:
        return []

    with filename.open(newline="") as f:
        return list(csv.DictReader(f))


def read_text(test, metric):

    filename = latest_file(test, f"{metric}.txt")

    if filename is None:
        return ""

    return filename.read_text(errors="replace")


def number(value, default=None):

    try:
        if value is None or value == "":
            return default

        return float(value)

    except (TypeError, ValueError):
        return default


def integer(value, default=0):

    result = number(value)

    return int(result) if result is not None else default


def mib(value):

    return round(value / 1024**2, 2) if value is not None else None


def gib(value):

    return round(value / 1024**3, 3) if value is not None else None


def write_csv(filename, rows):

    if not rows:
        return

    columns = []

    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)

    with (TABLES / filename).open("w", newline="") as f:

        writer = csv.DictWriter(f, fieldnames=columns)

        writer.writeheader()
        writer.writerows(rows)


def save_plot(filename):

    plt.tight_layout()
    plt.savefig(PLOTS / filename, dpi=160, bbox_inches="tight")
    plt.close()


def test_colors(rows):

    return [
        COLORS[TESTS.index(
            next(t for t in TESTS if t[0] == row["test"])
        )]
        for row in rows
    ]


def setup_plot(title, ylabel):

    plt.figure(figsize=(10, 5))

    plt.title(title)
    plt.xlabel("Benchmark scenario")
    plt.ylabel(ylabel)

    plt.grid(axis="y", alpha=0.3)


def bar_plot(filename, title, ylabel, metric, rows):

    if not rows:
        return

    values = [number(row.get(metric)) for row in rows]
    labels = [row["test"] for row in rows]

    setup_plot(title, ylabel)

    plt.bar(
        labels,
        [v if v is not None else 0 for v in values],
        color=test_colors(rows),
    )

    save_plot(filename)


def scaling_plot(filename, title, ylabel, metric, rows,
                 log_y=False):

    if not rows:
        return

    plt.figure(figsize=(10, 5))

    for row in rows:

        value = number(row.get(metric))

        if value is None:
            continue

        x = row["experiments"]

        plt.scatter(
            x,
            value,
            label=row["test"],
            color=test_colors([row])[0],
            s=65,
        )

        plt.annotate(
            row["test"],
            (x, value),
            xytext=(5, 6),
            textcoords="offset points",
            fontsize=8,
        )

    plt.title(title)
    plt.xlabel("Number of experiments")
    plt.ylabel(ylabel)

    plt.xscale("symlog", linthresh=1)

    if log_y:
        plt.yscale("log")

    plt.grid(alpha=0.3)

    save_plot(filename)


def parse_system_metrics(text, test, experiments):

    metrics = {
        "test": test,
        "experiments": experiments,
        "pgdata_bytes": None,
        "postgres_rss_mib": None,
        "memory_total_gib": None,
        "memory_used_gib": None,
        "memory_available_gib": None,
        "memory_used_percent": None,
        "cpu_idle_percent": None,
        "cpu_busy_percent": None,
    }

    # PostgreSQL data directory

    match = re.search(
        r"===== POSTGRESQL DATA DIRECTORY =====\s*\n(\d+)",
        text,
    )

    if match:
        metrics["pgdata_bytes"] = int(match.group(1))

    # PostgreSQL aggregate RSS

    match = re.search(
        r"Aggregate PostgreSQL process RSS:\s*([\d.]+)\s*MiB",
        text,
    )

    if match:
        metrics["postgres_rss_mib"] = float(match.group(1))

    # System memory (free -b format)

    match = re.search(
        r"===== MEMORY =====\s*\n.*\nMem:\s+"
        r"(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)",
        text,
    )

    if match:

        total, used, free, shared, buff_cache, available = map(
            int, match.groups()
        )

        metrics["memory_total_gib"] = gib(total)
        metrics["memory_used_gib"] = gib(used)
        metrics["memory_available_gib"] = gib(available)

        metrics["memory_used_percent"] = round(
            (total - available) / total * 100,
            2,
        ) if total else None

    # CPU idle percentage (mpstat)

    cpu_idle = []

    for line in text.splitlines():

        fields = line.split()

        if len(fields) >= 12 and fields[1] == "all":

            try:
                cpu_idle.append(float(fields[-1]))
            except ValueError:
                pass

    if cpu_idle:

        idle = sum(cpu_idle) / len(cpu_idle)

        metrics["cpu_idle_percent"] = round(idle, 2)
        metrics["cpu_busy_percent"] = round(100 - idle, 2)

    return metrics


def get_baseline(rows):

    return next(
        (row for row in rows if row["test"] == "T0-AS"),
        None,
    )


# ============================================================
# Read benchmark results
# ============================================================

print("Reading benchmark results...")

overview = []
catalog_rows = []
database_rows = []
relation_rows = []
object_rows = []
catalog_size_rows = []
timing_rows = []
system_rows = []

available_tests = []

for test, experiments in TESTS:

    directory = RESULTS / test

    if not directory.is_dir():
        print(f"  Skipping {test}: directory not found")
        continue

    catalog = read_csv(test, "catalog_counts")
    schemas = read_csv(test, "schema_counts")
    database = read_csv(test, "database_sizes")
    relations = read_csv(test, "relation_counts")
    objects = read_csv(test, "autosubmit_object_counts")
    catalog_sizes = read_csv(test, "catalog_sizes")

    if not catalog:
        print(f"  Skipping {test}: catalog_counts.csv missing")
        continue

    print(f"  Processing {test} ({experiments:,} experiments)")

    available_tests.append(test)

    c = catalog[0]
    s = schemas[0] if schemas else {}

    # Database size

    autosubmit_size = next(
        (
            integer(row["size_bytes"])
            for row in database
            if row["datname"] == "autosubmit"
        ),
        0,
    )

    # Autosubmit objects

    object_totals = {
        "tables": 0,
        "indexes": 0,
        "sequences": 0,
        "views": 0,
        "materialized_views": 0,
        "columns": 0,
    }

    for row in objects:

        for key in object_totals:
            object_totals[key] += integer(row.get(key, 0))

    # Overview

    overview.append({
        "test": test,
        "experiments": experiments,
        "schemas": integer(c["schemas"]),
        "relations": integer(c["relations"]),
        "attributes": integer(c["attributes"]),
        "indexes": integer(c["indexes"]),
        "constraints": integer(c["constraints"]),
        "types": integer(c["types"]),
        "dependencies": integer(c["dependencies"]),
        "non_system_schemas": integer(s.get("non_system_schemas")),
        "database_size_bytes": autosubmit_size,
        "database_size_mib": mib(autosubmit_size),
        **object_totals,
    })

    # Catalog counts

    catalog_rows.append({
        "test": test,
        "experiments": experiments,
        **{
            key: integer(c[key])
            for key in CATALOG_METRICS
        },
    })

    # Database sizes

    for row in database:

        size = integer(row["size_bytes"])

        database_rows.append({
            "test": test,
            "experiments": experiments,
            "database": row["datname"],
            "size_bytes": size,
            "size_mib": mib(size),
            "size_gib": gib(size),
        })

    # Relation counts

    for row in relations:

        relation_rows.append({
            "test": test,
            "experiments": experiments,
            "relkind": row["relkind"],
            "object_count": integer(row["object_count"]),
        })

    # Autosubmit objects by schema

    for row in objects:

        object_rows.append({
            "test": test,
            "experiments": experiments,
            **row,
        })

    # PostgreSQL catalog sizes

    for row in catalog_sizes:

        relation_size = integer(row["relation_size_bytes"])
        total_size = integer(row["total_size_bytes"])

        catalog_size_rows.append({
            "test": test,
            "experiments": experiments,
            "relation": row["relname"],
            "relkind": row["relkind"],
            "relation_size_bytes": relation_size,
            "total_size_bytes": total_size,
            "relation_size_mib": mib(relation_size),
            "total_size_mib": mib(total_size),
        })

    # Query execution timings

    text = read_text(test, "query_benchmarks")

    timings = re.findall(
        r"Time:\s*([\d.]+)\s*ms",
        text,
    )

    for name, timing in zip(QUERY_NAMES, timings):

        timing_rows.append({
            "test": test,
            "experiments": experiments,
            "query": name,
            "execution_time_ms": float(timing),
        })

    # System metrics

    text = read_text(test, "system")

    system_rows.append(
        parse_system_metrics(text, test, experiments)
    )


# ============================================================
# Calculate growth relative to T0-AS
# ============================================================

baseline = get_baseline(catalog_rows)

if baseline:

    for row in catalog_rows:

        for metric in CATALOG_METRICS:

            row[f"{metric}_growth"] = (
                row[metric] - baseline[metric]
            )

            row[f"{metric}_growth_percent"] = (
                round(
                    (row[metric] - baseline[metric])
                    / baseline[metric] * 100,
                    2,
                )
                if baseline[metric] else None
            )

# Calculate metrics per experiment.

for row in overview + catalog_rows + system_rows:

    experiments = row["experiments"]

    if experiments:

        for metric in CATALOG_METRICS:

            if metric in row:

                row[f"{metric}_per_experiment"] = round(
                    row[metric] / experiments,
                    3,
                )

    else:

        for metric in CATALOG_METRICS:

            if metric in row:
                row[f"{metric}_per_experiment"] = None


# ============================================================
# Write consolidated CSV files
# ============================================================

print()
print("Writing consolidated CSV files...")

write_csv("overview.csv", overview)
write_csv("catalog_growth.csv", catalog_rows)
write_csv("database_sizes.csv", database_rows)
write_csv("relation_counts.csv", relation_rows)
write_csv("schema_objects.csv", object_rows)
write_csv("catalog_sizes.csv", catalog_size_rows)
write_csv("query_timings.csv", timing_rows)
write_csv("system_metrics.csv", system_rows)


# ============================================================
# Generate plots
# ============================================================

print("Generating plots...")

if not overview:
    sys.exit("ERROR: No benchmark results were found.")


# 01. Schema growth

bar_plot(
    "01_schema_growth.png",
    "PostgreSQL schemas",
    "Number of schemas",
    "schemas",
    overview,
)


# 02. Catalog growth

for metric in CATALOG_METRICS[1:]:

    bar_plot(
        f"02_{metric}.png",
        f"PostgreSQL catalog: {metric.title()}",
        f"Number of {metric}",
        metric,
        overview,
    )


# 03. Database size

bar_plot(
    "03_database_size.png",
    "Autosubmit database size",
    "Database size (MiB)",
    "database_size_mib",
    overview,
)


# 04. PostgreSQL catalog sizes

important_catalogs = [
    "pg_attribute",
    "pg_depend",
    "pg_constraint",
    "pg_class",
    "pg_index",
    "pg_type",
    "pg_namespace",
]

plt.figure(figsize=(11, 6))

for relation in important_catalogs:

    values = []

    for test in available_tests:

        row = next(
            (
                r for r in catalog_size_rows
                if r["test"] == test
                and r["relation"] == relation
            ),
            None,
        )

        values.append(
            row["relation_size_mib"] if row else None
        )

    plt.plot(
        available_tests,
        values,
        marker="o",
        label=relation,
    )

plt.title("PostgreSQL system catalog relation sizes")
plt.xlabel("Benchmark scenario")
plt.ylabel("Relation size (MiB)")
plt.grid(alpha=0.3)
plt.legend()

save_plot("04_catalog_sizes.png")


# 05. Query execution performance

plt.figure(figsize=(11, 6))

for query in QUERY_NAMES:

    rows = [
        r for r in timing_rows
        if r["query"] == query
    ]

    if not rows:
        continue

    plt.plot(
        [r["experiments"] for r in rows],
        [r["execution_time_ms"] for r in rows],
        marker="o",
        label=query,
    )

plt.title("PostgreSQL catalog query execution times")
plt.xlabel("Number of experiments")
plt.ylabel("Execution time (ms)")
plt.xscale("symlog", linthresh=1)
plt.yscale("log")
plt.grid(alpha=0.3)
plt.legend()

save_plot("05_query_performance.png")


# 06. PostgreSQL aggregate RSS

bar_plot(
    "06_postgresql_memory.png",
    "PostgreSQL aggregate process RSS",
    "Memory (MiB)",
    "postgres_rss_mib",
    system_rows,
)

scaling_plot(
    "06b_postgresql_memory_scaling.png",
    "PostgreSQL RSS versus experiment count",
    "Aggregate RSS (MiB)",
    "postgres_rss_mib",
    system_rows,
)


# 07. System memory used

bar_plot(
    "07_system_memory.png",
    "System memory usage",
    "Memory used (GiB)",
    "memory_used_gib",
    system_rows,
)


# 08. System memory available

scaling_plot(
    "08_memory_available.png",
    "Available system memory versus experiment count",
    "Available memory (GiB)",
    "memory_available_gib",
    system_rows,
)


# 09. System memory utilisation

scaling_plot(
    "09_memory_utilisation.png",
    "System memory utilisation versus experiment count",
    "Memory utilisation (%)",
    "memory_used_percent",
    system_rows,
)


# 10. CPU utilisation

scaling_plot(
    "10_cpu_utilisation.png",
    "CPU utilisation versus experiment count",
    "CPU busy (%)",
    "cpu_busy_percent",
    system_rows,
)


# 11. PostgreSQL storage

storage_rows = [
    row for row in system_rows
    if row["pgdata_bytes"] is not None
]

if storage_rows:

    plt.figure(figsize=(10, 5))

    plt.bar(
        [r["test"] for r in storage_rows],
        [r["pgdata_bytes"] / 1024**3 for r in storage_rows],
        color=test_colors(storage_rows),
    )

    plt.title("PostgreSQL data directory size")
    plt.xlabel("Benchmark scenario")
    plt.ylabel("Size (GiB)")
    plt.grid(axis="y", alpha=0.3)

    save_plot("11_postgresql_storage.png")


# 12. Catalog growth versus experiment count

for metric in [
    "relations",
    "attributes",
    "indexes",
    "dependencies",
]:

    scaling_plot(
        f"12_{metric}_scaling.png",
        f"{metric.title()} versus experiment count",
        f"Number of {metric}",
        metric,
        catalog_rows,
    )


# 13. Objects per schema

if object_rows:

    values = [
        integer(row["tables"])
        for row in object_rows
        if row.get("schema_name") != "public"
    ]

    if values:

        plt.figure(figsize=(10, 5))

        plt.hist(
            values,
            bins=20,
            color="#2563eb",
            edgecolor="white",
        )

        plt.title("Distribution of tables per Autosubmit schema")
        plt.xlabel("Tables per schema")
        plt.ylabel("Number of schemas")
        plt.grid(axis="y", alpha=0.3)

        save_plot("13_objects_per_schema.png")


# ============================================================
# Generate Markdown report
# ============================================================

print("Generating Markdown report...")

with (REPORT / "summary.md").open("w") as f:

    f.write("# Autosubmit PostgreSQL benchmark\n\n")

    # Overview

    f.write("## Overview\n\n")

    f.write(
        "| Test | Experiments | Schemas | Relations | "
        "Attributes | Indexes | Constraints | "
        "Dependencies | Database (MiB) |\n"
    )

    f.write("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")

    for row in overview:

        f.write(
            f"| {row['test']} "
            f"| {row['experiments']:,} "
            f"| {row['schemas']:,} "
            f"| {row['relations']:,} "
            f"| {row['attributes']:,} "
            f"| {row['indexes']:,} "
            f"| {row['constraints']:,} "
            f"| {row['dependencies']:,} "
            f"| {row['database_size_mib']:,.2f} |\n"
        )

    # Query timings

    f.write("\n## Query execution times\n\n")

    f.write(
        "| Test | Experiments | Query | Execution time (ms) |\n"
    )

    f.write("|---|---:|---|---:|\n")

    for row in timing_rows:

        f.write(
            f"| {row['test']} "
            f"| {row['experiments']:,} "
            f"| {row['query']} "
            f"| {row['execution_time_ms']:.3f} |\n"
        )

    # System resources

    f.write("\n## System resource metrics\n\n")

    f.write(
        "| Test | Experiments | PostgreSQL RSS (MiB) | "
        "Memory available (GiB) | Memory used (%) | "
        "CPU busy (%) | PGDATA (GiB) |\n"
    )

    f.write("|---|---:|---:|---:|---:|---:|---:|\n")

    for row in system_rows:

        def fmt(key, digits=2):

            value = row.get(key)

            return f"{value:.{digits}f}" if value is not None else "N/A"

        f.write(
            f"| {row['test']} "
            f"| {row['experiments']:,} "
            f"| {fmt('postgres_rss_mib')} "
            f"| {fmt('memory_available_gib')} "
            f"| {fmt('memory_used_percent')} "
            f"| {fmt('cpu_busy_percent')} "
            f"| {fmt('pgdata_bytes', 0) if row['pgdata_bytes'] is not None else 'N/A'} |\n"
        )

    # Growth per experiment

    f.write("\n## Catalog objects per experiment\n\n")

    f.write(
        "| Test | Experiments | Relations/experiment | "
        "Attributes/experiment | Indexes/experiment |\n"
    )

    f.write("|---|---:|---:|---:|---:|\n")

    for row in catalog_rows:

        experiments = row["experiments"]

        if not experiments:
            continue

        f.write(
            f"| {row['test']} "
            f"| {experiments:,} "
            f"| {row['relations_per_experiment']:.2f} "
            f"| {row['attributes_per_experiment']:.2f} "
            f"| {row['indexes_per_experiment']:.2f} |\n"
        )

    # Plots

    f.write("\n## Plots\n\n")

    for filename in sorted(PLOTS.glob("*.png")):

        f.write(
            f"### {filename.stem}\n\n"
            f"![{filename.stem}](../plots/{filename.name})\n\n"
        )

    # Notes

    f.write("\n## Notes\n\n")

    f.write(
        "- T0-PG represents the PostgreSQL installation baseline.\n"
        "- T0-AS represents PostgreSQL with Autosubmit installed.\n"
        "- T1-T6 represent increasing experiment counts.\n"
        "- Missing measurements are represented as N/A, not zero.\n"
        "- PostgreSQL RSS is the aggregate RSS of PostgreSQL processes. "
        "It is not equivalent to unique physical memory consumption.\n"
        "- System memory utilisation is calculated as "
        "(total - available) / total.\n"
        "- CPU busy is calculated as 100 - average CPU idle.\n"
        "- CPU and memory figures represent collected snapshots, "
        "not continuous peak measurements.\n"
        "- Query timings are individual measurements, not averages "
        "of repeated benchmark runs.\n"
        "- PostgreSQL catalog total sizes can overlap because "
        "pg_total_relation_size includes indexes and TOAST storage.\n"
        "- Catalog objects per experiment are aggregate counts "
        "divided by experiment count.\n"
        "- These results measure database size and resource snapshots, "
        "not concurrent operational workload capacity.\n"
    )


# ============================================================
# Summary
# ============================================================

print()
print("================================================")
print("Summarisation completed")
print("================================================")
print()
print(f"Tests processed: {', '.join(available_tests)}")
print()
print(f"Tables:  {TABLES}")
print(f"Plots:   {PLOTS}")
print(f"Report:  {REPORT / 'summary.md'}")
print()

PYTHON

echo "================================================"
echo "Done."
echo "================================================"
