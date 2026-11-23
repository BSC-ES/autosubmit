#!/bin/bash

set -Eeuo pipefail

# ============================================================
# Autosubmit PostgreSQL benchmark - Metrics collection
# Rocky Linux / PostgreSQL 18
#
# Usage:
#   DBNAME=autosubmit ./collect_metrics.sh T0-AS
#   DBNAME=autosubmit ./collect_metrics.sh T1
#
# Optional:
#   PGDATA=/var/lib/pgsql/data
#   DBNAME=postgres
# ============================================================

LABEL="${1:?Usage: $0 <test-label>}"

PGDATA="${PGDATA:-/var/lib/pgsql/data}"
DBNAME="${DBNAME:-postgres}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/../results/${LABEL}"

mkdir -p "$RESULTS_DIR"

TIMESTAMP="$(date +%Y%m%d_%H%M%S_%N)"
ISO_TIMESTAMP="$(date --iso-8601=ns)"

echo "================================================"
echo "Autosubmit PostgreSQL benchmark"
echo "================================================"

echo "Label: $LABEL"
echo "Database: $DBNAME"
echo "Timestamp: $TIMESTAMP"
echo "Output: $RESULTS_DIR"

# ------------------------------------------------------------
# PostgreSQL connection and validation
# ------------------------------------------------------------

PSQL=(runuser -u postgres -- psql -X --csv -v ON_ERROR_STOP=1)

if ! systemctl is-active --quiet postgresql; then
    echo "ERROR: PostgreSQL is not running."
    exit 1
fi

if ! "${PSQL[@]}" -d "$DBNAME" -c "SELECT 1;" >/dev/null; then
    echo "ERROR: Cannot connect to database '$DBNAME'."
    exit 1
fi

run_sql() {
    local filename="$1"
    local query="$2"

    echo "Collecting: $filename"

    "${PSQL[@]}" -d "$DBNAME" -c "$query" \
        >"$RESULTS_DIR/${TIMESTAMP}_${filename}.csv"
}

# ------------------------------------------------------------
# 1. Database sizes
# ------------------------------------------------------------

run_sql "database_sizes" "
SELECT
    datname,
    pg_database_size(datname) AS size_bytes
FROM pg_database
ORDER BY size_bytes DESC;
"

# ------------------------------------------------------------
# 2. PostgreSQL system catalog sizes
# ------------------------------------------------------------

# pg_total_relation_size() includes associated indexes and TOAST storage.
# So the total size reported for a catalog table can overlap with the size
# reported for its indexes.

run_sql "catalog_sizes" "
SELECT
    c.relname,
    c.relkind,
    pg_relation_size(c.oid) AS relation_size_bytes,
    pg_total_relation_size(c.oid) AS total_size_bytes
FROM pg_class c
JOIN pg_namespace n
    ON n.oid = c.relnamespace
WHERE n.nspname = 'pg_catalog'
ORDER BY total_size_bytes DESC;
"

# ------------------------------------------------------------
# 3. Catalog object counts
# ------------------------------------------------------------

# Counts of catalog entries across the entire database, not just
# objects belonging to pg_catalog.

run_sql "catalog_counts" "
SELECT
    (SELECT COUNT(*) FROM pg_namespace) AS schemas,
    (SELECT COUNT(*) FROM pg_class) AS relations,
    (SELECT COUNT(*) FROM pg_attribute) AS attributes,
    (SELECT COUNT(*) FROM pg_index) AS indexes,
    (SELECT COUNT(*) FROM pg_constraint) AS constraints,
    (SELECT COUNT(*) FROM pg_type) AS types,
    (SELECT COUNT(*) FROM pg_depend) AS dependencies;
"

# ------------------------------------------------------------
# 4. Relation counts by type
# ------------------------------------------------------------

run_sql "relation_counts" "
SELECT
    relkind,
    COUNT(*) AS object_count
FROM pg_class
GROUP BY relkind
ORDER BY relkind;
"

# ------------------------------------------------------------
# 5. Schema counts
# ------------------------------------------------------------

run_sql "schema_counts" "
SELECT
    COUNT(*) AS total_schemas,
    COUNT(*) FILTER (
        WHERE nspname !~ '^pg_'
        AND nspname <> 'information_schema'
    ) AS non_system_schemas
FROM pg_namespace;
"

# ------------------------------------------------------------
# 6. Autosubmit objects by schema
# ------------------------------------------------------------

# Count columns per relation and then aggregate them by schema.
# Partitioned tables (p) and partitioned indexes (I) are counted.
# Columns are counted only for ordinary and partitioned tables,
# not indexes or views.
# Other relation types, such as foreign tables, are intentionally excluded.
# Internal PostgreSQL schemas (including TOAST schemas) are also excluded.

run_sql "autosubmit_object_counts" "
WITH relation_counts AS (
    SELECT
        n.oid AS schema_oid,
        COUNT(*) FILTER (
            WHERE c.relkind IN ('r', 'p')
        ) AS tables,
        COUNT(*) FILTER (
            WHERE c.relkind IN ('i', 'I')
        ) AS indexes,
        COUNT(*) FILTER (
            WHERE c.relkind = 'S'
        ) AS sequences,
        COUNT(*) FILTER (
            WHERE c.relkind = 'v'
        ) AS views,
        COUNT(*) FILTER (
            WHERE c.relkind = 'm'
        ) AS materialized_views
    FROM pg_namespace n
    LEFT JOIN pg_class c
        ON c.relnamespace = n.oid
    GROUP BY n.oid
),
column_counts AS (
    SELECT
        n.oid AS schema_oid,
        COUNT(*) AS columns
    FROM pg_namespace n
    JOIN pg_class c
        ON c.relnamespace = n.oid
    JOIN pg_attribute a
        ON a.attrelid = c.oid
    WHERE c.relkind IN ('r', 'p')
      AND a.attnum > 0
      AND NOT a.attisdropped
    GROUP BY n.oid
)
SELECT
    n.nspname AS schema_name,
    r.tables,
    r.indexes,
    r.sequences,
    r.views,
    r.materialized_views,
    COALESCE(cc.columns, 0) AS columns
FROM pg_namespace n
JOIN relation_counts r
    ON r.schema_oid = n.oid
LEFT JOIN column_counts cc
    ON cc.schema_oid = n.oid
WHERE n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
ORDER BY n.nspname;
"

# ------------------------------------------------------------
# 7. System catalog maintenance statistics
# ------------------------------------------------------------

run_sql "system_table_statistics" "
SELECT
    relname,
    n_live_tup,
    n_dead_tup,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_sys_tables
ORDER BY n_dead_tup DESC;
"

# ------------------------------------------------------------
# 8. User table maintenance statistics
# ------------------------------------------------------------

run_sql "user_table_statistics" "
SELECT
    schemaname,
    relname,
    n_live_tup,
    n_dead_tup,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
ORDER BY schemaname, relname;
"

# ------------------------------------------------------------
# 9. Database activity statistics
# ------------------------------------------------------------

# pg_stat_database contains cumulative database statistics.
# Counters are accumulated since stats_reset and are not instantaneous.
# A clean PostgreSQL cluster gives us a consistent starting point
# for each benchmark scenario.

run_sql "database_statistics" "
SELECT
    datname,
    stats_reset,
    xact_commit,
    xact_rollback,
    blks_read,
    blks_hit,
    tup_returned,
    tup_fetched,
    temp_files,
    temp_bytes,
    deadlocks,
    checksum_failures,
    checksum_last_failure
FROM pg_stat_database
ORDER BY datname;
"

# ------------------------------------------------------------
# 10. PostgreSQL I/O statistics
# ------------------------------------------------------------

# pg_stat_io provides statistics grouped by backend type, object
# and I/O context. It is not a direct measurement of total physical
# disk traffic attributable to a particular experiment.

run_sql "io_statistics" "
SELECT *
FROM pg_stat_io;
"

# ------------------------------------------------------------
# 11. Active PostgreSQL connections
# ------------------------------------------------------------

run_sql "connections" "
SELECT
    datname,
    usename,
    state,
    COUNT(*) AS connection_count
FROM pg_stat_activity
GROUP BY datname, usename, state
ORDER BY connection_count DESC;
"

# ------------------------------------------------------------
# 12. PostgreSQL version
# ------------------------------------------------------------

run_sql "postgresql_version" "
SELECT
    version() AS full_version,
    current_setting('server_version_num') AS server_version_num;
"

# ------------------------------------------------------------
# 13. PostgreSQL configuration
# ------------------------------------------------------------

run_sql "postgresql_settings" "
SELECT
    name,
    setting,
    unit,
    source,
    pending_restart
FROM pg_settings
ORDER BY name;
"

# ------------------------------------------------------------
# 14. Catalog query execution timings
# ------------------------------------------------------------

# each query is executed only once, and sequentially.
# Individual execution times can be influenced by caching, background
# activity and operating-system scheduling.

echo "Collecting: query_benchmarks"

{
    echo "Timestamp: $ISO_TIMESTAMP"
    echo "Label: $LABEL"
    echo "Database: $DBNAME"
    echo
    echo "===== CATALOG QUERY BENCHMARKS ====="

    "${PSQL[@]}" -d "$DBNAME" <<'SQL'
\timing on

SELECT COUNT(*) AS schemas
FROM pg_namespace
WHERE nspname !~ '^pg_'
  AND nspname <> 'information_schema';

SELECT COUNT(*) AS relations
FROM pg_class;

SELECT COUNT(*) AS attributes
FROM pg_attribute;

SELECT COUNT(*) AS dependencies
FROM pg_depend;

SELECT COUNT(*) AS indexes
FROM pg_index;

SELECT nspname
FROM pg_namespace
WHERE nspname !~ '^pg_'
  AND nspname <> 'information_schema';
SQL

} >"$RESULTS_DIR/${TIMESTAMP}_query_benchmarks.txt" 2>&1

# ------------------------------------------------------------
# 15. Operating system measurements
# ------------------------------------------------------------

# RSS represents resident memory per process. PostgreSQL processes share
# memory mappings, so summing RSS can count shared memory multiple times.
# Aggregate RSS is retained as a process-level indicator, not as an
# estimate of PostgreSQL's actual total physical memory consumption.

echo "Collecting: system_metrics"

{
    echo "Timestamp: $ISO_TIMESTAMP"
    echo "Label: $LABEL"

    echo
    echo "===== MEMORY ====="
    free -b

    echo
    echo "===== DISK ====="
    df -B1 "$PGDATA"

    echo
    echo "===== POSTGRESQL DATA DIRECTORY ====="
    du -sb "$PGDATA"

    echo
    echo "===== POSTGRESQL DIRECTORY BREAKDOWN ====="
    du -sh "$PGDATA"/* 2>/dev/null | sort -h || true

    echo
    echo "===== POSTGRESQL PROCESSES ====="
    ps -eo pid,ppid,rss,vsz,%mem,%cpu,comm |
        grep '[p]ostgres' || true

    echo
    echo "===== POSTGRESQL MEMORY SUMMARY ====="

    ps -C postgres -o rss= |
        awk '
            {
                sum += $1
                count++
            }
            END {
                printf "Processes: %d\n", count
                printf "Aggregate PostgreSQL process RSS: %.2f MiB\n", sum / 1024
            }
        '

    echo
    echo "===== SYSTEM LOAD ====="
    uptime

    echo
    echo "===== CPU UTILIZATION (10-SECOND SAMPLE) ====="
    mpstat -P ALL 1 10

    echo
    echo "===== SWAP ====="
    swapon --show

} >"$RESULTS_DIR/${TIMESTAMP}_system.txt"

echo
echo "================================================"
echo "Metrics collection completed."
echo "Results: $RESULTS_DIR"
echo "================================================"
