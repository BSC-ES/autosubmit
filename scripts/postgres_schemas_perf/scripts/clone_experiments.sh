#!/usr/bin/env bash

set -Eeuo pipefail

# ============================================================
# Autosubmit experiment cloning
#
# Usage:
#   ./clone_experiments.sh BASE_EXPID TOTAL_EXPERIMENTS
#
# Example:
#   ./clone_experiments.sh a000 100
#
# TOTAL includes the original experiment.
# Existing experiments are skipped.
# ============================================================

# Configuration

AUTOSUBMIT_HOME="${AUTOSUBMIT_HOME:-$HOME/autosubmit/experiments}"
DBNAME="${DBNAME:-autosubmit}"
DBUSER="${DBUSER:-postgres}"

BASE_EXP_ID="${1:-}"
TOTAL="${2:-}"

CURRENT_EXP_ID=""

# ------------------------------------------------------------
# Validation
# ------------------------------------------------------------

if [[ -z "$BASE_EXP_ID" || -z "$TOTAL" ]]; then
    echo "Usage: $0 BASE_EXPID TOTAL_EXPERIMENTS"
    exit 1
fi

if ! [[ "$TOTAL" =~ ^[0-9]+$ ]] || (( TOTAL < 1 )); then
    echo "ERROR: TOTAL must be a positive integer."
    exit 1
fi

if [[ ! -d "$AUTOSUBMIT_HOME/$BASE_EXP_ID" ]]; then
    echo "ERROR: Base experiment directory not found."
    exit 1
fi

if ! command -v pg_dump >/dev/null 2>&1 ||
   ! command -v psql >/dev/null 2>&1; then
    echo "ERROR: pg_dump and psql are required."
    exit 1
fi

# ------------------------------------------------------------
# Base36 conversion
# ------------------------------------------------------------

base36_encode() {
    local n="$1"
    local chars="0123456789abcdefghijklmnopqrstuvwxyz"
    local result=""

    if (( n == 0 )); then
        echo "0"
        return
    fi

    while (( n > 0 )); do
        result="${chars:n%36:1}${result}"
        n=$((n / 36))
    done

    echo "$result"
}

base36_decode() {
    local value="${1,,}"
    local chars="0123456789abcdefghijklmnopqrstuvwxyz"
    local result=0
    local digit

    for (( i=0; i<${#value}; i++ )); do
        digit=$(expr index "$chars" "${value:i:1}")

        if (( digit == 0 )); then
            echo "ERROR: Invalid base36 ID: $1" >&2
            return 1
        fi

        result=$((result * 36 + digit - 1))
    done

    echo "$result"
}

if ! [[ "$BASE_EXP_ID" =~ ^[a-z0-9]+$ ]]; then
    echo "ERROR: Invalid experiment ID."
    exit 1
fi

BASE_NUM=$(base36_decode "$BASE_EXP_ID")

# ------------------------------------------------------------
# PostgreSQL helpers
# ------------------------------------------------------------

psql_exec() {
    sudo -u "$DBUSER" psql \
        -X \
        -v ON_ERROR_STOP=1 \
        -d "$DBNAME" "$@"
}

schema_exists() {
    local schema="$1"

    local result

    result=$(psql_exec -Atq -c "
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.schemata
            WHERE schema_name = '$schema'
        );
    ")

    [[ "$result" == "t" ]]
}

experiment_exists() {
    local expid="$1"

    local result

    result=$(psql_exec -Atq -c "
        SELECT EXISTS (
            SELECT 1
            FROM public.experiment
            WHERE name = '$expid'
        );
    ")

    [[ "$result" == "t" ]]
}

# ------------------------------------------------------------
# Cleanup
# ------------------------------------------------------------

cleanup() {
    local expid="$1"

    echo
    echo "Cleaning up incomplete experiment: $expid"

    rm -rf -- "$AUTOSUBMIT_HOME/$expid"

    psql_exec -c \
        "DROP SCHEMA IF EXISTS \"$expid\" CASCADE;" \
        >/dev/null || true

    # Remove global registration if it was created.
    psql_exec -c "
        DELETE FROM public.experiment_status
        WHERE name = '$expid';

        DELETE FROM public.details
        WHERE exp_id IN (
            SELECT id
            FROM public.experiment
            WHERE name = '$expid'
        );

        DELETE FROM public.experiment
        WHERE name = '$expid';
    " >/dev/null || true
}

on_error() {
    local status=$1

    trap - ERR

    if [[ -n "$CURRENT_EXP_ID" ]]; then
        cleanup "$CURRENT_EXP_ID"
    fi

    echo
    echo "ERROR: Cloning failed."
    exit "$status"
}

trap 'on_error $?' ERR

# ------------------------------------------------------------
# Check PostgreSQL connection
# ------------------------------------------------------------

echo "Checking PostgreSQL connection..."

psql_exec -Atc "SELECT 1;" >/dev/null

# ------------------------------------------------------------
# Validate base experiment
# ------------------------------------------------------------

echo "Checking base experiment..."

schema_exists "$BASE_EXP_ID"

experiment_exists "$BASE_EXP_ID"

echo "Base experiment: $BASE_EXP_ID"
echo "Total experiments requested: $TOTAL"
echo "Database: $DBNAME"
echo

# ------------------------------------------------------------
# Temporary directory
# ------------------------------------------------------------

WORKDIR=$(mktemp -d)

trap 'rm -rf "$WORKDIR"' EXIT

DUMP_FILE="$WORKDIR/schema.sql"
RESTORE_FILE="$WORKDIR/schema_restored.sql"

# ------------------------------------------------------------
# Clone experiments
# ------------------------------------------------------------

COUNTED=1
CREATED=0
OFFSET=1
SKIPPED=0

while (( COUNTED < TOTAL )); do

    NUM=$((BASE_NUM + OFFSET))

    NEW_EXP_ID=$(base36_encode "$NUM")

    OFFSET=$((OFFSET + 1))

    DEST_DIR="$AUTOSUBMIT_HOME/$NEW_EXP_ID"

    # --------------------------------------------------------
    # Skip existing experiments
    # --------------------------------------------------------

    if [[ -e "$DEST_DIR" ]] ||
       schema_exists "$NEW_EXP_ID" ||
       experiment_exists "$NEW_EXP_ID"; then

        echo "Skipping $NEW_EXP_ID (already exists)"

        SKIPPED=$((SKIPPED + 1))
        COUNTED=$((COUNTED + 1))

        continue
    fi

    CURRENT_EXP_ID="$NEW_EXP_ID"

    echo
    echo "================================================"
    echo "Cloning $BASE_EXP_ID -> $NEW_EXP_ID"
    echo "================================================"

    # --------------------------------------------------------
    # Copy experiment directory
    # --------------------------------------------------------

    echo "Copying experiment directory..."

    cp -a "$AUTOSUBMIT_HOME/$BASE_EXP_ID" "$DEST_DIR"

    # --------------------------------------------------------
    # Rename configuration files
    # --------------------------------------------------------

    echo "Renaming configuration files..."

    for FILE in "$DEST_DIR"/conf/*"$BASE_EXP_ID"*.yml; do

        [[ -e "$FILE" ]] || continue

        NEW_FILE="${FILE//$BASE_EXP_ID/$NEW_EXP_ID}"

        mv "$FILE" "$NEW_FILE"

    done

    find "$DEST_DIR/conf" -type f -name '*.yml' \
        -exec sed -i \
        "s/${BASE_EXP_ID}/${NEW_EXP_ID}/g" {} +

    # --------------------------------------------------------
    # PostgreSQL schema cloning
    # --------------------------------------------------------

    echo "Dumping PostgreSQL schema..."

    sudo -u "$DBUSER" pg_dump \
        --schema="$BASE_EXP_ID" \
        --no-owner \
        --no-privileges \
        "$DBNAME" > "$DUMP_FILE"

    echo "Replacing schema references..."

    sed \
        -e "s/${BASE_EXP_ID}\./${NEW_EXP_ID}./g" \
        -e "s/SCHEMA ${BASE_EXP_ID}/SCHEMA ${NEW_EXP_ID}/g" \
        -e "s/SCHEMA IF NOT EXISTS ${BASE_EXP_ID}/SCHEMA IF NOT EXISTS ${NEW_EXP_ID}/g" \
        "$DUMP_FILE" > "$RESTORE_FILE"

    echo "Restoring PostgreSQL schema..."

    psql_exec < "$RESTORE_FILE"

    # --------------------------------------------------------
    # Register experiment in global database
    # --------------------------------------------------------

    echo "Registering experiment in global database..."

    psql_exec \
        -v base_id="$BASE_EXP_ID" \
        -v new_id="$NEW_EXP_ID" <<'SQL'

    SELECT set_config('clone.base_id', :'base_id', false);
    SELECT set_config('clone.new_id', :'new_id', false);

    DO $$
    DECLARE
        base_id TEXT := current_setting('clone.base_id');
        new_id TEXT := current_setting('clone.new_id');

        source_experiment RECORD;
        source_status RECORD;

        new_exp_id INTEGER;
        has_status BOOLEAN;

    BEGIN

        -- Retrieve source metadata.

        SELECT
            e.description,
            e.autosubmit_version,
            d."user" AS owner_user,
            d.created AS created_at,
            d.model,
            d.branch,
            d.hpc

        INTO source_experiment

        FROM public.experiment e

        JOIN public.details d
            ON d.exp_id = e.id

        WHERE e.name = base_id;

        IF NOT FOUND THEN
            RAISE EXCEPTION
                'Base experiment metadata not found: %', base_id;
        END IF;

        -- Retrieve source status.

        SELECT
            status,
            seconds_diff,
            modified

        INTO source_status

        FROM public.experiment_status

        WHERE name = base_id

        ORDER BY exp_id

        LIMIT 1;

        has_status := FOUND;

        -- Insert new experiment.

        INSERT INTO public.experiment (
            name,
            description,
            autosubmit_version
        )
        VALUES (
            new_id,
            source_experiment.description,
            source_experiment.autosubmit_version
        )
        RETURNING id INTO new_exp_id;

        -- Copy experiment details with new global ID.

        INSERT INTO public.details (
            exp_id,
            "user",
            created,
            model,
            branch,
            hpc
        )
        VALUES (
            new_exp_id,
            source_experiment.owner_user,
            source_experiment.created_at,
            source_experiment.model,
            source_experiment.branch,
            source_experiment.hpc
        );

        -- Copy status if available.

        IF has_status THEN

            INSERT INTO public.experiment_status (
                exp_id,
                name,
                status,
                seconds_diff,
                modified
            )
            VALUES (
                new_exp_id,
                new_id,
                source_status.status,
                source_status.seconds_diff,
                source_status.modified
            );

        END IF;

    END $$;

SQL

    CREATED=$((CREATED + 1))
    COUNTED=$((COUNTED + 1))

    CURRENT_EXP_ID=""

    echo
    echo "Successfully cloned $NEW_EXP_ID"
    echo "Progress: $COUNTED / $TOTAL"

done

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------

echo
echo "================================================"
echo "Cloning completed successfully."
echo "================================================"

echo "Base experiment: $BASE_EXP_ID"
echo "Target total: $TOTAL"
echo "Created during this run: $((CREATED - 1))"
echo "Skipped existing IDs: $SKIPPED"

echo
echo "Experiment schemas:"

psql_exec -c "
SELECT COUNT(*) AS total_experiment_schemas
FROM information_schema.schemata
WHERE schema_name ~ '^a[0-9a-z]{3}$';
"

echo
echo "Registered experiments:"

psql_exec -c "
SELECT COUNT(*) AS total_registered_experiments
FROM public.experiment
WHERE name ~ '^a[0-9a-z]{3}$';
"

echo
echo "Done."
