#!/bin/bash

set -Eeuo pipefail

# ============================================================
# PostgreSQL 18.6 - Complete cluster reset
# Rocky Linux
#
# WARNING: DESTROYS ALL DATABASES, ROLES, SCHEMAS AND DATA.
# ============================================================

SERVICE="postgresql"
PGDATA="/var/lib/pgsql/data"
PGUSER="postgres"

AUTOSUBMIT_USER="autosubmit"
AUTOSUBMIT_PASSWORD="autosubmit"
AUTOSUBMIT_DB="autosubmit"

# Require explicit confirmation
if [[ "${CONFIRM_RESET:-}" != "YES" ]]; then
    echo "ERROR: This operation destroys the entire PostgreSQL cluster."
    echo "Run with: CONFIRM_RESET=YES $0"
    exit 1
fi

# Must run as root
if [[ $EUID -ne 0 ]]; then
    echo "ERROR: Run as root."
    exit 1
fi

# Safety checks
if [[ "$PGDATA" != "/var/lib/pgsql/data" ]]; then
    echo "ERROR: Unexpected PGDATA."
    exit 1
fi

if [[ ! -d "$PGDATA" || -L "$PGDATA" ]]; then
    echo "ERROR: Invalid PostgreSQL data directory."
    exit 1
fi

echo "================================================"
echo "PostgreSQL cluster reset"
echo "================================================"

echo "Service: $SERVICE"
echo "Data directory: $PGDATA"

echo
echo "[1/7] Stopping PostgreSQL..."

systemctl stop "$SERVICE"

if systemctl is-active --quiet "$SERVICE"; then
    echo "ERROR: PostgreSQL is still running."
    exit 1
fi

echo "PostgreSQL stopped."

echo
echo "[2/7] Removing existing cluster..."

find "$PGDATA" -mindepth 1 -maxdepth 1 \
    -exec rm -rf -- {} +

chown "$PGUSER:$PGUSER" "$PGDATA"
chmod 700 "$PGDATA"

echo "Existing cluster removed."

echo
echo "[3/7] Initialising fresh PostgreSQL cluster..."

INITDB="$(command -v initdb || true)"

if [[ -z "$INITDB" ]]; then
    for candidate in \
        /usr/bin/initdb \
        /usr/pgsql-18/bin/initdb
    do
        if [[ -x "$candidate" ]]; then
            INITDB="$candidate"
            break
        fi
    done
fi

if [[ -z "$INITDB" ]]; then
    echo "ERROR: initdb executable not found."
    exit 1
fi

runuser -u "$PGUSER" -- \
    "$INITDB" -D "$PGDATA"

echo
echo "[4/7] Starting PostgreSQL..."

systemctl start "$SERVICE"

echo "Waiting for PostgreSQL..."

TIMEOUT=30
ELAPSED=0

until pg_isready -q; do
    sleep 0.5
    ELAPSED=$((ELAPSED + 1))

    if [[ $ELAPSED -ge $((TIMEOUT * 2)) ]]; then
        echo "ERROR: PostgreSQL did not become ready."
        systemctl status "$SERVICE" --no-pager
        exit 1
    fi
done

echo "PostgreSQL is ready."

echo
echo "[5/7] Creating Autosubmit PostgreSQL user..."

runuser -u "$PGUSER" -- \
    psql -X -v ON_ERROR_STOP=1 \
    -c "CREATE USER ${AUTOSUBMIT_USER} WITH PASSWORD '${AUTOSUBMIT_PASSWORD}';"

runuser -u "$PGUSER" -- \
    psql -X -v ON_ERROR_STOP=1 \
    -c "ALTER USER ${AUTOSUBMIT_USER} CREATEDB;"

echo "Autosubmit user created."

echo
echo "[6/7] Creating Autosubmit database..."

runuser -u "$PGUSER" -- \
    createdb -O "$AUTOSUBMIT_USER" "$AUTOSUBMIT_DB"

echo "Autosubmit database created."

echo
echo "[7/7] Verifying installation..."

runuser -u "$PGUSER" -- \
    psql -X -c "SELECT version();"

echo
echo "PostgreSQL roles:"

runuser -u "$PGUSER" -- \
    psql -X -c "\du"

echo
echo "Databases:"

runuser -u "$PGUSER" -- \
    psql -X -c "\l"

echo
echo "Autosubmit database connection test:"

PGPASSWORD="$AUTOSUBMIT_PASSWORD" \
    psql -h localhost \
    -U "$AUTOSUBMIT_USER" \
    -d "$AUTOSUBMIT_DB" \
    -c "SELECT current_user, current_database();"

echo
echo "Initial disk consumption:"

du -sh "$PGDATA"

echo
echo "================================================"
echo "PostgreSQL reset completed successfully."
echo "================================================"
