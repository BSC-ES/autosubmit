# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
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
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

"""Utility code for Postgres."""

from sqlalchemy import Connection, text

__all__ = [
    'setup_pg_db'
]


def setup_pg_db(conn: Connection) -> None:
    """Reset the database.

    Drops all schemas except the system ones and restoring the public schema.

    :param conn: Database connection.
    """
    # Get all schema names that are not from the system
    results = conn.execute(
        text("""SELECT schema_name FROM information_schema.schemata
               WHERE schema_name NOT LIKE 'pg_%'
               AND schema_name != 'information_schema'""")
    ).all()
    schema_names = [res[0] for res in results]

    # Drop all schemas
    for schema_name in schema_names:
        conn.execute(text(f"""DROP SCHEMA IF EXISTS "{schema_name}" CASCADE"""))

    # Restore default public schema
    conn.execute(text("CREATE SCHEMA public"))
    conn.execute(text("GRANT ALL ON SCHEMA public TO public"))
    conn.execute(text("GRANT ALL ON SCHEMA public TO postgres"))
