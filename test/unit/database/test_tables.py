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

"""Unit tests for ``autosubmit.database.tables``."""

import pytest
from sqlalchemy import MetaData

from autosubmit.database.tables import (
    get_table_with_schema,
    get_table_from_name,
    get_all_tables_by_name,
    create_wrapper_tables,
    TableRegistry,
    ExperimentTable,
    JobDataTable,
    JobListTable,
    TABLES,
    WrapperInfoTable,
    WrapperJobsTable,
)


@pytest.fixture
def meta() -> MetaData:
    """Return a fresh SQLAlchemy MetaData instance."""
    return MetaData()

from autosubmit.database.tables import ExperimentTable, TableRegistry


def test_get_table():
    table = ExperimentTable
    table_registry = TableRegistry("testing")
    assert table.schema is None
    table = table_registry.get(table.name)
    assert table.schema == 'testing'


def test_get_table_from_name_invalid_table_name() -> None:
    """Raise ``KeyError`` for an unknown table name."""
    table_registry = TableRegistry("testing")

    with pytest.raises(KeyError) as exc_info:
        table_registry.get(table_name="catch-me-if-you-can")

    assert exc_info.value.args[0] == "No table definition found for 'catch-me-if-you-can'."


@pytest.mark.parametrize('schema', [None, 'paraguay'])
def test_get_table_from_name(schema):
    table_registry = TableRegistry(schema)
    table = table_registry.get(table_name=ExperimentTable.name)
    assert table.name == ExperimentTable.name
    assert len(table.columns) == len(ExperimentTable.columns)  # type: ignore
    assert all([left.name == right.name for left, right in zip(table.columns, ExperimentTable.columns)])  # type: ignore
    assert table.schema == schema


def test_create_wrapper_tables_creates_two_tables(meta):
    info_table, jobs_table = create_wrapper_tables('test_wrapper', meta)
    assert info_table.name == 'test_wrapper_info'
    assert jobs_table.name == 'test_wrapper_jobs'


def test_create_wrapper_tables_info_columns(meta):
    info_table, _ = create_wrapper_tables('my_wrapper', meta)
    col_names = {c.name for c in info_table.columns}
    expected = {
        'name', 'id', 'script_name', 'status',
        'local_logs_out', 'local_logs_err',
        'remote_logs_out', 'remote_logs_err',
        'updated_log', 'platform_name', 'wallclock',
        'num_processors', 'type', 'sections', 'method',
    }
    assert col_names == expected


def test_create_wrapper_tables_jobs_columns(meta):
    _, jobs_table = create_wrapper_tables('my_wrapper', meta)
    col_names = {c.name for c in jobs_table.columns}
    expected = {'package_id', 'package_name', 'job_name', 'timestamp'}
    assert col_names == expected


def test_create_wrapper_tables_jobs_has_unique_constraint(meta):
    _, jobs_table = create_wrapper_tables('my_wrapper', meta)
    constraint_names = {c.name for c in jobs_table.constraints}
    assert 'unique_my_wrapper_jobs_package_id_package_name_job_name' in constraint_names


def test_create_wrapper_tables_info_primary_key(meta):
    info_table, _ = create_wrapper_tables('wp', meta)
    pk_cols = [c.name for c in info_table.primary_key.columns]
    assert pk_cols == ['name']


def test_create_wrapper_tables_jobs_composite_primary_key(meta):
    _, jobs_table = create_wrapper_tables('wp', meta)
    pk_cols = {c.name for c in jobs_table.primary_key.columns}
    assert pk_cols == {'package_id', 'package_name', 'job_name'}


@pytest.mark.parametrize('schema', ['test_schema', None])
def test_registry_init(schema):
    registry = TableRegistry(schema=schema)
    assert registry._schema == schema


@pytest.mark.parametrize('schema', ['my_schema', None])
def test_registry_get_metadata_creates_instance(schema):
    registry = TableRegistry(schema=schema)
    meta = registry.metadata
    assert meta.schema == schema


def test_registry_get_metadata_caches_instance():
    registry = TableRegistry(schema='my_schema')
    meta1 = registry.metadata
    meta2 = registry.metadata
    assert meta1 is meta2


def test_registry_get_returns_cached_table():
    registry = TableRegistry(schema=None)
    table1 = registry.get('experiment')
    table2 = registry.get('experiment')
    assert table1 is table2


def test_registry_get_returns_table_with_schema():
    registry = TableRegistry(schema='test_schema')
    table = registry.get('experiment')
    assert table.name == 'experiment'
    assert table.metadata.schema == 'test_schema'


def test_registry_get_creates_table_with_schema():
    registry = TableRegistry(schema='build_schema')
    table = registry.get('job_data')
    assert table.name == 'job_data'
    assert table.metadata.schema == 'build_schema'


def test_registry_get_unknown_table_raises_keyerror():
    registry = TableRegistry(schema=None)
    with pytest.raises(KeyError, match="No table definition found for 'unknown_table'"):
        registry.get('unknown_table')


def test_registry_get_multiple_tables():
    registry = TableRegistry(schema='multi_schema')
    exp = registry.get('experiment')
    job = registry.get('job_data')
    job_list = registry.get('job_list')
    assert exp.name == 'experiment'
    assert job.name == 'job_data'
    assert job_list.name == 'job_list'
    # All share the same metadata.
    assert exp.metadata is job.metadata
    assert job.metadata is job_list.metadata


def test_registry_get_all_known_tables():
    """Verify all known table names can be retrieved."""
    registry = TableRegistry(schema=None)
    known_names = {t.name for t in TABLES}
    for name in known_names:
        table = registry.get(name)
        assert table is not None
        assert table.name == name


# --- get_all_tables_by_name ---

def test_get_all_tables_by_name_returns_dict():
    result = get_all_tables_by_name()
    assert isinstance(result, dict)


def test_get_all_tables_by_name_contains_expected_tables():
    result = get_all_tables_by_name()
    expected_names = {
        'experiment', 'db_version', 'experiment_structure',
        'experiment_status', 'experiment_run', 'job_data',
        'job_list', 'job_pkl', 'details', 'user_metrics',
        'wrappers_info', 'wrappers_jobs',
        'preview_wrappers_info', 'preview_wrappers_jobs',
    }
    for name in expected_names:
        assert name in result, f"Missing table: {name}"


def test_get_all_tables_by_name_values_are_tables():
    result = get_all_tables_by_name()
    for name, table in result.items():
        assert hasattr(table, 'name'), f"{name} is not a Table object"
        assert table.name == name


def test_get_all_tables_by_name_count():
    result = get_all_tables_by_name()
    assert len(result) == len(TABLES)


# --- TABLES tuple ---

@pytest.mark.parametrize('table', [
    ExperimentTable,
    JobDataTable,
    JobListTable,
    WrapperInfoTable,
    WrapperJobsTable,
])
def test_tables_tuple_contains(table):
    assert table in TABLES


def test_tables_tuple_length():
    assert len(TABLES) == 14


@pytest.mark.parametrize('table_name,expected_name,schema', [
    ('EXPERIMENT', 'experiment', None),
    ('Job_Data', 'job_data', 'my_schema'),
])
def test_get_table_from_name_case_insensitive(table_name, expected_name, schema):
    table = get_table_from_name(schema=schema, table_name=table_name)
    assert table.name == expected_name
    if schema is not None:
        assert table.metadata.schema == schema


@pytest.mark.parametrize('source_table', [JobDataTable, JobListTable])
def test_get_table_with_schema_preserves_columns(source_table):
    result = get_table_with_schema(schema='test', table=source_table)
    assert len(result.columns) == len(source_table.columns)
    orig_names = {c.name for c in source_table.columns}
    new_names = {c.name for c in result.columns}
    assert orig_names == new_names
