from unittest.mock import create_autospec

import pytest

from autosubmit.database.db_manager_job_list import JobsDbManager


def _job(**overrides):
    base = {
        'chunk': 1, 'date': '1990-01-01', 'split': 1, 'splits': 2,
        'name': 'test_job', 'section': 'TEST_SECTION',
        'script_name': 'test.cmd', 'status': 'WAITING',
        'member': 'fc0',
    }
    base.update(overrides)
    return base


def _should_delete(job, section_diff):
    mgr = create_autospec(JobsDbManager, instance=True)
    return JobsDbManager._should_delete_job(mgr, job, section_diff)


CASES = [
    pytest.param({'split': 3, 'date': '1990-01-01', 'chunk': 1}, {'splits': '5'}, False, id="int_splits_within_bounds"),
    pytest.param({'split': 3, 'date': '1990-01-01', 'chunk': 1}, {'splits': '2'}, True, id="int_splits_exceeded"),
    pytest.param({'split': 1, 'date': '1990-01-01', 'chunk': 1}, {"splits": "{'19900101': [2]}"}, False, id="dict_splits_within_bounds"),
    pytest.param({'split': 3, 'date': '1990-01-01', 'chunk': 1}, {"splits": "{'19900101': [2]}"}, True, id="dict_splits_exceeded"),
    pytest.param({'split': 1, 'date': '1990-02-01', 'chunk': 1}, {"splits": "{'19900101': [2]}"}, True, id="dict_splits_date_missing"),
    pytest.param({'split': 4, 'date': '1990-01-01', 'chunk': 2}, {"splits": "{'19900101': [2, 4]}"}, False, id="dict_splits_chunk2_within_bounds"),
    pytest.param({'split': 5, 'date': '1990-01-01', 'chunk': 2}, {"splits": "{'19900101': [2, 4]}"}, True, id="dict_splits_chunk2_exceeded"),
    pytest.param({'split': -1, 'date': '1990-01-01', 'chunk': 1}, {"splits": "{'19900101': [2]}"}, True, id="dict_splits_negative_split"),
    pytest.param({'split': 2, 'date': '1990-01-01', 'chunk': 1}, {"splits": "{'19900101': [2]}"}, False, id="dict_splits_exact_boundary"),
]


@pytest.mark.parametrize('job_overrides, section_diff, expected', CASES)
def test_should_delete_job_splits(job_overrides, section_diff, expected):
    assert _should_delete(_job(**job_overrides), section_diff) is expected
