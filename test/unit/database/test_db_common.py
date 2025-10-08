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

"""Unit tests for ``autosubmit.database.db_common``.

We cover mainly error and validation scenarios here. See
the ``test/integration/test_db_common.py`` for more tests.
"""

import inspect

import pytest

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database import db_common
from autosubmit.log.log import AutosubmitCritical


def test_db_common_sqlite_multiprocessing_queue_error(mocker):
    """Test the queue timeout error path for ``db_common`` functions.

    ``db_common`` uses multiprocessing and a ``Queue`` to launch database operations (SQLite).
    This test simply iterates all the functions and confirms that upon an error in the
    ``Queue`` reading (like a timeout) the function will raise an ``AutosubmitCritical``
    error.

    TODO: In the future we should possibly drop that multiprocessing approach, and use
          simpler timeouts directly in SQLAlchemy or sqlite3.
    """
    mocked_queue = mocker.patch('multiprocessing.Queue')
    mocker.patch('multiprocessing.Queue', return_value=mocked_queue)
    mocker.patch('multiprocessing.Process')

    mocked_queue.get.side_effect = [Exception]

    for fn in [
        'check_experiment_exists',
        'delete_experiment',
        'get_autosubmit_version',
        'get_experiment_id',
        'last_name_used',
        'save_experiment',
        'update_experiment_description_version'
    ]:
        try:
            db_common_fn = getattr(db_common, fn)
            sig = inspect.signature(db_common_fn)
            params = ['' for _ in range(len(sig.parameters))]
            db_common_fn(*params)
        except AutosubmitCritical:
            pass
        else:
            raise AssertionError(f'The function {fn} should raise an AutosubmitCritical in this case!')


def test_save_experiment_sqlite_open_conn_error(monkeypatch, tmp_path, mocker):
    """Test the ``open_conn`` error path for ``db_common`` functions.

    Several functions in ``db_common`` (SQLite) follow the pattern of
    calling ``check_db`` and then try/catch the ``open_conn`` call.

    Here, we verify that ``open_conn`` failing results in the expected
    outcome."""
    monkeypatch.setattr(db_common, 'TIMEOUT', 1)
    monkeypatch.setattr(BasicConfig, 'DB_PATH', str(tmp_path))

    mocker.patch('autosubmit.database.db_common.open_conn', side_effect=db_common.DbException('bla'))

    for fn in [
        'get_experiment_id',
        'update_experiment_description_version',
        'last_name_used',
        'delete_experiment',
        'save_experiment',
        'get_experiment_description',
        'get_autosubmit_version'
    ]:
        with pytest.raises(AutosubmitCritical):
            db_common_fn = getattr(db_common, fn)
            sig = inspect.signature(db_common_fn)
            params = ['' for _ in range(len(sig.parameters))]
            db_common_fn(*params)
