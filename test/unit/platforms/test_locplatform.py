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

"""Unit tests for the Local Platform."""

import pytest
from pathlib import Path

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.platforms.locplatform import LocalPlatform

_EXPID = 't001'


def test_local_platform_copy():
    local_platform = LocalPlatform(_EXPID, 'local', {}, auth_password=None)

    copied = local_platform.create_a_new_copy()

    assert local_platform.name == copied.name
    assert local_platform.expid == copied.expid
    assert local_platform.get_checkhost_cmd() == copied.get_checkhost_cmd()


@pytest.mark.parametrize(
    'count,stats_file_exists,job_fail_count,remote_file_exists',
    [
        (-1, True, 0, True),
        (0, False, 0, False),
        (1, False, 1, True),
        (100, True, 100, True)
    ],
    ids=[
        'use fail_count, delete stats_file, remote file transferred',
        'use count, no stats_file, failed to transfer',
        'use count, no stats_file, remote file transferred',
        'use count, delete stats_file, remote file transferred',
    ]
)
def test_get_stat_file(count: int, stats_file_exists: bool, job_fail_count: int, remote_file_exists: bool,
                       autosubmit_config, mocker):
    """Test that ``get_stat_file`` uses the correct file name."""
    mocked_os_remove = mocker.patch('os.remove')

    as_conf = autosubmit_config(_EXPID, experiment_data={})
    exp_path = Path(as_conf.basic_config.LOCAL_ROOT_DIR) / _EXPID

    local = LocalPlatform(_EXPID, __name__, as_conf.experiment_data)

    job = Job('job', '1', Status.WAITING, None, None)
    job.fail_count = job_fail_count

    # TODO: this is from ``job.py``; we can probably find an easier way to fetch the file name,
    #       so we can re-use it in tests (e.g. move the logic to a small function/property/etc.).
    if count == -1:
        filename = f"{job.stat_file}{job.fail_count}"
    else:
        filename = job.name + '_STAT_{0}'.format(str(count))

    if remote_file_exists:
        # Create fake remote stat file transferred.
        Path(exp_path, as_conf.basic_config.LOCAL_TMP_DIR, f'LOG_{_EXPID}', filename).touch()

    if stats_file_exists:
        # Create fake local stat file, to be deleted before copying the remote file (created above).
        Path(exp_path, as_conf.basic_config.LOCAL_TMP_DIR, filename).touch()

    assert remote_file_exists == local.get_stat_file(job=job, count=count)
    assert mocked_os_remove.called == stats_file_exists
