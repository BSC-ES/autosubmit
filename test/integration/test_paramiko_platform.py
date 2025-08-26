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

"""Integration tests for the paramiko platform.

Note that tests will start and destroy an SSH server. For unit tests, see ``test_paramiko_platform.py``
in the ``test/unit`` directory."""

from getpass import getuser
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter

if TYPE_CHECKING:
    from autosubmit.platforms.psplatform import PsPlatform

_EXPID = 't000'


@pytest.mark.docker
@pytest.mark.parametrize('filename, check', [
    ('test1', True),
    ('sub/test2', True)
], ids=['filename', 'filename_long_path'])
def test_send_file(filename: str, check: bool, autosubmit_exp, ssh_server):
    """This test opens an SSH connection (via sftp) and sends a file to the remote location.

    It launches a Docker Image using testcontainers library.
    """
    user = getuser()
    platform_name = 'TEST_PS_PLATFORM'

    remote_dir = '/app/'
    project = 'test'

    exp = autosubmit_exp(_EXPID, experiment_data={
        'PLATFORMS': {
            platform_name: {
                'TYPE': 'ps',
                'HOST': ssh_server.get_docker_client().host(),
                'PROJECT': project,
                'USER': user,
                'SCRATCH_DIR': remote_dir,
                'ADD_PROJECT_TO_HOST': 'False',
                'MAX_WALLCLOCK': '48:00',
                'DISABLE_RECOVERY_THREADS': 'True'
            }
        },
        'JOBS': {
            # FIXME: This is poorly designed. First, to load platforms you need an experiment
            #        (even if you are in test/code mode). Then, platforms only get the user
            #        populated by a submitter. This is strange, as the information about the
            #        user is in the ``AutosubmitConfig``, and the platform has access to the
            #        ``AutosubmitConfig``. It is just never accessing the user (expid, yes).
            'BECAUSE_YOU_NEED_AT_LEAST_ONE_JOB_USING_THE_PLATFORM': {
                'RUNNING': 'once',
                'SCRIPT': "sleep 0",
                'PLATFORM': platform_name
            }
        }
    })

    # We load the platforms with the submitter so that the platforms have all attributes.
    # NOTE: The set up of platforms is done partially in the platform constructor, and
    #       partially by a submitter (i.e. they are tightly coupled, which makes it hard
    #       to maintain & test).
    submitter = ParamikoSubmitter()
    submitter.load_platforms(asconf=exp.as_conf, retries=0)

    ps_platform: 'PsPlatform' = submitter.platforms[platform_name]

    ps_platform.connect(as_conf=exp.as_conf, reconnect=False, log_recovery_process=False)
    assert ps_platform.check_remote_permissions()

    # generate file
    if "/" in filename:
        filename_dir = Path(filename).parent
        Path(ps_platform.tmp_path, filename_dir).mkdir(parents=True, exist_ok=True)
        filename = Path(filename).name
    with open(str(Path(ps_platform.tmp_path, filename)), 'w') as f:
        f.write('test')

    assert ps_platform.send_file(filename)

    result = ssh_server.exec(f'ls {remote_dir}/{project}/{user}/{exp.expid}/LOG_{exp.expid}/{filename}')
    assert result.exit_code == 0
