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
from pathlib import Path
from textwrap import dedent

import pytest


@pytest.mark.parametrize('suffix', [
    '',
    '/',
    '//'
])
def test_configure(mocker, tmp_path, suffix: str, autosubmit) -> None:
    # To update ``Path.home`` appending the provided suffix.
    mocker.patch('autosubmit.autosubmit.get_rc_path').return_value = \
        Path(str(tmp_path) + suffix, '.autosubmitrc')

    # assign values that will be passed on cmd
    database_filename = "autosubmit.db"
    db_path = tmp_path / 'database'
    lr_path = tmp_path / 'experiments'

    autosubmit.configure(
        advanced=False,
        database_path=str(db_path),
        database_filename=database_filename,
        local_root_path=str(lr_path),
        platforms_conf_path=None,  # type: ignore
        jobs_conf_path=None,  # type: ignore
        smtp_hostname=None,  # type: ignore
        mail_from=None,  # type: ignore
        machine=False,
        local=False)

    expected = dedent(f"""\
        [database]
        path = {str(tmp_path)}/database
        filename = autosubmit.db
        
        [local]
        path = {str(tmp_path)}/experiments
        
        [globallogs]
        path = {str(tmp_path)}/experiments/logs
        
        [structures]
        path = {str(tmp_path)}/experiments/metadata/structures
        
        [historicdb]
        path = {str(tmp_path)}/experiments/metadata/data
        
        [historiclog]
        path = {str(tmp_path)}/experiments/metadata/logs
        
        [autosubmitapi]
        url = http://192.168.11.91:8081 # Replace me?
        
        """)

    with open(tmp_path / '.autosubmitrc', 'r') as file:
        assert file.read() == expected
