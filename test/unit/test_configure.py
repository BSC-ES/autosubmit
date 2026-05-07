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


@pytest.mark.parametrize("suffix", ["", "/", "//"])
def test_configure(mocker, tmp_path, suffix: str, autosubmit) -> None:
    # To update ``Path.home`` appending the provided suffix.
    mocker.patch("autosubmit.autosubmit.get_rc_path").return_value = Path(
        str(tmp_path) + suffix, ".autosubmitrc"
    )

    # assign values that will be passed on cmd
    database_filename = "autosubmit.db"
    db_path = tmp_path / "database"
    lr_path = tmp_path / "experiments"

    autosubmit.configure(
        advanced=False,
        database_path=str(db_path),
        database_filename=database_filename,
        local_root_path=str(lr_path),
        platforms_conf_path=None,
        jobs_conf_path=None,
        smtp_hostname=None,
        mail_from=None,
        machine=False,
        local=False,
    )

    expected = dedent(f"""\
        [database]
        backend = sqlite
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

    with open(tmp_path / ".autosubmitrc", "r") as file:
        assert file.read() == expected


@pytest.mark.parametrize("suffix", ["", "/", "//"])
def test_configure_advanced(mocker, tmp_path, suffix: str, autosubmit) -> None:
    """Test the advanced configuration options."""
    # To update ``Path.home`` appending the provided suffix.
    mocker.patch("autosubmit.autosubmit.get_rc_path").return_value = Path(
        str(tmp_path) + suffix, ".autosubmitrc"
    )

    # assign values that will be passed on cmd
    database_filename = "autosubmit.db"
    db_path = tmp_path / "database"
    lr_path = tmp_path / "experiments"
    platforms_conf = tmp_path / "platforms.yml"
    jobs_conf = tmp_path / "jobs.yml"
    smtp_hostname = "smtp.example.com"
    mail_from = "autosubmit@example.com"

    # Create the platform and jobs config files
    platforms_conf.write_text("# Platforms configuration\n")
    jobs_conf.write_text("# Jobs configuration\n")

    autosubmit.configure(
        advanced=True,
        database_path=str(db_path),
        database_filename=database_filename,
        local_root_path=str(lr_path),
        platforms_conf_path=str(platforms_conf),
        jobs_conf_path=str(jobs_conf),
        smtp_hostname=smtp_hostname,
        mail_from=mail_from,
        machine=False,
        local=False,
    )

    expected = dedent(f"""\
        [database]
        backend = sqlite
        path = {str(tmp_path)}/database
        filename = autosubmit.db
        
        [local]
        path = {str(tmp_path)}/experiments
        
        [conf]
        jobs = {str(jobs_conf)}
        platforms = {str(platforms_conf)}
        
        [mail]
        smtp_server = smtp.example.com
        mail_from = autosubmit@example.com
        
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

    with open(tmp_path / ".autosubmitrc", "r") as file:
        assert file.read() == expected


def test_configure_does_not_create_directories(mocker, tmp_path, autosubmit) -> None:
    """configure must not create directories. Moved to install"""
    mocker.patch("autosubmit.autosubmit.get_rc_path").return_value = (
        tmp_path / ".autosubmitrc"
    )

    db_path = tmp_path / "database"
    lr_path = tmp_path / "experiments"

    autosubmit.configure(
        advanced=False,
        database_path=str(db_path),
        database_filename="autosubmit.db",
        local_root_path=str(lr_path),
        platforms_conf_path=None,
        jobs_conf_path=None,
        smtp_hostname=None,
        mail_from=None,
        machine=False,
        local=False,
    )

    assert (tmp_path / ".autosubmitrc").exists()
    assert not (lr_path / "logs").exists()
    assert not (lr_path / "metadata").exists()
    assert not (lr_path / "metadata" / "structures").exists()
    assert not (lr_path / "metadata" / "data").exists()
    assert not (lr_path / "metadata" / "logs").exists()