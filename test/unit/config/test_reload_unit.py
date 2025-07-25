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

import time
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "force_load,current_loaded_files,expected_result",
    [
        (True, None, ['%NOTFOUND%', '%TEST%', '%TEST2%']),
        (True, None, ['%NOTFOUND%', '%TEST%', '%TEST2%']),
        (False, "older", ['%NOTFOUND%', '%TEST%', '%TEST2%']),
        (False, "newer", None),
        (True, "older", ['%NOTFOUND%', '%TEST%', '%TEST2%']),
        (True, "newer", ['%NOTFOUND%', '%TEST%', '%TEST2%']),
    ]
)
def test_reload_unittest(autosubmit_config, tmpdir, force_load, current_loaded_files, expected_result):
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data={})
    as_conf.conf_folder_yaml = tmpdir / 'conf'
    Path(as_conf.conf_folder_yaml).mkdir(parents=True, exist_ok=True)

    with open(as_conf.conf_folder_yaml / 'test.yml', 'w') as f:
        f.write('VAR: ["%NOTFOUND%", "%TEST%", "%TEST2%"]')
    if current_loaded_files:
        if current_loaded_files == "older":
            as_conf.current_loaded_files[as_conf.conf_folder_yaml / 'test.yml'] = time.time() - 1000
        else:
            as_conf.current_loaded_files[as_conf.conf_folder_yaml / 'test.yml'] = time.time() + 1000
    as_conf.reload(force_load=force_load)
    if expected_result:
        assert as_conf.experiment_data['VAR'] == expected_result
    else:
        assert as_conf.experiment_data.get('VAR', None) is None


@pytest.mark.parametrize(
    "current_loaded_files,reload_while_running,expected_result",
    [
        (None, True, True),
        ("older", True, True),
        ("newer", True, False),
        (None, False, True),
        ("older", False, False),
        ("newer", False, False),
    ]
)
def test_needs_reload(autosubmit_config, tmpdir, current_loaded_files, reload_while_running, expected_result):
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data={})
    as_conf.conf_folder_yaml = as_conf.basic_config.LOCAL_ROOT_DIR / as_conf.expid / 'conf'

    # The fixture includes a file by default, to avoid reloading in tests.
    # So we reset it here.
    as_conf.current_loaded_files = {}

    now = time.time()

    with open(as_conf.conf_folder_yaml / 'test.yml', 'w') as f:
        f.write('VAR: ["%NOTFOUND%", "%TEST%", "%TEST2%"]')

    if current_loaded_files:
        if current_loaded_files == "older":
            as_conf.current_loaded_files[as_conf.conf_folder_yaml / 'test.yml'] = now - 1000
            as_conf.current_loaded_files[as_conf.conf_folder_yaml / 'test2.yml'] = now - 1000
        else:
            as_conf.current_loaded_files[as_conf.conf_folder_yaml / 'test.yml'] = now + 1000
            as_conf.current_loaded_files[as_conf.conf_folder_yaml / 'test2.yml'] = now + 1000

    if not reload_while_running:
        as_conf.experiment_data["CONFIG"] = {}
        as_conf.experiment_data["CONFIG"]["RELOAD_WHILE_RUNNING"] = False

    assert as_conf.needs_reload() == expected_result
