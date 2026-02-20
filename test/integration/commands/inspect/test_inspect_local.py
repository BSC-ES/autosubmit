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
from textwrap import dedent, indent
from typing import Any

import pytest
from ruamel.yaml import YAML

from autosubmit.config.basicconfig import BasicConfig

# -- Tests
_TEMPLATE_CONTENT = dedent("""
echo "Hello World with id=Success"
echo "HPCARCH=%HPCARCH%"
echo "HPCROOTDIR=%HPCROOTDIR%"
echo "HPCLOGDIR=%HPCLOGDIR%"
echo "HPCPLATFORM=%HPCARCH%"
echo "HPCHOST=%HPCHOST%"
echo "HPCCUSTOM_DIR=%HPCCUSTOM_DIR%"
echo "HPCCUSTOM_DIR_POINTS_TO_OTHER_DIR=%HPCCUSTOM_DIR_POINTS_TO_OTHER_DIR%"
""")
_TAB_SPACES = 4
tabs = 4
_SCRIPT_CONTENT = indent(_TEMPLATE_CONTENT, " " * tabs * _TAB_SPACES)


@pytest.mark.parametrize("additional_data", [
    (dedent(f"""\
    TEST_REFERENCE: "OK"
    EXPERIMENT:
        NUMCHUNKS: '5'
    JOBS:
        job_with_chunks:
            SCRIPT: | {_SCRIPT_CONTENT}
            PLATFORM: LOCAL
            RUNNING: chunk
            wallclock: 00:01
        job_with_chunks_splits:
            SCRIPT: | {_SCRIPT_CONTENT}
            PLATFORM: LOCAL
            SPLITS: '2'
            RUNNING: chunk
            wallclock: 00:01
        job_with_members:
            SCRIPT: | {_SCRIPT_CONTENT}
            PLATFORM: LOCAL
            RUNNING: member
            wallclock: 00:01
        job_with_dates:
            SCRIPT: | {_SCRIPT_CONTENT}
            PLATFORM: LOCAL
            RUNNING: date
            wallclock: 00:01
        job_once:
            SCRIPT: | {_SCRIPT_CONTENT}
            PLATFORM: LOCAL
            RUNNING: once
            wallclock: 00:01
        job_other_platform:
            SCRIPT: | {_SCRIPT_CONTENT}
            PLATFORM: TEST_SLURM
            RUNNING: once
            wallclock: 00:01
        job_match_platform:
            SCRIPT: | {_SCRIPT_CONTENT}
            PLATFORM: TEST_PS
            RUNNING: once
            wallclock: 00:01
    """)),
    (dedent("""\
TEST_REFERENCE: "OK"
EXPERIMENT:
    NUMCHUNKS: '5'
    DATELIST: '20240101 20250101'
    MEMBERS: '000 001'
JOBS:
    job_with_chunks:
        FILE: test.sh 
        PLATFORM: LOCAL
        SPLITS: '2'
        RUNNING: chunk
        wallclock: 00:01
    job_with_chunks_splits:
        FILE: test.sh 
        PLATFORM: LOCAL
        SPLITS: '2'
        RUNNING: chunk
        wallclock: 00:01
    job_with_members:
        FILE: test.sh 
        PLATFORM: LOCAL
        RUNNING: member
        wallclock: 00:01
    job_with_dates:
        FILE: test.sh 
        PLATFORM: LOCAL
        RUNNING: date
        wallclock: 00:01
    job_once:
        FILE: test.sh 
        PLATFORM: LOCAL
        RUNNING: once
        wallclock: 00:01
    job_other_platform:
        FILE: test.sh 
        PLATFORM: TEST_SLURM
        RUNNING: once
        wallclock: 00:01
    job_match_platform:
        FILE: test.sh 
        PLATFORM: TEST_PS
        RUNNING: once
        wallclock: 00:01
""")),
], ids=[
    "HPC*_TEST_SCRIPT",
    "HPC*_TEST_FILE",
])
def test_inspect(
        tmp_path,
        autosubmit_exp,
        additional_data: str,
        general_data: dict[str, Any],
):
    """Test inspect command for local platform with different job types to see that HPC parameters are correctly set in the job scripts."""
    yaml = YAML(typ='rt')

    if 'FILE' in additional_data:
        general_data['PROJECT']['PROJECT_TYPE'] = 'local'
        templates_dir = tmp_path / "templates"
        templates_dir.mkdir(parents=True, exist_ok=True)
        general_data['LOCAL'] = {}
        general_data['LOCAL']['PROJECT_PATH'] = str(templates_dir)
        script_file = templates_dir / "test.sh"
        script_file.write_text(_TEMPLATE_CONTENT)
        script_file.chmod(0o755)

    # TODO: This should be 'local' but we can't customaize LOCAL platform yet, also scratch_dir, user, host, project shouldn't be modifiable at all
    general_data['PLATFORMS']['LOCAL'] = {'TYPE': 'ps', 'HOST': "127.0.0.1", 'SCRATCH_DIR': str(tmp_path), 'USER': ""}

    for hpcarch in ['TEST_PS', 'TEST_SLURM', 'LOCAL']:
        general_data['DEFAULT']['HPCARCH'] = hpcarch
        general_data['PLATFORMS'][hpcarch]['CUSTOM_DIR'] = 'test'
        general_data['PLATFORMS'][hpcarch]['CUSTOM_DIR_POINTS_TO_OTHER_DIR'] = '%TEST_REFERENCE%'
        as_exp = autosubmit_exp(experiment_data=general_data | yaml.load(additional_data), include_jobs=False, create=True)

        # TODO: This shouldn't be needed but we can't customaize LOCAL platform yet
        if hpcarch == 'LOCAL':
            general_data['PLATFORMS'][hpcarch]['PROJECT'] = as_exp.expid

        as_conf = as_exp.as_conf
        as_conf.set_last_as_command('inspect')
        hpcarch_info = as_conf.experiment_data.get('PLATFORMS', {}).get('TEST_PS', {})

        # TODO: This shouldn't be needed 'local' platform info should be inyected in the memory of the experiment_data when creating the experiment
        if hpcarch == 'LOCAL':
            expected_hpcrootdir = Path(BasicConfig.LOCAL_ROOT_DIR) / as_exp.expid / Path(BasicConfig.LOCAL_TMP_DIR)
            expected_hpclogdir = expected_hpcrootdir / f"LOG_{as_exp.expid}"
        else:
            expected_hpcrootdir = Path(hpcarch_info.get('SCRATCH_DIR', '')) / hpcarch_info.get('PROJECT', '') / hpcarch_info.get('USER', '') / as_exp.expid
            expected_hpclogdir = expected_hpcrootdir / f"LOG_{as_exp.expid}"

        templates_dir = Path(as_conf.basic_config.LOCAL_ROOT_DIR) / as_exp.expid / BasicConfig.LOCAL_TMP_DIR

        # Inspect the experiment
        as_exp.autosubmit.inspect(expid=as_exp.expid, lst=None, check_wrapper=False, force=True, filter_chunks=None, filter_section=None, filter_status=None, quick=False)
        assert as_exp.as_conf.experiment_data["HPCARCH"] == hpcarch
        assert as_exp.as_conf.experiment_data["HPCROOTDIR"] == str(expected_hpcrootdir)
        assert as_exp.as_conf.experiment_data["HPCLOGDIR"] == str(expected_hpclogdir)

        assert as_exp.expid in str(expected_hpclogdir)

        for file in templates_dir.glob(f"{as_exp.expid}*.cmd"):
            content = file.read_text()
            assert f"HPCARCH={hpcarch}" in content
            assert f"HPCPLATFORM={hpcarch}" in content
            assert f"HPCHOST={hpcarch_info['HOST']}" in content
            assert "HPCCUSTOM_DIR=test" in content
            assert "HPCCUSTOM_DIR_POINTS_TO_OTHER_DIR=OK" in content
            assert f"HPCROOTDIR={str(expected_hpcrootdir)}" in content
            assert f"HPCLOGDIR={str(expected_hpclogdir)}" in content
