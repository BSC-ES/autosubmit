# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

_TEMPLATE_CONTENT_CALENDAR_SPLITS = dedent("""
echo "SPLIT_START_DATE=%SPLIT_START_DATE%"
echo "SPLIT_END_DATE=%SPLIT_END_DATE%"
echo "CHUNK_START_DATE=%CHUNK_START_DATE%"
echo "CHUNK_END_DATE=%CHUNK_END_DATE%"
echo "SPLIT_FIRST=%SPLIT_FIRST%"
echo "SPLIT_LAST=%SPLIT_LAST%"
echo "CHUNK_FIRST=%CHUNK_FIRST%"
echo "CHUNK_LAST=%CHUNK_LAST%"
echo "CURRENT_SPLIT=%SPLIT%"
echo "MAX_SPLITS=%SPLITS%"
echo "CURRENT_CHUNK=%CHUNK%"
echo "MAX_CHUNKS=%EXPERIMENT.NUMCHUNKS%"
""")

_TAB_SPACES = 4
tabs = 4
_SCRIPT_CONTENT = indent(_TEMPLATE_CONTENT, " " * tabs * _TAB_SPACES)
_SCRIPT_CONTENT_CALENDAR_SPLITS = indent(_TEMPLATE_CONTENT_CALENDAR_SPLITS, " " * tabs * _TAB_SPACES)


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
    """Test inspect command for LOCAL platform.

     Uses different job types to see that HPC parameters are correctly set in the job scripts."""
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

    # TODO: This should be 'local' but we can't customize LOCAL platform yet,
    #       also scratch_dir, user, host, project shouldn't be allowed to be modified.
    general_data['PLATFORMS']['LOCAL'] = {'TYPE': 'ps', 'HOST': "127.0.0.1", 'SCRATCH_DIR': str(tmp_path), 'USER': ""}
    for hpcarch in ['TEST_PS', 'TEST_SLURM', 'LOCAL']:
        general_data['DEFAULT']['HPCARCH'] = hpcarch
        general_data['PLATFORMS'][hpcarch]['CUSTOM_DIR'] = 'test'
        general_data['PLATFORMS'][hpcarch]['CUSTOM_DIR_POINTS_TO_OTHER_DIR'] = '%TEST_REFERENCE%'
        as_exp = autosubmit_exp(experiment_data=general_data | yaml.load(additional_data), include_jobs=False, create=True)

        # TODO: This shouldn't be needed but we can't customize LOCAL platform yet
        if hpcarch == 'LOCAL':
            general_data['PLATFORMS'][hpcarch]['PROJECT'] = as_exp.expid

        as_conf = as_exp.as_conf
        as_conf.set_last_as_command('inspect')
        hpcarch_info = as_conf.experiment_data.get('PLATFORMS', {}).get('TEST_PS', {})

        # TODO: This shouldn't be needed 'local' platform info should be injected in
        #       the memory of the experiment_data when creating the experiment.
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


@pytest.mark.parametrize("additional_data", [
    (dedent(f"""\
    JOBS:
        test_auto:
            SCRIPT: | {_SCRIPT_CONTENT_CALENDAR_SPLITS}
            PLATFORM: LOCAL
            RUNNING: chunk
            wallclock: 00:01
            SPLITS: "auto"
            SPLITPOLICY: 'flexible'
    """)),
    (dedent(f"""\
    JOBS:
        test_auto:
            SCRIPT: | {_SCRIPT_CONTENT_CALENDAR_SPLITS}
            PLATFORM: LOCAL
            RUNNING: chunk
            wallclock: 00:01
            SPLITS: "auto"
            SPLITPOLICY: 'flexible'
            SPLITSIZE: '1'
            SPLITSIZEUNIT: 'day'
    """)),
    (dedent(f"""\
    JOBS:
        test_auto:
            SCRIPT: | {_SCRIPT_CONTENT_CALENDAR_SPLITS}
            PLATFORM: LOCAL
            RUNNING: chunk
            wallclock: 00:01
            SPLITS: "auto"
            SPLITPOLICY: 'flexible'
            SPLITSIZE: '1'
            SPLITSIZEUNIT: 'hour'
    """)),
    (dedent(f"""\
JOBS:
    test_auto:
        SCRIPT: | {_SCRIPT_CONTENT_CALENDAR_SPLITS}
        PLATFORM: LOCAL
        RUNNING: chunk
        wallclock: 00:01
        SPLITS: "auto"
        SPLITPOLICY: 'flexible'
        SPLITSIZE: '2'
        SPLITSIZEUNIT: 'hour'
""")),
    (dedent(f"""\
JOBS:
    test_auto:
        SCRIPT: | {_SCRIPT_CONTENT_CALENDAR_SPLITS}
        PLATFORM: LOCAL
        RUNNING: chunk
        wallclock: 00:01
        SPLITS: "auto"
        SPLITPOLICY: 'flexible'
        chunksize: '3'
        SPLITSIZE: '2'
""")),
    (dedent(f"""\
    JOBS:
        test_auto:
            SCRIPT: | {_SCRIPT_CONTENT_CALENDAR_SPLITS}
            PLATFORM: LOCAL
            RUNNING: chunk
            wallclock: 00:01
            SPLITS: "auto"
            SPLITPOLICY: 'flexible'
            SPLITSIZE: '25'
            SPLITSIZEUNIT: 'hour'
    """)),
    (dedent(f"""\
    EXPERIMENT:
        NUMCHUNKS: 3
        CHUNKSIZE: 25
        CHUNKUNIT: hour
    JOBS:
        test:
            SCRIPT: | {_SCRIPT_CONTENT_CALENDAR_SPLITS}
            PLATFORM: LOCAL
            RUNNING: chunk
            wallclock: 00:01
""")),
    (dedent(f"""\
    EXPERIMENT:
        NUMCHUNKS: 3
        CHUNKSIZE: 1
        CHUNKUNIT: day
    JOBS:
        test:
            SCRIPT: | {_SCRIPT_CONTENT_CALENDAR_SPLITS}
            PLATFORM: LOCAL
            RUNNING: chunk
            wallclock: 00:01
""")),
    (dedent(f"""\
    EXPERIMENT:
        NUMCHUNKS: 3
        CHUNKSIZE: 1
        CHUNKUNIT: month
    JOBS:
        test:
            SCRIPT: | {_SCRIPT_CONTENT_CALENDAR_SPLITS}
            PLATFORM: LOCAL
            RUNNING: chunk
            wallclock: 00:01
""")),
], ids=[
    "CALENDAR_SPLITS_AUTO_SPLITSIZE_15_DAY",
    "CALENDAR_SPLITS_AUTO_SPLITSIZE_1_DAY",
    "CALENDAR_SPLITS_AUTO_SPLITSIZE_1_HOUR",
    "CALENDAR_SPLITS_AUTO_SPLITSIZE_2_HOUR",  # Equivalent to the removed test_calendar test
    "CALENDAR_SPLITS_AUTO_SPLITSIZE_splitsize_15_chunksize_3",  # Equivalent to docs/source/userguide/defining_workflows/code/jobs_splits_auto.yml and expdef_splits_auto.yml
    "CALENDAR_SPLITS_AUTO_check_hours_SPLITSIZE_25_HOUR",
    "CALENDAR_CHUNKS_check_hours_CHUNKSIZE_25_HOUR",
    "CALENDAR_CHUNKS_DAYS",
    "CALENDAR_CHUNKS_MONTHS",

])
def test_inspect_calendar(tmp_path, autosubmit_exp, general_data: dict[str, Any], additional_data: str):
    """Test that auto splits are correctly calculated and injected in the script."""

    # init
    yaml = YAML(typ='rt')
    general_data['EXPERIMENT'] = {
        'NUMCHUNKS': '3',
        'CHUNKSIZE': '1',
        'CHUNKUNIT': 'month',
        'SPLITSIZE': '15',
        'SPLITSIZEUNIT': 'day',
    }
    as_exp = autosubmit_exp(experiment_data=general_data | yaml.load(additional_data), include_jobs=False, create=True)
    as_conf = as_exp.as_conf
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR, as_exp.expid)
    tmp_path = Path(exp_path, BasicConfig.LOCAL_TMP_DIR)

    # Execute inspect
    as_conf.set_last_as_command('inspect')
    as_exp.autosubmit.inspect(expid=as_exp.expid, lst=None, check_wrapper=False, force=True, filter_chunks=None, filter_section=None, filter_status=None, quick=False)

    # Parse result
    jobs_info = {}
    for file in sorted(tmp_path.glob(f"{as_exp.expid}*.cmd")):
        content = file.read_text()
        parts = file.stem.split("_")
        # e.g. t001_20000101_fc0_1_1_TEST_AUTO -> parts[3]=chunk, parts[4]=split
        chunk = parts[3]
        split = parts[4]
        key = f"{chunk}_{split}"
        split_start_date = content.split("SPLIT_START_DATE=")[1].splitlines()[0].strip("'\"")
        split_end_date = content.split("SPLIT_END_DATE=")[1].splitlines()[0].strip("'\"")
        chunk_start_date = content.split("CHUNK_START_DATE=")[1].splitlines()[0].strip("'\"")
        chunk_end_date = content.split("CHUNK_END_DATE=")[1].splitlines()[0].strip("'\"")
        split_first = content.split("SPLIT_FIRST=")[1].splitlines()[0].strip("'\"")
        split_last = content.split("SPLIT_LAST=")[1].splitlines()[0].strip("'\"")
        chunk_first = content.split("CHUNK_FIRST=")[1].splitlines()[0].strip("'\"")
        chunk_last = content.split("CHUNK_LAST=")[1].splitlines()[0].strip("'\"")
        split = content.split("CURRENT_SPLIT=")[1].splitlines()[0].strip("'\"")
        max_splits = content.split("MAX_SPLITS=")[1].splitlines()[0].strip("'\"")
        chunk = content.split("CURRENT_CHUNK=")[1].splitlines()[0].strip("'\"")
        max_chunks = content.split("MAX_CHUNKS=")[1].splitlines()[0].strip("'\"")
        jobs_info[key] = {
            "SPLIT_START_DATE": int(split_start_date) if split_start_date.isdigit() else None,
            "SPLIT_END_DATE": int(split_end_date) if split_end_date.isdigit() else None,
            "CHUNK_START_DATE": int(chunk_start_date),
            "CHUNK_END_DATE": int(chunk_end_date),
            "SPLIT_FIRST": True if split_first.lower() == 'true' else False,
            "SPLIT_LAST": True if split_last.lower() == 'true' else False,
            "CHUNK_FIRST": True if chunk_first.lower() == 'true' else False,
            "CHUNK_LAST": True if chunk_last.lower() == 'true' else False,
            "CURRENT_SPLIT": int(split),
            "MAX_SPLITS": int(max_splits),
            "CURRENT_CHUNK": int(chunk),
            "MAX_CHUNKS": int(max_chunks),
        }

    assert jobs_info, "No cmd files found"

    lookup_date_errors = {}
    lookup_first_last_errors = {}
    # Assert that splits are correct
    for key, info in jobs_info.items():
        # Check one
        chunk_start_date = info["CHUNK_START_DATE"]
        chunk_end_date = info["CHUNK_END_DATE"]
        split_start_date = info["SPLIT_START_DATE"]
        split_end_date = info["SPLIT_END_DATE"]
        # Check two
        chunk = info["CURRENT_CHUNK"]
        split = info["CURRENT_SPLIT"]
        max_chunks = info["MAX_CHUNKS"]
        max_splits = info["MAX_SPLITS"]
        im_first_chunk: bool = info["CHUNK_FIRST"]
        im_first_split: bool = info["SPLIT_FIRST"]
        im_last_chunk: bool = info["CHUNK_LAST"]
        im_last_split: bool = info["SPLIT_LAST"]

        if split_start_date and split_end_date:
            if not (chunk_start_date <= chunk_end_date and split_start_date <= split_end_date and chunk_start_date <= split_start_date <= split_end_date <= chunk_end_date):
                lookup_date_errors[key] = jobs_info[key]
            if (im_first_chunk and chunk != 1 or im_first_split and split != 1 or im_last_split and split != max_splits or im_last_chunk and chunk != max_chunks) or (
                    im_first_chunk and im_first_split and split_start_date != chunk_start_date) or (im_last_chunk and im_last_split and split_end_date != chunk_end_date):
                lookup_first_last_errors[key] = jobs_info[key]
        else:
            if not chunk_start_date <= chunk_end_date or split_start_date or split_end_date:
                lookup_date_errors[key] = jobs_info[key]
            if (im_first_chunk and chunk != 1 or im_last_chunk and chunk != max_chunks) or (im_first_chunk and im_last_chunk and chunk_start_date != chunk_end_date):
                lookup_first_last_errors[key] = jobs_info[key]

    assert not lookup_first_last_errors, f"First/Last chunk/split errors found in splits: {lookup_first_last_errors}"
    assert not lookup_date_errors, f"Date errors found in splits: {lookup_date_errors}"
