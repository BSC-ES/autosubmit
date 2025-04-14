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

import pytest
from pathlib import Path
from textwrap import dedent
from typing import Callable

from autosubmitconfigparser.config.configcommon import AutosubmitConfig


def _get_script_files_path() -> Path:
    return Path(__file__).resolve().parents[1] / 'files'


def _write_test_files(expid, local_root_dir: Path):
    # touch as_misc
    platforms_path = Path(local_root_dir, f"{expid}/conf/platforms_{expid}.yml")
    jobs_path = Path(local_root_dir, f"{expid}/conf/jobs_{expid}.yml")

    exp_tmp_dir = local_root_dir / expid / 'tmp'
    aslogs_dir = local_root_dir / expid / 'ASLOGS'

    # Add each platform to test
    with platforms_path.open('w') as f:
        f.write(
            dedent(f"""\
                LOCAL_ROOT_DIR: {str(local_root_dir)}
                LOCAL_ASLOG_DIR: {aslogs_dir}
                LOCAL_TMP_DIR: {exp_tmp_dir}
                PLATFORMS:
                    pytest-pjm:
                        type: pjm
                        host: 127.0.0.1
                        user: {local_root_dir.owner()}
                        project: whatever
                        scratch_dir: {local_root_dir}/scratch   
                        MAX_WALLCLOCK: 48:00
                        TEMP_DIR: ''
                        MAX_PROCESSORS: 99999
                        queue: dummy
                        DISABLE_RECOVERY_THREADS: True
                    pytest-slurm:
                        type: slurm
                        host: 127.0.0.1
                        user: {local_root_dir.owner()}
                        project: whatever
                        scratch_dir: {local_root_dir}/scratch       
                        QUEUE: gp_debug
                        ADD_PROJECT_TO_HOST: false
                        MAX_WALLCLOCK: 48:00
                        TEMP_DIR: ''
                        MAX_PROCESSORS: 99999
                        PROCESSORS_PER_NODE: 123
                        DISABLE_RECOVERY_THREADS: True
                    pytest-ecaccess:
                        type: ecaccess
                        version: slurm
                        host: 127.0.0.1
                        QUEUE: nf
                        EC_QUEUE: hpc
                        user: {local_root_dir.owner()}
                        project: whatever
                        scratch_dir: {local_root_dir}/scratch
                        DISABLE_RECOVERY_THREADS: True
                    pytest-ps:
                        type: ps
                        host: 127.0.0.1
                        user: {local_root_dir.owner()}
                        project: whatever
                        scratch_dir: {local_root_dir}/scratch
                        DISABLE_RECOVERY_THREADS: True
                        """
                   )
        )
    # add a job of each platform type
    with jobs_path.open('w') as f:
        f.write(
            dedent(f"""\
                JOBS:
                    nodes:
                        SCRIPT: |
                            echo "Hello World"
                        For: 
                            PLATFORM: [ pytest-pjm , pytest-slurm, pytest-ecaccess, pytest-ps]
                            QUEUE: [dummy, gp_debug, nf, hpc]
                            NAME: [pjm, slurm, ecaccess, ps]
                        RUNNING: once
                        wallclock: 00:01
                        nodes: 1
                        threads: 40
                        tasks: 90
                    base:
                        SCRIPT: |
                            echo "Hello World"
                        For:
                            PLATFORM: [ pytest-pjm , pytest-slurm, pytest-ecaccess, pytest-ps]
                            QUEUE: [dummy, gp_debug, nf, hpc]
                            NAME: [pjm, slurm, ecaccess, ps]
                        RUNNING: once
                        wallclock: 00:01
                    wrap:
                        SCRIPT: |
                            echo "Hello World, I'm a wrapper"
                        For:
                             NAME: [horizontal,vertical,vertical_horizontal,horizontal_vertical]
                             DEPENDENCIES: [wrap_horizontal-1,wrap_vertical-1,wrap_vertical_horizontal-1,wrap_horizontal_vertical-1]
                        QUEUE: gp_debug
                        PLATFORM: pytest-slurm
                        RUNNING: chunk
                        wallclock: 00:01
                Wrappers:
                    wrapper_h:
                        type: horizontal
                        jobs_in_wrapper: wrap_horizontal
                    wrapper_v:
                        type: vertical
                        jobs_in_wrapper: wrap_vertical
                    wrapper_vh:
                        type: vertical-horizontal
                        jobs_in_wrapper: wrap_vertical_horizontal
                    wrapper_hv:
                        type: horizontal-vertical
                        jobs_in_wrapper: wrap_horizontal_vertical
                EXPERIMENT:
                    # List of start dates
                    DATELIST: '20000101'
                    # List of members.
                    MEMBERS: fc0 fc1
                    # Unit of the chunk size. Can be hour, day, month, or year.
                    CHUNKSIZEUNIT: month
                    # Size of each chunk.
                    CHUNKSIZE: '4'
                    # Number of chunks of the experiment.
                    NUMCHUNKS: '2'
                    CHUNKINI: ''
                    # Calendar used for the experiment. Can be standard or noleap.
                    CALENDAR: standard
                """)
        )

    expid_dir = Path(local_root_dir, f"scratch/whatever/{local_root_dir.owner()}/{expid}")
    dummy_dir = Path(local_root_dir, f"scratch/whatever/{local_root_dir.owner()}/{expid}/dummy_dir")
    real_data = Path(local_root_dir, f"scratch/whatever/{local_root_dir.owner()}/{expid}/real_data")
    # write some dummy data inside scratch dir
    expid_dir.mkdir(parents=True, exist_ok=True)
    dummy_dir.mkdir(parents=True, exist_ok=True)
    real_data.mkdir(parents=True, exist_ok=True)

    with open(dummy_dir.joinpath('dummy_file'), 'w') as f:
        f.write('dummy data')
    # create some dummy absolute symlinks in expid_dir to test migrate function
    Path(real_data / 'dummy_symlink').symlink_to(dummy_dir / 'dummy_file')
    return local_root_dir


@pytest.mark.parametrize("scheduler, job_type", [
    ('pjm', 'DEFAULT'),
    ('slurm', 'DEFAULT'),
    ('ecaccess', 'DEFAULT'),
    ('ps', 'DEFAULT'),
    ('pjm', 'NODES'),
    ('slurm', 'NODES'),
    ('slurm', 'horizontal'),
    ('slurm', 'vertical'),
    ('slurm', 'horizontal_vertical'),
    ('slurm', 'vertical_horizontal')
])
def test_scheduler_job_types(scheduler, job_type, autosubmit, autosubmit_exp: Callable) -> None:
    """
    Test that the default parameters are correctly set in the scheduler files.

    It is a comparison line to line, so the new templates must match the same
    line order as the old ones. Additional default parameters must be filled
    in the files/base_{scheduler}.yml as well as any change in the order.

    :param scheduler: Target scheduler
    :param job_type: Wrapped or not
    """

    exp = autosubmit_exp()
    expid = exp.expid
    as_conf: AutosubmitConfig = exp.as_conf

    _write_test_files(expid, Path(as_conf.basic_config.LOCAL_ROOT_DIR))

    exp_path = Path(as_conf.basic_config.LOCAL_ROOT_DIR, expid)

    autosubmit.inspect(
        expid,
        check_wrapper=True,
        force=True,
        lst=None,
        filter_chunks=None,
        filter_status=None,
        filter_section=None
    )

    # Load the base file for each scheduler
    scheduler = scheduler.upper()
    job_type = job_type.upper()
    expected_data = {}
    if job_type == "DEFAULT":
        for base_f in _get_script_files_path().glob('base_*.cmd'):
            if scheduler in base_f.stem.split('_')[1].upper():
                expected_data = Path(base_f).read_text()
                break
    elif job_type == "NODES":
        for nodes_f in _get_script_files_path().glob('nodes_*.cmd'):
            if scheduler in nodes_f.stem.split('_')[1].upper():
                expected_data = Path(nodes_f).read_text()
                break
    else:
        expected_data = (Path(_get_script_files_path()) / Path(
            f"base_{job_type.lower()}_{scheduler.lower()}.cmd")).read_text()
    if not expected_data:
        assert False, f"Could not find the expected data for {scheduler} and {job_type}"

    # Get the actual default parameters for the scheduler
    if job_type == "DEFAULT":
        actual = Path(exp_path, f"tmp/{expid}_BASE_{scheduler}.cmd").read_text()
    elif job_type == "NODES":
        actual = Path(exp_path, f"tmp/{expid}_NODES_{scheduler}.cmd").read_text()
    else:
        for asthread in Path(exp_path, f"tmp").glob(f"*ASThread_WRAP_{job_type}_[0-9]*.cmd"):
            actual = asthread.read_text()
            break
        else:
            assert False, f"Could not find the actual data for {scheduler} and {job_type}"
    # Remove all after # Autosubmit header
    # ###################
    # count number of lines in expected
    expected_lines = expected_data.split('\n')
    actual = actual.split('\n')[:len(expected_lines)]
    actual = '\n'.join(actual)
    for i, (line1, line2) in enumerate(zip(expected_data.split('\n'), actual.split('\n'))):
        if "PJM -o" in line1 or "PJM -e" in line1 or "#SBATCH --output" in line1 or "#SBATCH --error" in line1 or "#SBATCH -J" in line1:  # output error will be different
            continue
        elif "##" in line1 or "##" in line2:  # comment line
            continue
        elif "header" in line1 or "header" in line2:  # header line
            continue
        else:
            assert line1 == line2
