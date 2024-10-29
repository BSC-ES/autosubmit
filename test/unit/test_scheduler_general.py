import pytest
from pathlib import Path
from autosubmit.autosubmit import Autosubmit
import os
import pwd

from test.unit.utils.common import create_database, init_expid

def _get_script_files_path() -> Path:
    return Path(__file__).resolve().parent / 'files'

# Maybe this should be a regression test

@pytest.fixture
def scheduler_tmpdir(tmpdir_factory):
    folder = tmpdir_factory.mktemp(f'scheduler_tests')
    os.mkdir(folder.join('scratch'))
    os.mkdir(folder.join('scheduler_tmp_dir'))
    file_stat = os.stat(f"{folder.strpath}")
    file_owner_id = file_stat.st_uid
    file_owner = pwd.getpwuid(file_owner_id).pw_name
    folder.owner = file_owner

    # Write an autosubmitrc file in the temporary directory
    autosubmitrc = folder.join('autosubmitrc')
    autosubmitrc.write(f'''
[database]
path = {folder}
filename = tests.db

[local]
path = {folder}

[globallogs]
path = {folder}

[structures]
path = {folder}

[historicdb]
path = {folder}

[historiclog]
path = {folder}

[defaultstats]
path = {folder}

''')
    os.environ['AUTOSUBMIT_CONFIGURATION'] = str(folder.join('autosubmitrc'))
    create_database(str(folder.join('autosubmitrc')))
    assert "tests.db" in [Path(f).name for f in folder.listdir()]
    init_expid(str(folder.join('autosubmitrc')), platform='local', create=False, test_type='test')
    assert "t000" in [Path(f).name for f in folder.listdir()]
    return folder

@pytest.fixture
def prepare_scheduler(scheduler_tmpdir):
    # touch as_misc
    platforms_path = Path(f"{scheduler_tmpdir.strpath}/t000/conf/platforms_t000.yml")
    jobs_path = Path(f"{scheduler_tmpdir.strpath}/t000/conf/jobs_t000.yml")
    # Add each platform to test
    with platforms_path.open('w') as f:
        f.write(f"""
PLATFORMS:
    pytest-pjm:
        type: pjm
        host: 127.0.0.1
        user: {scheduler_tmpdir.owner}
        project: whatever
        scratch_dir: {scheduler_tmpdir}/scratch   
        MAX_WALLCLOCK: 48:00
        TEMP_DIR: ''
        MAX_PROCESSORS: 99999
        queue: dummy
        DISABLE_RECOVERY_THREADS: True
    pytest-slurm:
        type: slurm
        host: 127.0.0.1
        user: {scheduler_tmpdir.owner}
        project: whatever
        scratch_dir: {scheduler_tmpdir}/scratch       
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
        user: {scheduler_tmpdir.owner}
        project: whatever
        scratch_dir: {scheduler_tmpdir}/scratch
        DISABLE_RECOVERY_THREADS: True
    pytest-ps:
        type: ps
        host: 127.0.0.1
        user: {scheduler_tmpdir.owner}
        project: whatever
        scratch_dir: {scheduler_tmpdir}/scratch
        DISABLE_RECOVERY_THREADS: True
        """)
    # add a job of each platform type
    with jobs_path.open('w') as f:
        f.write(f"""
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

    expid_dir = Path(f"{scheduler_tmpdir.strpath}/scratch/whatever/{scheduler_tmpdir.owner}/t000")
    dummy_dir = Path(f"{scheduler_tmpdir.strpath}/scratch/whatever/{scheduler_tmpdir.owner}/t000/dummy_dir")
    real_data = Path(f"{scheduler_tmpdir.strpath}/scratch/whatever/{scheduler_tmpdir.owner}/t000/real_data")
    # write some dummy data inside scratch dir
    os.makedirs(expid_dir, exist_ok=True)
    os.makedirs(dummy_dir, exist_ok=True)
    os.makedirs(real_data, exist_ok=True)

    with open(dummy_dir.joinpath('dummy_file'), 'w') as f:
        f.write('dummy data')
    # create some dummy absolute symlinks in expid_dir to test migrate function
    (real_data / 'dummy_symlink').symlink_to(dummy_dir / 'dummy_file')
    return scheduler_tmpdir

@pytest.fixture
def generate_cmds(prepare_scheduler):
    init_expid(os.environ["AUTOSUBMIT_CONFIGURATION"], platform='local', expid='t000', create=True)
    Autosubmit.inspect(expid='t000', check_wrapper=True, force=True, lst=None, filter_chunks=None, filter_status=None, filter_section=None)
    return prepare_scheduler

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

def test_scheduler_job_types(scheduler, job_type, generate_cmds):
    # Test code that uses scheduler and job_typedef test_default_parameters(scheduler: str, job_type: str, generate_cmds):
    """
    Test that the default parameters are correctly set in the scheduler files. It is a comparasion line to line, so the new templates must match the same line order as the old ones. Additional default parameters must be filled in the files/base_{scheduler}.yml as well as any change in the order
    :param generate_cmds: fixture that generates the templates.
    :param scheduler: Target scheduler
    :param job_type: Wrapped or not
    :return:
    """

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
        expected_data = (Path(_get_script_files_path()) / Path(f"base_{job_type.lower()}_{scheduler.lower()}.cmd")).read_text()
    if not expected_data:
        assert False, f"Could not find the expected data for {scheduler} and {job_type}"

    # Get the actual default parameters for the scheduler
    if job_type == "DEFAULT":
        actual = Path(f"{generate_cmds.strpath}/t000/tmp/t000_BASE_{scheduler}.cmd").read_text()
    elif job_type == "NODES":
        actual = Path(f"{generate_cmds.strpath}/t000/tmp/t000_NODES_{scheduler}.cmd").read_text()
    else:
        for asthread in Path(f"{generate_cmds.strpath}/t000/tmp").glob(f"*ASThread_WRAP_{job_type}_[0-9]*.cmd"):
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
        if "PJM -o" in line1 or "PJM -e" in line1 or "#SBATCH --output" in line1 or "#SBATCH --error" in line1 or "#SBATCH -J" in line1: # output error will be different
            continue
        elif "##" in line1 or "##" in line2: # comment line
            continue
        elif "header" in line1 or "header" in line2: # header line
            continue
        else:
            assert line1 == line2
