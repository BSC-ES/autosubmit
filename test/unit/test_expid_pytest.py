import pytest
from pathlib import Path
from autosubmit.autosubmit import Autosubmit
import os
import pwd
import sqlite3

from autosubmitconfigparser.config.basicconfig import BasicConfig
from log.log import AutosubmitCritical, AutosubmitError
from test.unit.utils.common import create_database, init_expid


def _get_script_files_path() -> Path:
    return Path(__file__).resolve().parent / 'files'


@pytest.fixture
def create_autosubmit_tmpdir(tmpdir_factory):
    folder = tmpdir_factory.mktemp('autosubmit_tests')
    Path(folder).joinpath('scratch').mkdir()
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
path = {folder}/globallogs

[structures]
path = {folder}/metadata/structures

[historicdb]
path = {folder}/metadata/database

[historiclog]
path = {folder}/metadata/logs

[defaultstats]
path = {folder}

''')
    os.environ['AUTOSUBMIT_CONFIGURATION'] = str(folder.join('autosubmitrc'))
    create_database(str(folder.join('autosubmitrc')))
    Path(folder).joinpath('metadata').mkdir()
    Path(folder).joinpath('metadata/structures').mkdir()
    Path(folder).joinpath('metadata/database').mkdir()
    Path(folder).joinpath('metadata/logs').mkdir()
    assert "tests.db" in [Path(f).name for f in folder.listdir()]
    return folder


@pytest.fixture
def generate_new_experiment(create_autosubmit_tmpdir, request):
    test_type = request.param
    # Setup code that depends on the expid parameter
    expid = init_expid(os.environ["AUTOSUBMIT_CONFIGURATION"], platform='local', expid=None, create=True, test_type=test_type)
    Path(f"{BasicConfig.STRUCTURES_DIR}/structure_{expid}.db").touch()

    yield expid


@pytest.fixture
def setup_experiment_yamlfiles(generate_new_experiment, create_autosubmit_tmpdir):
    expid = generate_new_experiment
    # touch as_misc
    platforms_path = Path(f"{create_autosubmit_tmpdir.strpath}/{expid}/conf/platforms_{expid}.yml")
    jobs_path = Path(f"{create_autosubmit_tmpdir.strpath}/{expid}/conf/jobs_{expid}.yml")
    # Add each platform to test
    with platforms_path.open('w') as f:
        f.write(f"""
PLATFORMS:
    pytest-ps:
        type: ps
        host: 127.0.0.1
        user: {create_autosubmit_tmpdir.owner}
        project: whatever
        scratch_dir: {create_autosubmit_tmpdir}/scratch
        DISABLE_RECOVERY_THREADS: True
        """)
    # add a job of each platform type
    with jobs_path.open('w') as f:
        f.write("""
JOBS:
    debug:
        script: echo "Hello world"
        running: once
EXPERIMENT:
    DATELIST: '20000101'
    MEMBERS: fc0
    CHUNKSIZEUNIT: month
    CHUNKSIZE: '1'
    NUMCHUNKS: '1'
    CHUNKINI: ''
    CALENDAR: standard
  """)

    expid_dir = Path(f"{create_autosubmit_tmpdir.strpath}/scratch/whatever/{create_autosubmit_tmpdir.owner}/{expid}")
    dummy_dir = Path(f"{create_autosubmit_tmpdir.strpath}/scratch/whatever/{create_autosubmit_tmpdir.owner}/{expid}/dummy_dir")
    real_data = Path(f"{create_autosubmit_tmpdir.strpath}/scratch/whatever/{create_autosubmit_tmpdir.owner}/{expid}/real_data")
    # write some dummy data inside scratch dir
    expid_dir.mkdir(parents=True, exist_ok=True)
    dummy_dir.mkdir(parents=True, exist_ok=True)
    real_data.mkdir(parents=True, exist_ok=True)

    with open(dummy_dir.joinpath('dummy_file'), 'w') as f:
        f.write('dummy data')
    real_data.joinpath('dummy_symlink').symlink_to(dummy_dir / 'dummy_file')
    yield expid


@pytest.mark.parametrize("generate_new_experiment", ['test', 'normal', 'operational', 'evaluation'], indirect=True)
def test_expid_generated_correctly(create_autosubmit_tmpdir, generate_new_experiment, setup_experiment_yamlfiles):
    expid = generate_new_experiment
    print(f"Running test for {expid}")
    Autosubmit.inspect(expid=f'{expid}', check_wrapper=True, force=True, lst=None, filter_chunks=None, filter_status=None, filter_section=None)
    assert expid in ['t000', 'a000', 'o000', 'e000']
    assert f"{expid}_DEBUG.cmd" in [Path(f).name for f in Path(f"{create_autosubmit_tmpdir.strpath}/{expid}/tmp").iterdir()]
    # Consult if the expid is in the database
    db_path = Path(f"{create_autosubmit_tmpdir.strpath}/tests.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM experiment WHERE name='{expid}'")
    assert cursor.fetchone() is not None
    cursor.close()


@pytest.mark.parametrize("generate_new_experiment", ['test', 'normal', 'operational', 'evaluation'], indirect=True)
def test_delete_experiment(create_autosubmit_tmpdir, generate_new_experiment, setup_experiment_yamlfiles):
    expid = generate_new_experiment
    print(f"Running test for {expid}")
    Autosubmit.delete(expid=f'{expid}', force=True)
    assert all(expid not in Path(f).name for f in Path(f"{create_autosubmit_tmpdir.strpath}").iterdir())
    assert all(expid not in Path(f).name for f in Path(f"{create_autosubmit_tmpdir.strpath}/metadata/database").iterdir())
    assert all(expid not in Path(f).name for f in Path(f"{create_autosubmit_tmpdir.strpath}/metadata/logs").iterdir())
    assert all(expid not in Path(f).name for f in Path(f"{create_autosubmit_tmpdir.strpath}/metadata/structures").iterdir())
    # Consult if the expid is not in the database
    db_path = Path(f"{create_autosubmit_tmpdir.strpath}/tests.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM experiment WHERE name='{expid}'")
    assert cursor.fetchone() is None
    cursor.close()
    # Test doesn't exist
    with pytest.raises(AutosubmitCritical):
        Autosubmit.delete(expid=f'{expid}', force=True)


@pytest.mark.parametrize("generate_new_experiment", ['test', 'normal', 'operational', 'evaluation'], indirect=True)
def test_delete_experiment_not_owner(create_autosubmit_tmpdir, generate_new_experiment, setup_experiment_yamlfiles, mocker):
    expid = generate_new_experiment
    print(f"Running test for {expid}")
    mocker.patch('autosubmit.autosubmit.Autosubmit._user_yes_no_query', return_value=True)
    with mocker.patch('pwd.getpwuid', side_effect=TypeError):
        _, _, current_owner = Autosubmit._check_ownership(expid)
        assert current_owner is None
    # test not owner not eadmin
    mocker.patch("autosubmit.autosubmit.Autosubmit._check_ownership", return_value=(False, False, create_autosubmit_tmpdir.owner))
    with pytest.raises(AutosubmitCritical):
        Autosubmit.delete(expid=f'{expid}', force=True)
    # test eadmin
    mocker.patch("autosubmit.autosubmit.Autosubmit._check_ownership", return_value=(False, True, create_autosubmit_tmpdir.owner))
    with pytest.raises(AutosubmitCritical):
        Autosubmit.delete(expid=f'{expid}', force=False)
    # test eadmin force
    Autosubmit.delete(expid=f'{expid}', force=True)
    assert all(expid not in Path(f).name for f in Path(f"{create_autosubmit_tmpdir.strpath}").iterdir())
    assert all(expid not in Path(f).name for f in Path(f"{create_autosubmit_tmpdir.strpath}/metadata/database").iterdir())
    assert all(expid not in Path(f).name for f in Path(f"{create_autosubmit_tmpdir.strpath}/metadata/logs").iterdir())
    assert all(expid not in Path(f).name for f in Path(f"{create_autosubmit_tmpdir.strpath}/metadata/structures").iterdir())
    # Consult if the expid is not in the database
    db_path = Path(f"{create_autosubmit_tmpdir.strpath}/tests.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM experiment WHERE name='{expid}'")
    assert cursor.fetchone() is None
    cursor.close()


@pytest.mark.parametrize("generate_new_experiment", ['normal'], indirect=True)
def test_delete_expid(create_autosubmit_tmpdir, generate_new_experiment, setup_experiment_yamlfiles, mocker):
    expid = generate_new_experiment
    mocker.patch("autosubmit.autosubmit.Autosubmit._check_ownership", return_value=(True, True, create_autosubmit_tmpdir.owner))
    mocker.patch('autosubmit.autosubmit.Autosubmit._perform_deletion', return_value="error")
    with pytest.raises(AutosubmitError):
        Autosubmit._delete_expid(expid, force=True)
    mocker.stopall()
    mocker.patch("autosubmit.autosubmit.Autosubmit._check_ownership", return_value=(True, True, create_autosubmit_tmpdir.owner))
    Autosubmit._delete_expid(expid, force=True)
    assert not Autosubmit._delete_expid(expid, force=True)


@pytest.mark.parametrize("generate_new_experiment", ['normal'], indirect=True)
def test_perform_deletion(create_autosubmit_tmpdir, generate_new_experiment, setup_experiment_yamlfiles, mocker):
    expid = generate_new_experiment
    mocker.patch("shutil.rmtree", side_effect=FileNotFoundError)
    mocker.patch("os.remove", side_effect=FileNotFoundError)
    basic_config = BasicConfig()
    basic_config.read()
    experiment_path = Path(f"{basic_config.LOCAL_ROOT_DIR}/{expid}")
    structure_db_path = Path(f"{basic_config.STRUCTURES_DIR}/structure_{expid}.db")
    job_data_db_path = Path(f"{basic_config.JOBDATA_DIR}/job_data_{expid}")
    if all("tmp" not in path for path in [str(experiment_path), str(structure_db_path), str(job_data_db_path)]):
        raise AutosubmitCritical("tmp not in path")
    mocker.patch("autosubmit.autosubmit.delete_experiment", side_effect=FileNotFoundError)
    err_message = Autosubmit._perform_deletion(experiment_path, structure_db_path, job_data_db_path, expid)
    assert all(x in err_message for x in ["Cannot delete experiment entry", "Cannot delete directory", "Cannot delete structure", "Cannot delete job_data"])
