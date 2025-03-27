import pytest
from pathlib import Path
from autosubmitconfigparser.config.basicconfig import BasicConfig
from typing import Dict, Any
import shutil
import cProfile
import pstats
import os
from test.unit.utils.common import create_database, init_expid
import difflib

PROFILE = False  # Enable/disable profiling ( speed up the tests )

def prepare_custom_config_tests(default_yaml_file: Dict[str, Any], project_yaml_files: Dict[str, Dict[str, str]], current_tmpdir: Path) -> Dict[str, Any]:
    """
    Prepare custom configuration tests by creating necessary YAML files.

    :param default_yaml_file: Default YAML file content.
    :type default_yaml_file: Dict[str, Any]
    :param project_yaml_files: Dictionary of project YAML file paths and their content.
    :type project_yaml_files: Dict[str, Dict[str, str]]
    :param current_tmpdir: Temporary folder .
    :type current_tmpdir: Path
    :return: Updated default YAML file content.
    :rtype: Dict[str, Any]
    """
    yaml_file_path = Path(f"{str(current_tmpdir)}/test_exp_data.yml")
    for path, content in project_yaml_files.items():
        test_file_path = Path(f"{str(current_tmpdir)}{path}")
        test_file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(test_file_path, "w") as f:
            f.write(str(content))
    default_yaml_file["job"]["path"] = f"{str(current_tmpdir)}/%NAME%/test.yml"
    with yaml_file_path.open("w") as f:
        f.write(str(default_yaml_file))
    return default_yaml_file


@pytest.fixture()
def prepare_basic_config(current_tmpdir):
    basic_conf = BasicConfig()
    BasicConfig.DB_DIR = (current_tmpdir / "workflows")
    BasicConfig.DB_FILE = "as_times.db"
    BasicConfig.LOCAL_ROOT_DIR = (current_tmpdir / "workflows")
    BasicConfig.LOCAL_TMP_DIR = "tmp"
    BasicConfig.LOCAL_ASLOG_DIR = "ASLOGS"
    BasicConfig.LOCAL_PROJ_DIR = "proj"
    BasicConfig.DEFAULT_PLATFORMS_CONF = ""
    BasicConfig.CUSTOM_PLATFORMS_PATH = ""
    BasicConfig.DEFAULT_JOBS_CONF = ""
    BasicConfig.SMTP_SERVER = ""
    BasicConfig.MAIL_FROM = ""
    BasicConfig.ALLOWED_HOSTS = ""
    BasicConfig.DENIED_HOSTS = ""
    BasicConfig.CONFIG_FILE_FOUND = False
    return basic_conf


@pytest.fixture
def prepare_workflow_runs(current_tmpdir: Path) -> Path:
    """
    factory creating path and directories for test execution
    :param current_tmpdir: mktemp
    :return: LocalPath
    """

    # Write an autosubmitrc file in the temporary directory
    folder = Path(current_tmpdir)
    autosubmitrc = folder.joinpath('autosubmitrc')
    with autosubmitrc.open('w') as f:
        f.write(f'''
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
    os.environ['AUTOSUBMIT_CONFIGURATION'] = str(autosubmitrc)
    create_database(str(autosubmitrc))
    current_script_location = Path(__file__).resolve().parent
    experiments_root = Path(f"{current_script_location}/workflows")
    current_tmpdir_experiments_root = Path(f"{current_tmpdir}/workflows")
    current_tmpdir_experiments_root.parent.mkdir(parents=True, exist_ok=True)
    # copy experiment files
    shutil.copytree(experiments_root, current_tmpdir_experiments_root)
    # create basic structure
    for experiment in current_tmpdir_experiments_root.iterdir():
        if not experiment.is_file():
            experiment.joinpath("proj").mkdir(parents=True, exist_ok=True)
            experiment.joinpath("conf").mkdir(parents=True, exist_ok=True)
            experiment.joinpath("pkl").mkdir(parents=True, exist_ok=True)
            experiment.joinpath("plot").mkdir(parents=True, exist_ok=True)
            experiment.joinpath("status").mkdir(parents=True, exist_ok=True)
            as_tmp = experiment.joinpath("tmp")
            as_tmp.joinpath("ASLOGS").mkdir(parents=True, exist_ok=True)


# By default, the test will be performed on all the workflows in the 'workflows' directory
workflow_folders = [f.name for f in Path('./workflows').iterdir() if f.is_dir() and "pycache" not in f.name]


@pytest.mark.parametrize("expid", workflow_folders)
def test_workflows_dependencies(prepare_workflow_runs, expid, current_tmpdir: Path, mocker, prepare_basic_config: Any) -> None:
    """
    Compare current workflow dependencies with the reference ones.
    """
    profiler = cProfile.Profile()
    # Allows to have any name for the configuration folder
    mocker.patch.object(BasicConfig, 'read', return_value=True)
    if PROFILE:
        profiler.enable()

    init_expid(os.environ["AUTOSUBMIT_CONFIGURATION"], platform='local', expid=expid, create=True, test_type='test')

    with open(Path(f"{current_tmpdir}/workflows/{expid}/ref_workflow.txt")) as ref_file, \
            open(Path(f"{current_tmpdir}/workflows/{expid}/tmp/ASLOGS/jobs_active_status.log"), "r") as new_file:
        ref_lines = ref_file.readlines()
        new_lines = new_file.readlines()
        diff = difflib.unified_diff(ref_lines, new_lines, lineterm='')

    diff_list = list(diff)
    if diff_list:
        print('\n'.join(diff_list))

    assert not diff_list

    if PROFILE:
        stats = pstats.Stats(profiler).sort_stats('cumtime')
        stats.print_stats()
