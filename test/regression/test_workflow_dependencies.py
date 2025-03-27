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
SHOW_WORKFLOW_PLOT = True  # Enable only for debugging purposes

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


class SimpleJoblist:
    def __init__(self, name):
        self.name = name
        self.children = []

    def add_child(self, child):
        self.children.append(child)

    def __str__(self):
        return self.name


def parse_job_list(lines):
    roots = []
    stack = []

    for line in lines:
        indent_level = line.count('|  ')
        line = line.replace('|  ', '').replace('~ ', '').strip()
        name = line.rsplit(' ')[0].strip("\n")

        node = SimpleJoblist(name)

        if indent_level == 0:
            roots.append(node)
        else:
            while len(stack) > indent_level:
                stack.pop()
            stack[-1].add_child(node)

        stack.append(node)

    return sorted(roots, key=lambda x: x.name)


def compare_and_print_differences(node1, node2):
    differences = []
    path = ""
    stack = [(node1, node2, path)]

    while stack:
        n1, n2, current_path = stack.pop()

        if n1 is None and n2 is None:
            continue
        if n1 is None or n2 is None:
            differences.append(f"Difference at {current_path}: One of the nodes is None")
            continue
        if n1.name != n2.name:
            differences.append(f"{current_path}: {n1.name} != {n2.name}")
        if len(n1.children) != len(n2.children):
            differences.append(
                f"{current_path}: Number of children differ ({len(n1.children)} != {len(n2.children)})")

        sorted_children1 = sorted(n1.children, key=lambda x: x.name)
        sorted_children2 = sorted(n2.children, key=lambda x: x.name)

        for child1, child2 in zip(sorted_children1, sorted_children2):
            stack.append((child1, child2, current_path + "/" + n1.name[-10:]))

    return differences


def remove_noise_from_list(lines):
    lines = [line.strip().rstrip(' [WAITING]').rstrip(' [READY]').strip() for line in lines]
    lines = [line.replace("child", "children") if "child" in line and "children" not in line else line for line in lines]
    if lines and lines[0].strip() == '':
        lines = lines[1:]

    return lines


# By default, the test will be performed on all the workflows in the 'workflows' directory
workflow_folders = sorted([f.name for f in Path('./workflows').iterdir() if f.is_dir() and "pycache" not in f.name])


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

    with open(Path(f"{current_tmpdir}/workflows/{expid}/ref_workflow.txt")) as ref_file:
        ref_lines = remove_noise_from_list(ref_file.readlines())

    with open(Path(f"{current_tmpdir}/workflows/{expid}/tmp/ASLOGS/jobs_active_status.log"), "r") as new_file:
        new_lines = remove_noise_from_list(new_file.readlines())

    new_file_nodes = parse_job_list(new_lines[1:])
    ref_file_nodes = parse_job_list(ref_lines[1:])

    differences = []
    if len(new_file_nodes) != len(ref_file_nodes):
        differences.append(f"Number of roots differ: {len(new_file_nodes)} != {len(ref_file_nodes)}")
    if len(new_file_nodes) > len(ref_file_nodes):
        new_file_nodes = new_file_nodes[:len(ref_file_nodes)]
    else:
        ref_file_nodes = ref_file_nodes[:len(new_file_nodes)]

    for new_root, ref_root in zip(new_file_nodes, ref_file_nodes):
        if new_root.name != ref_root.name:
            differences.append(f"Difference at root: {new_root.name} != {ref_root.name}")
        else:
            differences.extend(compare_and_print_differences(new_root, ref_root))

    if differences:
        if SHOW_WORKFLOW_PLOT:
            init_expid(os.environ["AUTOSUBMIT_CONFIGURATION"], platform='local', expid=expid, create=True, test_type='test', plot=SHOW_WORKFLOW_PLOT)
        pytest.fail("\n".join(differences))

    if PROFILE:
        stats = pstats.Stats(profiler).sort_stats('cumtime')
        stats.print_stats()
