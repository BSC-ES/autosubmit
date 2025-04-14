import cProfile
import os
import pstats
import shutil
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import pytest

from autosubmitconfigparser.config.basicconfig import BasicConfig
from test.regression.utils.common import create_database, init_expid

PROFILE = False  # Enable/disable profiling ( speed up the tests )


def prepare_custom_config_tests(default_yaml_file: Dict[str, Any], project_yaml_files: Dict[str, Dict[str, str]],
                                current_tmpdir: Path) -> Dict[str, Any]:
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


def parse_job_list(lines: List[str]) -> List[SimpleJoblist]:
    """
    Parse a list of lines representing a job list and return a list of root nodes.

    :param lines: List of lines to parse.
    :type lines: List[str]
    :return: List of root nodes.
    :rtype: List[SimpleJoblist]
    """
    roots = []
    stack: List[SimpleJoblist] = []

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


def compare_and_print_differences(node1: Optional[SimpleJoblist], node2: Optional[SimpleJoblist]) -> List[str]:
    """
    Compare two job list nodes and return a list of differences.

    :param node1: The first job list node to compare.
    :type node1: Optional[SimpleJoblist]
    :param node2: The second job list node to compare.
    :type node2: Optional[SimpleJoblist]
    :return: A list of differences between the two nodes.
    :rtype: List[str]
    """
    differences = []
    path = ""
    stack: List[Tuple[Optional[SimpleJoblist], Optional[SimpleJoblist], str]] = [(node1, node2, path)]

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


def remove_noise_from_list(lines: List[str]) -> List[str]:
    """
    Remove noise from a list of lines by stripping whitespace and specific substrings.

    :param lines: List of lines to process.
    :type lines: List[str]
    :return: List of cleaned lines.
    :rtype: List[str]
    """
    lines = [line.strip().rstrip(' [WAITING]').rstrip(' [READY]').strip() for line in lines]
    lines = [line.replace("child", "children") if "child" in line and "children" not in line else line for line in
             lines]
    if lines and lines[0].strip() == '':
        lines = lines[1:]

    return lines


def get_project_root() -> Path:
    """
    Find the autosubmit project root.

    :return: The project root path.
    :rtype: Path
    :raises FileNotFoundError: If the project root directory is not found.
    """
    project_root = Path(__file__).resolve().parent
    while not project_root.joinpath("autosubmit").exists() and project_root.name:
        project_root = project_root.parent
    if not project_root.name:
        raise FileNotFoundError("Could not find the Autosubmit root directory.")
    return project_root


def get_workflow_folder() -> List[str]:
    """
    Get a sorted list of workflow folder names in the 'test/regression/workflows' directory.

    :return: A sorted list of workflow folder names.
    :rtype: List[str]
    """
    workflow_dir = get_project_root() / 'test' / 'regression' / 'workflows'
    # The test will be performed on all the workflows in the 'workflows' directory
    return sorted([f.name for f in Path(workflow_dir).iterdir() if f.is_dir() and "pycache" not in f.name])


@pytest.mark.parametrize("expid", get_workflow_folder())
def test_workflows_dependencies(prepare_workflow_runs: Any, expid: str, current_tmpdir: Path, mocker: Any,
                                prepare_basic_config: Any) -> None:
    """
    Compare current workflow dependencies with the reference ones.

    :param prepare_workflow_runs: Fixture to prepare workflow runs.
    :type prepare_workflow_runs: Any
    :param expid: Experiment ID.
    :type expid: str
    :param current_tmpdir: Temporary directory for the current test.
    :type current_tmpdir: Path
    :param mocker: Mocking object for patching.
    :type mocker: Any
    :param prepare_basic_config: Fixture to prepare basic configuration.
    :type prepare_basic_config: Any
    """
    # Modify this section to add a new test, debugging...
    add_new_test = False  # Enable when adding a new test
    show_workflow_plot = False  # Enable only for debugging purposes
    expids_to_plot = []
    if expid.startswith("auto-monarch-op"):  # Modify only for debugging purposes
        expids_to_plot.append(expid)
    profiler = cProfile.Profile()

    # Running section
    workflow_dir = get_project_root() / 'test' / 'regression' / 'workflows'

    # Allows to have any name for the configuration folder
    mocker.patch.object(BasicConfig, 'read', return_value=True)
    if PROFILE:
        profiler.enable()

    init_expid(os.environ["AUTOSUBMIT_CONFIGURATION"], platform='local', expid=expid, create=True, test_type='test')

    with open(Path(f"{current_tmpdir}/workflows/{expid}/tmp/ASLOGS/jobs_active_status.log"), "r") as new_file:
        new_lines = new_file.readlines()

    if not Path(f"{workflow_dir}/{expid}/ref_workflow.txt").exists():
        if not add_new_test:
            pytest.fail(f"Reference file for {expid} does not exist. Please create it using the following command:\n"
                        f"python <autosubmit_git_root>/test/resources/upload_workflow_config.py <autosubmit_path>/<target_expid>/conf/metadata/experiment_data.yml {workflow_dir}/{expid}/conf")
        print(f"Reference file for {expid} does not exist. Creating a new reference file.")
        with open(Path(f"{workflow_dir}/{expid}/ref_workflow.txt"), "w") as ref_file:
            ref_file.writelines(new_lines)

    with open(Path(f"{workflow_dir}/{expid}/ref_workflow.txt")) as ref_file:
        ref_lines = ref_file.readlines()

    new_lines = remove_noise_from_list(new_lines)
    ref_lines = remove_noise_from_list(ref_lines)
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

    if show_workflow_plot and expid in expids_to_plot:
        init_expid(os.environ["AUTOSUBMIT_CONFIGURATION"], platform='local', expid=expid, create=True, test_type='test',
                   plot=show_workflow_plot)
    if differences:
        pytest.fail("\n".join(differences))

    if PROFILE:
        stats = pstats.Stats(profiler).sort_stats('cumtime')
        stats.print_stats()
