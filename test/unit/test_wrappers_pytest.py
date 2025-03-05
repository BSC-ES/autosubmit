import pytest

from autosubmit.job.job_common import Status
from autosubmit.job.job_list_persistence import JobListPersistencePkl
from autosubmit.job.job_list import JobList
from autosubmit.platforms.slurmplatform import SlurmPlatform
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from autosubmit.job.job import Job
from autosubmit.job.job_packager import JobPackager
from autosubmit.job.job_packages import JobPackageVertical
from pathlib import Path
import copy

from log.log import AutosubmitCritical


@pytest.fixture
def prepare_basic_config(tmpdir):
    basic_conf = BasicConfig()
    BasicConfig.DB_DIR = (tmpdir / "exp_root")
    BasicConfig.DB_FILE = "debug.db"
    BasicConfig.LOCAL_ROOT_DIR = (tmpdir / "exp_root")
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


@pytest.fixture(scope='function')
def setup(autosubmit_config, tmpdir, prepare_basic_config):
    experiment_id = 'random-id'
    as_conf = autosubmit_config(experiment_id, {})
    as_conf.experiment_data = dict()
    as_conf.experiment_data["JOBS"] = dict()
    as_conf.experiment_data["PLATFORMS"] = dict()
    as_conf.experiment_data["LOCAL_ROOT_DIR"] = tmpdir
    as_conf.experiment_data["LOCAL_TMP_DIR"] = ""
    as_conf.experiment_data["LOCAL_ASLOG_DIR"] = ""
    as_conf.experiment_data["LOCAL_PROJ_DIR"] = ""
    as_conf.experiment_data["WRAPPERS"] = dict()
    as_conf.experiment_data["WRAPPERS"]["WRAPPERS"] = dict()
    as_conf.experiment_data["WRAPPERS"]["WRAPPERS"]["JOBS_IN_WRAPPER"] = "SECTION1"
    as_conf.experiment_data["WRAPPERS"]["WRAPPERS"]["TYPE"] = "vertical"
    Path(tmpdir / experiment_id / "tmp").mkdir(parents=True, exist_ok=True)
    job_list = JobList(experiment_id, as_conf, YAMLParserFactory(),
                       JobListPersistencePkl())

    platform = SlurmPlatform(experiment_id, 'dummy-platform', as_conf.experiment_data)

    job_list._platforms = [platform]
    # add some jobs to the job list
    job = Job("job1", "1", Status.COMPLETED, 0)
    job._init_runtime_parameters()
    job.wallclock = "00:20"
    job.section = "SECTION1"
    job.platform = platform
    job_list._job_list.append(job)
    job = Job("job2", "2", Status.SUBMITTED, 0)
    job._init_runtime_parameters()
    job.wallclock = "00:20"
    job.section = "SECTION1"
    job.platform = platform
    job_list._job_list.append(job)
    wrapper_jobs = copy.deepcopy(job_list.get_job_list())
    for job in wrapper_jobs:
        job.platform = platform
    job_packager = JobPackager(as_conf, platform, job_list)
    vertical_package = JobPackageVertical(wrapper_jobs, configuration=as_conf)
    yield job_packager, vertical_package


@pytest.mark.parametrize("any_simple_packages, not_wrappeable_package_info, built_packages_tmp, expected", [
    (False, ["dummy-1", "dummy-2", "dummy-3"], ["dummy-1", "dummy-2", "dummy-3"], False),
    (True, ["dummy-1", "dummy-2", "dummy-3"], ["dummy-1", "dummy-2", "dummy-3"], False),
    (False, ["dummy-1", "dummy-2", "dummy-3"], ["dummy-1", "dummy-2"], False),
], ids=["no_simple_packages", "simple_packages_exist", "mismatch_in_package_info"])
def test_is_deadlock_jobs_in_queue(setup, any_simple_packages, not_wrappeable_package_info, built_packages_tmp, expected):
    job_packager, _ = setup
    deadlock = job_packager.is_deadlock(any_simple_packages, not_wrappeable_package_info, built_packages_tmp)
    assert deadlock == expected


@pytest.mark.parametrize("any_simple_packages, not_wrappeable_package_info, built_packages_tmp, expected", [
    (False, ["dummy-1", "dummy-2", "dummy-3"], ["dummy-1", "dummy-2", "dummy-3"], True),
    (True, ["dummy-1", "dummy-2", "dummy-3"], ["dummy-1", "dummy-2", "dummy-3"], False),
    (False, ["dummy-1", "dummy-2", "dummy-3"], ["dummy-1", "dummy-2"], False),
], ids=["no_simple_packages", "simple_packages_exist", "mismatch_in_package_info"])
def test_is_deadlock_no_jobs_in_queue(setup, any_simple_packages, not_wrappeable_package_info, built_packages_tmp, expected):
    job_packager, _ = setup
    for job in job_packager._jobs_list._job_list:
        job.status = Status.COMPLETED
    deadlock = job_packager.is_deadlock(any_simple_packages, not_wrappeable_package_info, built_packages_tmp)
    assert deadlock == expected


wrapper_limits = {
    "min": 1,
    "min_h": 1,
    "min_v": 1,
    "max": 99,
    "max_h": 99,
    "max_v": 99,
    "real_min": 2
}


@pytest.mark.parametrize("not_wrappeable_package_info, packages_to_submit, max_jobs_to_submit, expected, unparsed_policy", [
    ([["_", 1, 1, True]], [], 100, 99, "strict"),
    ([["_", 1, 1, False]], [], 100, 99, "mixed"),
    ([["_", 1, 1, True]], [], 100, 99, "flexible"),
    ([["_", 1, 1, True]], [], 100, 99, "strict_one_job"),
    ([["_", 1, 1, True]], [], 100, 99, "mixed_one_job"),
    ([["_", 1, 1, True]], [], 100, 99, "flexible_one_job"),
], ids=["strict_policy", "mixed_policy", "flexible_policy", "strict_one_job", "mixed_one_job", "flexible_one_job"])
def test_process_not_wrappeable_packages_no_more_remaining_jobs(setup, not_wrappeable_package_info, packages_to_submit, max_jobs_to_submit, expected, unparsed_policy):
    job_packager, vertical_package = setup
    if unparsed_policy == "mixed_failed":
        policy = "mixed"
    elif unparsed_policy.endswith("_one_job"):
        policy = unparsed_policy.split("_")[0]
        job_packager._jobs_list._job_list = [job for job in job_packager._jobs_list._job_list if job.name == "job1"]
        vertical_package = JobPackageVertical([vertical_package.jobs[0]], configuration=job_packager._as_config)
    else:
        policy = unparsed_policy
    job_packager._as_config.experiment_data["WRAPPERS"]["WRAPPERS"]["POLICY"] = policy
    job_packager.wrapper_policy = {'WRAPPERS': policy}
    vertical_package.wrapper_policy = policy
    not_wrappeable_package_info[0][0] = vertical_package
    for job in vertical_package.jobs:
        job.status = Status.READY
    result = job_packager.process_not_wrappeable_packages(not_wrappeable_package_info, packages_to_submit, max_jobs_to_submit, wrapper_limits)
    assert result == expected


@pytest.mark.parametrize("not_wrappeable_package_info, packages_to_submit, max_jobs_to_submit, expected, unparsed_policy ", [
    ([["_", 1, 1, True]], [], 100, 100, "strict"),
    ([["_", 1, 1, False]], [], 100, 100, "mixed"),
    ([["_", 1, 1, True]], [], 100, 98, "flexible"),
    ([["_", 1, 1, True]], [], 100, 99, "mixed_failed"),
    ([["_", 1, 1, True]], [], 100, 98, "default"),
    ([["_", 1, 1, True]], [], 100, 100, "strict_one_job"),
    ([["_", 1, 1, True]], [], 100, 100, "mixed_one_job"),
    ([["_", 1, 1, True]], [], 100, 99, "flexible_one_job"),

], ids=["strict_policy", "mixed_policy", "flexible_policy", "mixed_policy_failed_job", "default_policy", "strict_one_job", "mixed_one_job", "flexible_one_job"])
def test_process_not_wrappeable_packages_more_jobs_of_that_section(setup, not_wrappeable_package_info, packages_to_submit, max_jobs_to_submit, expected, unparsed_policy, mocker):
    job_packager, vertical_package = setup
    if unparsed_policy == "mixed_failed":
        policy = "mixed"
    elif unparsed_policy.endswith("_one_job"):
        policy = unparsed_policy.split("_")[0]
        vertical_package = JobPackageVertical([vertical_package.jobs[0]], configuration=job_packager._as_config)
    else:
        policy = unparsed_policy
    if "default" not in unparsed_policy:
        job_packager._as_config.experiment_data["WRAPPERS"]["WRAPPERS"]["POLICY"] = policy
        job_packager.wrapper_policy = {'WRAPPERS': policy}
        vertical_package.wrapper_policy = policy
    not_wrappeable_package_info[0][0] = vertical_package

    for job in vertical_package.jobs:
        job.status = Status.READY
    if unparsed_policy == "mixed_failed":
        vertical_package.jobs[0].fail_count = 1
    job = Job("job3", "3", Status.WAITING, 0)
    job._init_runtime_parameters()
    job.wallclock = "00:20"
    job.section = "SECTION1"
    job.platform = job_packager._platform
    job_packager._jobs_list._job_list.append(job)
    if unparsed_policy in ["flexible", "mixed_failed", "flexible_one_job"]:
        result = job_packager.process_not_wrappeable_packages(not_wrappeable_package_info, packages_to_submit, max_jobs_to_submit, wrapper_limits)
    elif unparsed_policy in ["strict", "mixed", "strict_one_job", "mixed_one_job"]:
        with pytest.raises(AutosubmitCritical):
            job_packager.process_not_wrappeable_packages(not_wrappeable_package_info, packages_to_submit, max_jobs_to_submit, wrapper_limits)
        result = 100
    else:
        result = job_packager.process_not_wrappeable_packages(not_wrappeable_package_info, packages_to_submit, max_jobs_to_submit, wrapper_limits)
    assert result == expected
