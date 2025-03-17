import os
import pwd
import re
from datetime import datetime, timedelta

import pytest
from mock.mock import patch
from autosubmit.job.job import Job
from autosubmit.job.job_utils import get_job_package_code, SubJob, SimpleJob, SubJobManager
from autosubmit.platforms.psplatform import PsPlatform
from pathlib import Path

from autosubmit.platforms.slurmplatform import SlurmPlatform


def create_job_and_update_parameters(autosubmit_config, experiment_data, platform_type="ps"):
    as_conf = autosubmit_config("test-expid", experiment_data)
    as_conf.experiment_data = as_conf.deep_normalize(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.normalize_variables(as_conf.experiment_data, must_exists=True)
    as_conf.experiment_data = as_conf.deep_read_loops(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.substitute_dynamic_variables(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.parse_data_loops(as_conf.experiment_data)
    # Create some jobs
    job = Job('A', '1', 0, 1)
    if platform_type == "ps":
        platform = PsPlatform(expid='test-expid', name='DUMMY_PLATFORM', config=as_conf.experiment_data)
    else:
        platform = SlurmPlatform(expid='test-expid', name='DUMMY_PLATFORM', config=as_conf.experiment_data)
    job.section = 'RANDOM-SECTION'
    job.platform = platform
    parameters = job.update_parameters(as_conf, set_attributes=True)
    return job, as_conf, parameters


@pytest.mark.parametrize('experiment_data, expected_data', [(
    {
        'JOBS': {
            'RANDOM-SECTION': {
                'FILE': "test.sh",
                'PLATFORM': 'DUMMY_PLATFORM',
                'TEST': "%other%",
            },
        },
        'PLATFORMS': {
            'dummy_platform': {
                'type': 'ps',
                'whatever': 'dummy_value',
                'whatever2': 'dummy_value2',
                'CUSTOM_DIRECTIVES': ['$SBATCH directive1', '$SBATCH directive2'],
            },
        },
        'OTHER': "%CURRENT_WHATEVER%/%CURRENT_WHATEVER2%",
        'ROOTDIR': 'dummy_rootdir',
        'LOCAL_TMP_DIR': 'dummy_tmpdir',
        'LOCAL_ROOT_DIR': 'dummy_rootdir',
    },
    {
        'CURRENT_FILE': "test.sh",
        'CURRENT_PLATFORM': 'DUMMY_PLATFORM',
        'CURRENT_WHATEVER': 'dummy_value',
        'CURRENT_WHATEVER2': 'dummy_value2',
        'CURRENT_TEST': 'dummy_value/dummy_value2',

    }
)])
def test_update_parameters_current_variables(autosubmit_config, experiment_data, expected_data):
    _,_, parameters = create_job_and_update_parameters(autosubmit_config, experiment_data)
    for key, value in expected_data.items():
        assert parameters[key] == value


@pytest.mark.parametrize('test_with_file, file_is_empty, last_line_empty', [
    (False, False, False),
    (True, True, False),
    (True, False, False),
    (True, False, True)
], ids=["no file", "file is empty", "file is correct", "file last line is empty"])
def test_recover_last_ready_date(tmpdir, test_with_file, file_is_empty, last_line_empty):
    job = Job('dummy', '1', 0, 1)
    job._tmp_path = Path(tmpdir)
    stat_file = job._tmp_path.joinpath(f'{job.name}_TOTAL_STATS')
    ready_time = datetime.now() + timedelta(minutes=5)
    ready_date = int(ready_time.strftime("%Y%m%d%H%M%S"))
    expected_date = None
    if test_with_file:
        if file_is_empty:
            stat_file.touch()
            expected_date = datetime.fromtimestamp(stat_file.stat().st_mtime).strftime('%Y%m%d%H%M%S')
        else:
            if last_line_empty:
                with stat_file.open('w') as f:
                    f.write(" ")
                expected_date = datetime.fromtimestamp(stat_file.stat().st_mtime).strftime('%Y%m%d%H%M%S')
            else:
                with stat_file.open('w') as f:
                    f.write(f"{ready_date} {ready_date} {ready_date} COMPLETED")
                expected_date = str(ready_date)
    job.ready_date = None
    job.recover_last_ready_date()
    assert job.ready_date == expected_date


@pytest.mark.parametrize('test_with_logfiles, file_timestamp_greater_than_ready_date', [
    (False, False),
    (True, True),
    (True, False),
], ids=["no file", "log timestamp >= ready_date", "log timestamp < ready_date"])
def test_recover_last_log_name(tmpdir, test_with_logfiles, file_timestamp_greater_than_ready_date):
    job = Job('dummy', '1', 0, 1)
    job._log_path = Path(tmpdir)
    expected_local_logs = (f"{job.name}.out.0", f"{job.name}.err.0")
    if test_with_logfiles:
        if file_timestamp_greater_than_ready_date:
            ready_time = datetime.now() - timedelta(minutes=5)
            job.ready_date = str(ready_time.strftime("%Y%m%d%H%M%S"))
            log_name = job._log_path.joinpath(f'{job.name}_{job.ready_date}')
            expected_update_log = True
            expected_local_logs = (log_name.with_suffix('.out').name, log_name.with_suffix('.err').name)
        else:
            expected_update_log = False
            ready_time = datetime.now() + timedelta(minutes=5)
            job.ready_date = str(ready_time.strftime("%Y%m%d%H%M%S"))
            log_name = job._log_path.joinpath(f'{job.name}_{job.ready_date}')
        log_name.with_suffix('.out').touch()
        log_name.with_suffix('.err').touch()
    else:
        expected_update_log = False

    job.updated_log = False
    job.recover_last_log_name()
    assert job.updated_log == expected_update_log
    assert job.local_logs[0] == str(expected_local_logs[0])
    assert job.local_logs[1] == str(expected_local_logs[1])


@pytest.mark.parametrize('experiment_data, attributes_to_check', [(
    {
        'JOBS': {
            'RANDOM-SECTION': {
                'FILE': "test.sh",
                'PLATFORM': 'DUMMY_PLATFORM',
                'NOTIFY_ON': 'COMPLETED',
            },
        },
        'PLATFORMS': {
            'dummy_platform': {
                'type': 'ps',
            },
        },
        'ROOTDIR': 'dummy_rootdir',
        'LOCAL_TMP_DIR': 'dummy_tmpdir',
        'LOCAL_ROOT_DIR': 'dummy_rootdir',
    },
    {'notify_on': ['COMPLETED']}
)])
def test_update_parameters_attributes(autosubmit_config, experiment_data, attributes_to_check):
    job, _, _ = create_job_and_update_parameters(autosubmit_config, experiment_data)
    for attr in attributes_to_check:
        assert hasattr(job, attr)
        assert getattr(job, attr) == attributes_to_check[attr]

@pytest.mark.parametrize('custom_directives, test_type, result_by_lines', [
    ("test_str a", "platform", ["test_str a"]),
    (['test_list', 'test_list2'], "platform", ['test_list', 'test_list2']),
    (['test_list', 'test_list2'], "job", ['test_list', 'test_list2']),
    ("test_str", "job", ["test_str"]),
    (['test_list', 'test_list2'], "both", ['test_list', 'test_list2']),
    ("test_str", "both", ["test_str"]),
    (['test_list', 'test_list2'], "current_directive", ['test_list', 'test_list2']),
    ("['test_str_list', 'test_str_list2']", "job", ['test_str_list', 'test_str_list2']),
], ids=["Test str - platform", "test_list - platform", "test_list - job", "test_str - job", "test_list - both", "test_str - both", "test_list - job - current_directive", "test_str_list - current_directive"])
def test_custom_directives(tmpdir, custom_directives, test_type, result_by_lines, mocker, autosubmit_config):
    file_stat = os.stat(f"{tmpdir.strpath}")
    file_owner_id = file_stat.st_uid
    tmpdir.owner = pwd.getpwuid(file_owner_id).pw_name
    tmpdir_path = Path(tmpdir.strpath)
    project = "whatever"
    user = tmpdir.owner
    scratch_dir = f"{tmpdir.strpath}/scratch"
    full_path = f"{scratch_dir}/{project}/{user}"
    experiment_data = {
        'JOBS': {
            'RANDOM-SECTION': {
                'SCRIPT': "echo 'Hello World!'",
                'PLATFORM': 'DUMMY_PLATFORM',
            },
        },
        'PLATFORMS': {
            'dummy_platform': {
                "type": "slurm",
                "host": "127.0.0.1",
                "user": f"{user}",
                "project": f"{project}",
                "scratch_dir": f"{scratch_dir}",
                "QUEUE": "gp_debug",
                "ADD_PROJECT_TO_HOST": False,
                "MAX_WALLCLOCK": "48:00",
                "TEMP_DIR": "",
                "MAX_PROCESSORS": 99999,
                "PROCESSORS_PER_NODE": 123,
                "DISABLE_RECOVERY_THREADS": True
            },
        },
        'ROOTDIR': f"{full_path}",
        'LOCAL_TMP_DIR': f"{full_path}",
        'LOCAL_ROOT_DIR': f"{full_path}",
        'LOCAL_ASLOG_DIR': f"{full_path}",
    }
    tmpdir_path.joinpath(f"{scratch_dir}/{project}/{user}").mkdir(parents=True)
    tmpdir_path.joinpath("test-expid").mkdir(parents=True)
    tmpdir_path.joinpath("test-expid/tmp/LOG_test-expid").mkdir(parents=True)

    if test_type == "platform":
        experiment_data['PLATFORMS']['dummy_platform']['CUSTOM_DIRECTIVES'] = custom_directives
    elif test_type == "job":
        experiment_data['JOBS']['RANDOM-SECTION']['CUSTOM_DIRECTIVES'] = custom_directives
    elif test_type == "both":
        experiment_data['PLATFORMS']['dummy_platform']['CUSTOM_DIRECTIVES'] = custom_directives
        experiment_data['JOBS']['RANDOM-SECTION']['CUSTOM_DIRECTIVES'] = custom_directives
    elif test_type == "current_directive":
        experiment_data['PLATFORMS']['dummy_platform']['APP_CUSTOM_DIRECTIVES'] = custom_directives
        experiment_data['JOBS']['RANDOM-SECTION']['CUSTOM_DIRECTIVES'] = "%CURRENT_APP_CUSTOM_DIRECTIVES%"
    job, as_conf, parameters = create_job_and_update_parameters(autosubmit_config, experiment_data, "slurm")
    mocker.patch('autosubmitconfigparser.config.configcommon.AutosubmitConfig.reload')
    template_content, _ = job.update_content(as_conf, parameters)
    for directive in result_by_lines:
        pattern = r'^\s*' + re.escape(directive) + r'\s*$' # Match Start line, match directive, match end line
        assert re.search(pattern, template_content, re.MULTILINE) is not None


@pytest.mark.parametrize('experiment_data', [(
    {
        'JOBS': {
            'RANDOM-SECTION': {
                'FILE': "test.sh",
                'PLATFORM': 'DUMMY_PLATFORM',
                'TEST': "rng",
            },
        },
        'PLATFORMS': {
            'dummy_platform': {
                'type': 'ps',
                'whatever': 'dummy_value',
                'whatever2': 'dummy_value2',
                'CUSTOM_DIRECTIVES': ['$SBATCH directive1', '$SBATCH directive2'],
            },
        },
        'ROOTDIR': "asd",
        'LOCAL_TMP_DIR': "asd",
        'LOCAL_ROOT_DIR': "asd",
        'LOCAL_ASLOG_DIR': "asd",
    }
)], ids=["Simple job"])
def test_no_start_time(autosubmit_config, experiment_data):
    job, as_conf, parameters = create_job_and_update_parameters(autosubmit_config, experiment_data)
    del job.start_time
    as_conf.force_load = False
    as_conf.data_changed = False
    job.update_parameters(as_conf, set_attributes=True)
    assert isinstance(job.start_time, datetime)


def test_get_job_package_code(autosubmit_config):

    autosubmit_config('dummy', {})
    experiment_id = 'dummy'
    job = Job(experiment_id, '1', 0, 1)


    with patch ("autosubmit.job.job_utils.JobPackagePersistence") as mock_persistence:
        mock_persistence.return_value.load.return_value = [
            ['dummy', '0005_job_packages', 'dummy']
        ]
        code = get_job_package_code(job.expid ,job.name)

        assert code == 5


def test_simple_job_instantiation(tmp_path, autosubmit_config):
    job = SimpleJob("dummy", tmp_path, 100)

    assert job.name == "dummy"
    assert job._tmp_path == tmp_path
    assert job.status == 100


def test_sub_job_instantiation(tmp_path, autosubmit_config):
    job = SubJob("dummy",package=None,queue=0,run=0, total=0, status="UNKNOWN")

    assert job.name == "dummy"
    assert job.package is None
    assert job.queue == 0
    assert job.run == 0
    assert job.total == 0
    assert job.status == "UNKNOWN"


@pytest.mark.parametrize("current_structure",
                             [
                                 ({
                                         'dummy2':
                                             {'dummy','dummy1','dummy4'},
                                         'dummy3':
                                             'dummy'
                                     }),
                                 ({}),
                             ],
                         ids=["Current structure of the Job Manager with multiple values",
                              "Current structure of the Job Manager without values"]
                         )
def test_sub_job_manager(current_structure):
    """
    tester of the function _sub_job_manager
    """
    jobs = {
        SubJob("dummy",package="test2",queue=0, run=1, total=30, status="UNKNOWN"),
        SubJob("dummy",package=["test4","test1","test2","test3"],queue=1,
               run=2, total=10, status="UNKNOWN"),
        SubJob("dummy2",package="test2",queue=2, run=3, total=100, status="UNKNOWN"),
        SubJob("dummy",package="test3",queue=3, run=4, total=1000, status="UNKNOWN"),
    }

    job_to_package = {
        'dummy test'
    }

    package_to_job = {
        'test':
            {'dummy', 'dummy2'},
        'test2':
            {'dummy', 'dummy2'},
        'test3':
            {'dummy', 'dummy2'}
    }

    job_manager = SubJobManager(jobs, job_to_package, package_to_job, current_structure)
    job_manager.process_index()
    job_manager.process_times()

    print(type(job_manager.get_subjoblist()))

    assert job_manager is not None and type(job_manager) is SubJobManager
    assert job_manager.get_subjoblist() is not None and type(job_manager.get_subjoblist()) is set
    assert job_manager.subjobindex is not None and type(job_manager.subjobindex) is dict
    assert job_manager.subjobfixes is not None and type(job_manager.subjobfixes) is dict
    assert (job_manager.get_collection_of_fixes_applied() is not None
            and type(job_manager.get_collection_of_fixes_applied()) is dict)


def test_update_parameters_reset_logs(autosubmit_config, tmpdir):
    # TODO This experiment_data (aside from WORKFLOW_COMMIT and maybe JOBS) could be a good candidate for a fixture in the conf_test. "basic functional configuration"
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data={
            'AUTOSUBMIT': {'WORKFLOW_COMMIT': 'dummy'},
            'PLATFORMS': {'DUMMY_P': {'TYPE': 'ps'}},
            'JOBS': {'DUMMY_S': {'FILE': 'dummy.sh', 'PLATFORM': 'DUMMY_P'}},
            'DEFAULT': {'HPCARCH': 'DUMMY_P'},
        }
    )
    job = Job('DUMMY', '1', 0, 1)
    job.section = 'DUMMY_S'
    job.log_recovered = True
    job.packed_during_building = True
    job.workflow_commit = "incorrect"
    job.update_parameters(as_conf, set_attributes=True, reset_logs=True)
    assert job.workflow_commit == "dummy"
