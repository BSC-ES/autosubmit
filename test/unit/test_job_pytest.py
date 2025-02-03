import os
import pwd
import re
from datetime import datetime, timedelta
import pytest

from autosubmit.job.job import Job
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
    job.update_parameters(as_conf, as_conf.load_parameters())
    return job, as_conf


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
    job,_ = create_job_and_update_parameters(autosubmit_config, experiment_data)
    for key, value in expected_data.items():
        assert job.parameters[key] == value


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
    job, _ = create_job_and_update_parameters(autosubmit_config, experiment_data)
    for attr in attributes_to_check:
        assert hasattr(job, attr)
        assert getattr(job, attr) == attributes_to_check[attr]


@pytest.mark.parametrize('test_packed', [
    False,
    True,
], ids=["Simple job", "Wrapped job"])
def test_adjust_new_parameters(test_packed):
    job = Job('dummy', '1', 0, 1)
    stored_log_path = job._log_path
    job.wallclock = "00:01"
    del job.is_wrapper
    del job.wrapper_name
    del job._wallclock_in_seconds
    del job._log_path
    del job.ready_date
    job.packed = test_packed
    job._adjust_new_parameters()
    assert job.ready_date is None
    assert job.is_wrapper == test_packed
    assert int(job._wallclock_in_seconds) == int(60*1.3)
    if test_packed:
        assert job.wrapper_name == "wrapped"
    else:
        assert job.wrapper_name == "dummy"
    assert job._log_path == stored_log_path


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
    job, as_conf = create_job_and_update_parameters(autosubmit_config, experiment_data, "slurm")
    mocker.patch('autosubmitconfigparser.config.configcommon.AutosubmitConfig.reload')
    template_content, _ = job.update_content(as_conf)
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
    job, as_conf = create_job_and_update_parameters(autosubmit_config, experiment_data)
    del job.start_time
    as_conf.force_load = False
    as_conf.data_changed = False
    job.update_parameters(as_conf, job.parameters)
    assert isinstance(job.start_time, datetime)
