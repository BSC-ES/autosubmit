from datetime import datetime, timedelta
import pytest

from autosubmit.job.job import Job
from autosubmit.platforms.psplatform import PsPlatform
from pathlib import Path


@pytest.mark.parametrize('experiment_data, expected_data', [(
    {
        'JOBS': {
            'RANDOM-SECTION': {
                'FILE': "test.sh",
                'PLATFORM': 'DUMMY_PLATFORM',
                'TEST': "%other%"
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
    as_conf = autosubmit_config("test-expid", experiment_data)
    as_conf.experiment_data = as_conf.deep_normalize(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.normalize_variables(as_conf.experiment_data, must_exists=True)
    as_conf.experiment_data = as_conf.deep_read_loops(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.substitute_dynamic_variables(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.parse_data_loops(as_conf.experiment_data)
    # Create some jobs
    job = Job('A', '1', 0, 1)
    platform = PsPlatform(expid='a000', name='DUMMY_PLATFORM', config=as_conf.experiment_data)
    job.section = 'RANDOM-SECTION'
    job.platform = platform
    job.update_parameters(as_conf, {})
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
