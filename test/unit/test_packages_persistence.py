import pytest

from autosubmit.autosubmit import Autosubmit
from autosubmit.job.job_package_persistence import JobPackagePersistence
from log.log import AutosubmitCritical


def test_load(mocker):
    """
    Loads package of jobs from a database
    :param: wrapper: boolean
    :return: list of jobs per package
    """
    mocker.patch('autosubmit.database.db_manager.DbManager.select_all').return_value = [['random-id"', 'vertical-wrapper', 'dummy-job', '02:00']]
    mocker.patch('sqlite3.connect').return_value = mocker.MagicMock()
    job_package_persistence = JobPackagePersistence('dummy/path', 'dummy/file')
    assert job_package_persistence.load(wrapper=True) == [['random-id"', 'vertical-wrapper', 'dummy-job', '02:00']]
    mocker.patch('autosubmit.database.db_manager.DbManager.select_all').return_value = [['random-id"', 'vertical-wrapper', 'dummy-job']]
    with pytest.raises(AutosubmitCritical):
        job_package_persistence.load(wrapper=True)
