import pytest
from pathlib import Path
from textwrap import dedent

from autosubmit.autosubmit import Autosubmit


@pytest.mark.parametrize('suffix', [
    (''),
    ('/'),
    ('//')
])
def test_configure(mocker, tmp_path, suffix: str) -> None:
    # To update ``Path.home`` appending the provided suffix.
    mocker.patch('pathlib.Path.home').return_value = Path(str(tmp_path) + suffix)

    #asign values that will be passed on cmd
    database_filename = "autosubmit.db"
    db_path = Path.home() / 'database'
    lr_path = Path.home() / 'experiments'

    Autosubmit.configure(
        advanced=False,
        database_path=str(db_path),
        database_filename=database_filename,
        local_root_path=str(lr_path),
        platforms_conf_path=None,  # type: ignore
        jobs_conf_path=None,  # type: ignore
        smtp_hostname=None,  # type: ignore
        mail_from=None,  # type: ignore
        machine=False,
        local=False)

    expected = dedent(f"""\
        [database]
        path = {str(tmp_path)}/database
        filename = autosubmit.db
        
        [local]
        path = {str(tmp_path)}/experiments
        
        [globallogs]
        path = {str(tmp_path)}/experiments/logs
        
        [structures]
        path = {str(tmp_path)}/experiments/metadata/structures
        
        [historicdb]
        path = {str(tmp_path)}/experiments/metadata/data
        
        [historiclog]
        path = {str(tmp_path)}/experiments/metadata/logs
        
        [autosubmitapi]
        url = http://192.168.11.91:8081 # Replace me?
        
        """)

    with open(tmp_path / '.autosubmitrc', 'r') as file:
        assert file.read() == expected
