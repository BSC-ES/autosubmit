import pytest

from autosubmit.autosubmit import Autosubmit


@pytest.mark.parametrize("config", [
    {
        "DEFAULT": {
            "HPCARCH": "PYTEST-UNDEFINED",
        },
        "LOCAL_ROOT_DIR": "blabla",
        "LOCAL_TMP_DIR": 'tmp',
        "PLATFORMS": {
            "PYTEST-UNDEFINED": {
                "host": "",
                "user": "",
                "project": "",
                "scratch_dir": "",
                "MAX_WALLCLOCK": "",
                "DISABLE_RECOVERY_THREADS": True
            }
        },
        "JOBS": {
            "job1": {
                "PLATFORM": "PYTEST-UNDEFINED",
                "SCRIPT": "echo 'hello world'",
            },
        }
    },
    {
        "DEFAULT": {
            "HPCARCH": "PYTEST-PS",
        },
        "LOCAL_ROOT_DIR": "blabla",
        "LOCAL_TMP_DIR": 'tmp',
        "PLATFORMS": {
            "PYTEST-PS": {
                "TYPE": "ps",
                "host": "",
                "user": "",
                "project": "",
                "scratch_dir": "",
                "MAX_WALLCLOCK": "",
                "DISABLE_RECOVERY_THREADS": True
            }
        },
        "JOBS": {
            "job1": {
                "PLATFORM": "PYTEST-PS",
                "SCRIPT": "echo 'hello world'",
            },
        }
    }], ids=["Git clone without type defined", "Git clone with the correct type defined"])
def test_copy_code(autosubmit_config, config, mocker):
    expid = 'random-id'
    as_conf = autosubmit_config(expid, config)
    mocker.patch('autosubmit.git.autosubmit_git.AutosubmitGit.clone_repository', return_value=True)
    assert Autosubmit._copy_code(as_conf, expid, "git", True)
