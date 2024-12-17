import pytest
from log.log import AutosubmitCritical
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter


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
            "HPCARCH": "PYTEST-UNSUPPORTED",
        },
        "LOCAL_ROOT_DIR": "blabla",
        "LOCAL_TMP_DIR": 'tmp',
        "PLATFORMS": {
            "PYTEST-UNSUPPORTED": {
                "TYPE": "unknown",
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
                "PLATFORM": "PYTEST-UNSUPPORTED",
                "SCRIPT": "echo 'hello world'",
            },
        }
    }
], ids=["Undefined", "Unsupported"])
def test_load_platforms(autosubmit_config, config):
    experiment_id = 'random-id'
    as_conf = autosubmit_config(experiment_id, config)
    submitter = ParamikoSubmitter()
    with pytest.raises(AutosubmitCritical):
        submitter.load_platforms(as_conf)
