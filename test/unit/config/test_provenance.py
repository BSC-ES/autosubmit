from typing import Callable

from autosubmitconfigparser.config.configcommon import AutosubmitConfig


def test_provenance(autosubmit_config: Callable):
    as_conf: AutosubmitConfig = autosubmit_config(expid='a000', experiment_data={
        'GIT': {
            'PROJECT_SUBMODULES': False
        }
    })
    as_conf.reload(force_load=True, only_experiment_data=False)

    # TODO: now assert that we have the provenance for what was loaded?
