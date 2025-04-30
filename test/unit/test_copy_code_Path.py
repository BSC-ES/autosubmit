import pytest
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Callable, List
from tempfile import TemporaryDirectory

from autosubmit.autosubmit import Autosubmit
from autosubmit.platforms.slurmplatform import SlurmPlatform, ParamikoPlatform
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory

from test.unit.conftest import AutosubmitExperiment, autosubmit_exp

@pytest.fixture
def fake_description():
    return "test descript"


## define project:project_type: 'local'

    # to check project:project_destination
    # to check local:project_path
        #empty project_path
        #local_project_path not valid && empty

        #project_path exist 
            #local_destination exist
            #local_destination no exist 
                #check "cp -R " + local_project_path + "/* " + local_destination
                #check no possible to cp 
        #project_path not exist
            #check mkdirs project_path & local_destination (single folder + parent folder)

    #check what happens when any dir already exist on the cp functions
