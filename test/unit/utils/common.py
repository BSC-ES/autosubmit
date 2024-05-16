import os
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmit.autosubmit import Autosubmit
def create_database(envirom):
    os.environ['AUTOSUBMIT_CONFIGURATION'] = envirom
    BasicConfig.read()
    Autosubmit.install()

def generate_expid(envirom, platform="local"):
    os.environ['AUTOSUBMIT_CONFIGURATION'] = envirom
    expid = Autosubmit.expid("pytest", hpc=platform, copy_id='', dummy=True, minimal_configuration=False, git_repo="", git_branch="", git_as_conf="", operational=False,  testcase = True, use_local_minimal=False)
    Autosubmit.create(expid, True,False, force=True)
    return expid

