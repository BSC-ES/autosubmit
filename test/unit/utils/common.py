import os
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmit.autosubmit import Autosubmit
def create_database(envirom):
    os.environ['AUTOSUBMIT_CONFIGURATION'] = envirom
    BasicConfig.read()
    Autosubmit.install()


def init_expid(envirom, platform="local", expid=None, create=True, test_type="normal"):
    os.environ['AUTOSUBMIT_CONFIGURATION'] = envirom
    if not expid:
        if test_type == "normal":
            expid = Autosubmit.expid("pytest", hpc=platform, copy_id='', dummy=True, minimal_configuration=False, git_repo="", git_branch="", git_as_conf="", operational=False,  testcase = False, evaluation = False, use_local_minimal=False)
        elif test_type == "test":
            expid = Autosubmit.expid("pytest", hpc=platform, copy_id='', dummy=True, minimal_configuration=False, git_repo="", git_branch="", git_as_conf="", operational=False,  testcase = True, evaluation = False, use_local_minimal=False)
        elif test_type == "operational":
            expid = Autosubmit.expid("pytest", hpc=platform, copy_id='', dummy=True, minimal_configuration=False, git_repo="", git_branch="", git_as_conf="", operational=True,  testcase = False, evaluation = False, use_local_minimal=False)
        elif test_type == "evaluation":
            expid = Autosubmit.expid("pytest", hpc=platform, copy_id='', dummy=True, minimal_configuration=False, git_repo="", git_branch="", git_as_conf="", operational=False,  testcase = False, evaluation = True, use_local_minimal=False)
    if create:
        Autosubmit.create(expid, True,False, force=True)
    return expid
