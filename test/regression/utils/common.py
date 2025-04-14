# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

import os

from autosubmit.autosubmit import Autosubmit
from autosubmitconfigparser.config.basicconfig import BasicConfig


def create_database(env):
    os.environ['AUTOSUBMIT_CONFIGURATION'] = env
    BasicConfig.read()
    Autosubmit.install()


def init_expid(env, platform="local", expid=None, create=True, test_type="normal", plot=False):
    os.environ['AUTOSUBMIT_CONFIGURATION'] = env
    if not expid:
        if test_type == "normal":
            expid = Autosubmit.expid("pytest", hpc=platform, copy_id='', dummy=True, minimal_configuration=False,
                                     git_repo="", git_branch="", git_as_conf="", operational=False, testcase=False,
                                     evaluation=False, use_local_minimal=False)
        elif test_type == "test":
            expid = Autosubmit.expid("pytest", hpc=platform, copy_id='', dummy=True, minimal_configuration=False,
                                     git_repo="", git_branch="", git_as_conf="", operational=False, testcase=True,
                                     evaluation=False, use_local_minimal=False)
        elif test_type == "operational":
            expid = Autosubmit.expid("pytest", hpc=platform, copy_id='', dummy=True, minimal_configuration=False,
                                     git_repo="", git_branch="", git_as_conf="", operational=True, testcase=False,
                                     evaluation=False, use_local_minimal=False)
        elif test_type == "evaluation":
            expid = Autosubmit.expid("pytest", hpc=platform, copy_id='', dummy=True, minimal_configuration=False,
                                     git_repo="", git_branch="", git_as_conf="", operational=False, testcase=False,
                                     evaluation=True, use_local_minimal=False)
    if create:
        Autosubmit.create(expid, not plot, False, force=True, detail=True)
    return expid
