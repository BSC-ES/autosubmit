#!/usr/bin/env python3
from contextlib import suppress

# Copyright 2017-2020 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.
import locale
import os
from datetime import datetime
from time import mktime
from time import sleep
from time import time
from typing import List, Union
from xml.dom.minidom import parseString

from autosubmit.job.job_common import Status, parse_output_number
from autosubmit.platforms.headers.slurm_header import SlurmHeader
from autosubmit.platforms.slurmplatform import SlurmPlatform
from autosubmit.platforms.wrappers.wrapper_factory import SlurmWrapperFactory
from log.log import AutosubmitCritical, AutosubmitError, Log

class MaestroPlatform(SlurmPlatform):
    """
    Class to manage jobs to host using Maestro framework
    """
    
    def __init__(self, expid: str, name: str, config: dict, auth_password: str=None):
        """
        Initialization of the Class MaestroPlatform

        :param expid: ID of the experiment which will instantiate the MaestroPlatform.
        :type expid: str
        :param name: Name of the platform to be instantiated.
        :type name: str
        :param config: Configuration of the platform, PATHS to Files and DB.
        :type config: dict
        :param auth_password: Authenticator's password.
        :type auth_password: str
        :return: None
        """
        SlurmPlatform.__init__(self, expid, name, config, auth_password = auth_password)
        # other
