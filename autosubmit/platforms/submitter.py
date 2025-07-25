#!/usr/bin/env python3

# Copyright 2014 Climate Forecasting Unit, IC3

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
# along with Autosubmit.  If not, see <http: www.gnu.org / licenses / >.


from autosubmit.config.configcommon import AutosubmitConfig


class Submitter:
    """
    Class to manage the experiments platform
    """
    def load_platforms(self, asconf, retries=5, auth_password=None):
        """
        Create all the platforms object that will be used by the experiment

        :param retries: retries in case creation of service fails
        :param asconf: autosubmit config to use
        :type asconf: AutosubmitConfig
        :param auth_password: password to use for authentication
        :type auth_password: str
        :return: platforms used by the experiment
        :rtype: dict
        """

        raise Exception('Method Not Implemented')
