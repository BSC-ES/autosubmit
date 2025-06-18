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


from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from autosubmit.config.configcommon import AutosubmitConfig


# TODO: We have one submitter, this can possibly be deleted.
class Submitter:
    """Class to manage the experiments platforms."""

    def load_platforms(self, as_conf: 'AutosubmitConfig', auth_password: Optional[str] = None,
                       local_auth_password=None) -> None:
        """Create all the platforms object that will be used by the experiment.

        :param as_conf: Autosubmit configuration.
        :param auth_password: Password used for authentication.
        :param local_auth_password: Password used for local authorization.
        :return: A set with the platform names.
        """
        raise NotImplementedError  # pragma: no cover
