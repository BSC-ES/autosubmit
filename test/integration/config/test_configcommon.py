# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.database.db_common import update_experiment_description_version


def test_load_parameters_default_description(autosubmit_exp):
    """Verifies the ``DEFAULT.DESCRIPTION`` works with ``load_parameters``."""
    exp = autosubmit_exp(experiment_data={})

    as_conf = AutosubmitConfig(exp.expid)
    as_conf.load_parameters()

    assert as_conf.experiment_data['DEFAULT']['DESCRIPTION'].startswith('Pytest experiment')


def test_load_parameters_default_description_invalid_expid(autosubmit_exp):
    """Verifies the ``DEFAULT.DESCRIPTION`` is empty when the experiment ID is invalid."""
    exp = autosubmit_exp(experiment_data={})

    as_conf = AutosubmitConfig(exp.expid)
    # Empty the description to pretend we have an experiment without description.
    # This shouldn't happen; but who knows.
    update_experiment_description_version(exp.expid, '')

    as_conf.load_parameters()

    assert not as_conf.experiment_data['DEFAULT']['DESCRIPTION']
