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

"""A registry for Autosubmit experiment configurations.

It keeps a list of ``AutosubmitConfig`` objects. It is supposed to be
populated during the Autosubmit program initialisation. Ideally, it
should contain only the configuration objects for the experiments used
by the program.

For example, if you delete three experiments and Autosubmit ``delete``
subcommand code needs the objects, it is expected to contain the three
configuration objects. If you run ``autosubmit run``, the registry
is expected to have a single configuration object.

If you use this registry during your tests, remember to remove the
configuration objects once they are not needed any more.
"""

from autosubmit.config.configcommon import AutosubmitConfig

_REGISTRY: dict[str, "AutosubmitConfig"] = {}
"""The dictionary that backs the Autosubmit configuration objects registry."""


def load_config(expid: str) -> "AutosubmitConfig":
    """Load the configuration object.

    If the configuration object exists in the registry, the existing object
    is returned.

    Otherwise, a new configuration object is created and returned.

    :param expid: The experiment ID.
    :return: The Autosubmit configuration object.
    """
    if expid in _REGISTRY:
        return _REGISTRY[expid]

    as_conf = AutosubmitConfig(expid)
    _REGISTRY[expid] = as_conf

    return as_conf


def clear_registry() -> None:
    """Empty the registry."""
    _REGISTRY.clear()
