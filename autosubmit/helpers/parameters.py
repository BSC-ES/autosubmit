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

import functools
import inspect
from collections import defaultdict
from typing import Any

PARAMETERS: dict[str, Any] = defaultdict(defaultdict)
"""Global default dictionary holding a multi-level dictionary with the Autosubmit
parameters. At the first level we have the parameter groups.

  - ``JOB``

  - ``PLATFORM``
  
  - ``PROJECT``
  
Each entry in the ``PARAMETERS`` dictionary holds another default dictionary. Finally,
the lower level in the dictionary has a ``key=value`` where ``key`` is the parameter
name, and ``value`` the parameter documentation.

These values are used to create the Sphinx documentation for variables, as well as
to populate the comments in the Autosubmit YAML configuration files.
"""


def autosubmit_parameter(func=None, *, name, group: str | None = None):
    """Decorator for Autosubmit configuration parameters.

    Used to annotate properties of classes

    :param func: wrapped function. Always ``None`` due to how we call the decorator.
    :param name: parameter name.
    :param group: group name. Default to caller module name.
    """
    if group is None:
        stack = inspect.stack()
        group = stack[1][0].f_locals['__qualname__'].rsplit('.', 1)[-1]

    group = group.upper()

    if group not in PARAMETERS:
        PARAMETERS[group] = defaultdict(defaultdict)

    names = name
    if type(name) is not list:
        names = [name]

    for parameter_name in names:
        if parameter_name not in PARAMETERS[group]:
            PARAMETERS[group][parameter_name] = None

    def parameter_decorator(wrapped_func):
        parameter_group = getattr(parameter_decorator, "__group")
        parameter_names = getattr(parameter_decorator, "__names")
        for p_name in parameter_names:
            if wrapped_func.__doc__:
                PARAMETERS[parameter_group][p_name] = wrapped_func.__doc__.strip().split('\n')[0]

        # Delete the members created as we are not using them hereafter
        delattr(parameter_decorator, "__group")
        delattr(parameter_decorator, "__names")

        @functools.wraps(wrapped_func)
        def wrapper(*args, **kwargs):
            return wrapped_func(*args, **kwargs)

        return wrapper

    setattr(parameter_decorator, "__group", group)
    setattr(parameter_decorator, "__names", names)

    return parameter_decorator
