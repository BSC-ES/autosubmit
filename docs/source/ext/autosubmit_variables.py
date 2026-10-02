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

"""Autosubmit variables directive."""

import logging

from docutils.parsers.rst import Directive
from docutils.statemachine import StringList
from sphinx import addnodes

from autosubmit.helpers.parameters import PARAMETERS

__version__ = 0.1
logger = logging.getLogger(__name__)


class AutosubmitVariablesDirective(Directive):
    """A custom Sphinx directive that prints Autosubmit variables.

    It is able to recognize variables and separate them in groups,
    producing valid Sphinx documentation directly from the Python
    docstrings.
    """

    has_content = True
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = False

    def run(self):
        rst = [
            '',
            '.. list-table::',
            '   :widths: 25 75',
            '   :header-rows: 1',
            '   ',
            '   * - Variable',
            '     - Description'
        ]

        parameters_group = self.arguments[0].upper()
        if parameters_group not in PARAMETERS:
            logger.error(f'Parameter group {parameters_group} not set')
            return []

        parameters = sorted(PARAMETERS[parameters_group].items())

        for parameter_name, parameter_doc in parameters:
            # rst.append(f'- **{parameter_name.upper()}**: {parameter_doc}')
            rst.extend([f'   * - **{parameter_name.upper()}**', f'     - {parameter_doc}'])

        rst.extend(['', ''])

        node = addnodes.desc()
        self.state.nested_parse(
            StringList(rst),
            self.content_offset,
            node
        )
        return [node]


def setup(app):
    app.add_directive('autosubmit-variables', AutosubmitVariablesDirective)
    return {
        'version': __version__,
        'parallel_read_safe': True
    }
