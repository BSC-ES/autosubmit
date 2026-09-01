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
