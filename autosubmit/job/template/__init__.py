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

"""Code related to Autosubmit templates."""

from enum import Enum
from typing import TYPE_CHECKING

from autosubmit.job.template import bash, empty, python2, python3, r

if TYPE_CHECKING:
    from autosubmit.job.template.common import TemplateSnippet


# TODO: Use `StrEnum` when we go Py3.11+
class Language(str, Enum):
    BASH = 'bash'
    PYTHON2 = 'python2'
    PYTHON = 'python'
    PYTHON3 = 'python3'
    R = 'r'
    # TODO: Is empty == wrapper?
    EMPTY = 'empty'

    @staticmethod
    def get_executable(language: 'Language') -> str:
        _EXECUTABLES: dict['Language', str] = {
            Language.BASH: 'bash',
            Language.PYTHON2: 'python2',
            Language.PYTHON: 'python3',
            Language.PYTHON3: 'python3',
            Language.R: 'Rscript',
            Language.EMPTY: 'python3'
        }
        return _EXECUTABLES[language]

    @property
    def checkpoint(self) -> str:
        if self in [Language.PYTHON, Language.PYTHON2, Language.PYTHON3, Language.R]:
            return 'checkpoint()'
        # Bash, empty for wrappers, etc.
        return 'as_checkpoint'


# NOTE: PyCharm may complain about a type error here, but ``mypy`` will
#       run fine (modules can be verified with protocols).
_LANGUAGES_SNIPPETS: dict[Language, 'TemplateSnippet'] = {
    Language.BASH: bash,
    Language.EMPTY: empty,
    Language.PYTHON2: python2,
    Language.PYTHON: python3,
    Language.PYTHON3: python3,
    Language.R: r
}


def get_template_snippet(language: Language) -> 'TemplateSnippet':
    """
    >>> get_template_snippet(Language.PYTHON).__name__
    'autosubmit.job.template.python3'
    >>> get_template_snippet(None)
    Traceback (most recent call last):
    ...
    ValueError: Unknown Autosubmit template language requested: None

    :param language:
    :return:
    """
    if language not in _LANGUAGES_SNIPPETS:
        raise ValueError(f'Unknown Autosubmit template language requested: {language}')
    return _LANGUAGES_SNIPPETS[language]
