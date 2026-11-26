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

"""Cross-language tests for the ``as_checkpoint`` template helper."""

from subprocess import run
from typing import TYPE_CHECKING

import pytest

from autosubmit.job.template import Language

from ._helpers import TEMPLATE_LANGUAGES, TemplateLanguage, build_script

if TYPE_CHECKING:
    from pathlib import Path

_JOB_NAME = 't000_test'
_ATTEMPT = '0'
_CHECKPOINT = f'{_JOB_NAME}_CHECKPOINT_{_ATTEMPT}'

LANGUAGE_PARAMS = [
    pytest.param(
        language,
        id=language.name,
        marks=pytest.mark.skipif(language.skip_reason is not None, reason=language.skip_reason or ''),
    )
    for language in TEMPLATE_LANGUAGES
]


def _checkpoint_body(language: TemplateLanguage, calls: int) -> str:
    """Return a body that calls ``as_checkpoint`` the given number of times.

    :param language: Template language under test.
    :param calls: Number of checkpoint calls.
    :return: Script body.
    """
    statement = 'as_checkpoint' if language.language is Language.BASH else 'as_checkpoint()'
    return '\n'.join([statement] * calls)


@pytest.mark.parametrize('language', LANGUAGE_PARAMS)
def test_checkpoint_file_created(tmp_path: 'Path', language: TemplateLanguage) -> None:
    """Calling ``as_checkpoint`` creates a ``_CHECKPOINT_<attempt>_N`` file."""
    script_path = build_script(
        tmp_path,
        language.module,
        _checkpoint_body(language, 1),
        language.filename,
        executable=language.executable,
    )

    result = run(language.runner(script_path), capture_output=True, text=True, cwd=str(tmp_path))

    assert result.returncode == 0, result.stderr
    assert (tmp_path / f'{_CHECKPOINT}_1').exists()


@pytest.mark.parametrize('language', LANGUAGE_PARAMS)
def test_multiple_checkpoints_create_sequential_files(tmp_path: 'Path', language: TemplateLanguage) -> None:
    """Each ``as_checkpoint`` call creates a sequentially numbered file."""
    script_path = build_script(
        tmp_path,
        language.module,
        _checkpoint_body(language, 3),
        language.filename,
        executable=language.executable,
    )

    result = run(language.runner(script_path), capture_output=True, text=True, cwd=str(tmp_path))

    assert result.returncode == 0, result.stderr
    assert (tmp_path / f'{_CHECKPOINT}_1').exists()
    assert (tmp_path / f'{_CHECKPOINT}_2').exists()
    assert (tmp_path / f'{_CHECKPOINT}_3').exists()
