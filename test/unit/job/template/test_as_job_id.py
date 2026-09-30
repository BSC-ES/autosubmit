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

"""Cross-language tests for the ``AS_JOB_ID`` runtime variable."""

from collections.abc import Callable
from subprocess import run
from typing import TYPE_CHECKING

import pytest

from autosubmit.job.template import Language, get_template_snippet

from ._helpers import TEMPLATE_LANGUAGES, TemplateLanguage, build_script

if TYPE_CHECKING:
    from pathlib import Path

_SCHEDULER_JOB_ID_ENV_VARS = (
    'SLURM_JOBID',
    'PBS_JOBID',
    'JOB_ID',
    'LSB_JOBID',
    'LOADL_STEP_ID',
    'PJM_JOBID',
)
"""Scheduler variables every template must resolve ``AS_JOB_ID`` from, in priority order."""

LANGUAGE_PARAMS = [
    pytest.param(
        language,
        id=language.name,
        marks=pytest.mark.skipif(language.skip_reason is not None, reason=language.skip_reason or ''),
    )
    for language in TEMPLATE_LANGUAGES
]


@pytest.mark.parametrize('language', LANGUAGE_PARAMS)
def test_as_job_id_in_body(
    tmp_path: 'Path',
    clean_env: Callable[..., dict[str, str]],
    as_job_id_case: tuple[dict[str, str], str],
    language: TemplateLanguage,
) -> None:
    """The body can read the scheduler job id through ``AS_JOB_ID``.

    :param tmp_path: Temporary directory provided by pytest.
    :param clean_env: Factory returning an environment without scheduler variables.
    :param as_job_id_case: Environment overrides and the expected job id.
    :param language: Template language under test.
    """
    env_overrides, expected = as_job_id_case
    script_path = build_script(
        tmp_path, language.module, language.body, language.filename, executable=language.executable
    )
    result = run(
        language.runner(script_path),
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
        env=clean_env(**env_overrides),
    )
    assert result.returncode == 0, result.stderr
    assert f'[INFO] JOBID={expected}' in result.stdout
    assert (tmp_path / 'as_job_id.txt').read_text().strip() == expected


@pytest.mark.parametrize('language', LANGUAGE_PARAMS)
def test_as_job_id_falls_back_to_pid(
    tmp_path: 'Path',
    clean_env: Callable[..., dict[str, str]],
    language: TemplateLanguage,
) -> None:
    """Without scheduler variables, ``AS_JOB_ID`` falls back to the process id.

    :param tmp_path: Temporary directory provided by pytest.
    :param clean_env: Factory returning an environment without scheduler variables.
    :param language: Template language under test.
    """
    script_path = build_script(
        tmp_path, language.module, language.body, language.filename, executable=language.executable
    )
    result = run(
        language.runner(script_path),
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
        env=clean_env(),
    )
    assert result.returncode == 0, result.stderr
    assert (tmp_path / 'as_job_id.txt').read_text().strip().isdigit()


@pytest.mark.parametrize(
    'language',
    [Language.BASH, Language.PYTHON2, Language.PYTHON3, Language.R],
    ids=['bash', 'python2', 'python3', 'r'],
)
def test_template_header_references_all_scheduler_job_id_vars(language: Language) -> None:
    """Every template header must resolve ``AS_JOB_ID`` from all scheduler variables.

    Guards against adding or removing a scheduler variable in only some templates.

    :param language: Template language under test.
    """
    header = get_template_snippet(language).as_header(platform_header='', executable='')
    for var in _SCHEDULER_JOB_ID_ENV_VARS:
        assert var in header
