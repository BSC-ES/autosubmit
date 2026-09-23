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

"""Shared helpers for Autosubmit job template unit tests."""

import shutil
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent

from autosubmit.job.template import bash, python3, r
from autosubmit.job.template.common import TemplateSnippet

_RSCRIPT = shutil.which('Rscript')


def build_script(
    tmp_path: Path,
    module: TemplateSnippet,
    body: str,
    filename: str,
    executable: str | None = None,
    job_name: str = 't000_test',
    fail_count: str = '0',
) -> Path:
    """Assemble and write a runnable script, returning its path.

    :param tmp_path: Temporary directory provided by pytest.
    :param module: Template module exposing the ``as_*`` functions.
    :param body: User script body.
    :param filename: Name of the generated script file.
    :param executable: Interpreter used in the shebang.
    :param job_name: Value for the ``%JOBNAME%`` placeholder.
    :param fail_count: Value for the ``%FAIL_COUNT%`` placeholder.
    """
    script = '\n'.join([
        module.as_header(platform_header='', executable=executable or ''),
        module.as_body(dedent(body)),
        module.as_tailer(),
    ])
    script = script.replace('%EXTENDED_HEADER%', '')
    script = script.replace('%EXTENDED_TAILER%', '')
    script = script.replace('%CURRENT_LOGDIR%', str(tmp_path))
    script = script.replace('%JOBNAME%', job_name)
    script = script.replace('%FAIL_COUNT%', fail_count)

    script_path = tmp_path / filename
    script_path.write_text(script)
    script_path.chmod(0o755)
    return script_path


@dataclass(frozen=True)
class TemplateLanguage:
    """A job template language and the pieces needed to run a script for it.

    :param name: Language name used in test IDs.
    :param module: Template module exposing the ``as_*`` functions.
    :param filename: Name of the generated script file.
    :param executable: Interpreter used in the shebang.
    :param runner: Callable that builds the command to run the generated script.
    :param body: Script body that writes ``AS_JOB_ID`` to ``as_job_id.txt``.
    :param skip_reason: If set, tests for this language are skipped with this reason.
    """

    name: str
    module: TemplateSnippet
    filename: str
    executable: str | None
    runner: Callable[[Path], list[str]]
    body: str
    skip_reason: str | None = None


TEMPLATE_LANGUAGES: list[TemplateLanguage] = [
    TemplateLanguage(
        name='bash',
        module=bash,
        filename='the_script.sh',
        executable='/bin/bash',
        runner=lambda script_path: [str(script_path)],
        body='echo "$AS_JOB_ID" > as_job_id.txt',
    ),
    TemplateLanguage(
        name='python3',
        module=python3,
        filename='the_script.py',
        executable=sys.executable,
        runner=lambda script_path: [sys.executable, str(script_path)],
        body="open('as_job_id.txt', 'w').write(AS_JOB_ID)",
    ),
    TemplateLanguage(
        name='r',
        module=r,
        filename='the_script.R',
        executable=_RSCRIPT,
        runner=lambda script_path: [_RSCRIPT or 'Rscript', str(script_path)],
        body="writeLines(AS_JOB_ID, 'as_job_id.txt')",
        skip_reason=None if _RSCRIPT else 'Rscript not found on PATH',
    ),
]
"""Supported template languages for cross-language unit tests."""
