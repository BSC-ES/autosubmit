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

"""Autosubmit template scripts written with Bash, used by wrappers.

NOTE: This language snippet does NOT create STAT or COMPLETED files.
"""

_DEFAULT_EXECUTABLE = "/bin/bash"


def as_header(platform_header: str, executable: str) -> str:
    """
    >>> as_header(platform_header="# This is a test", executable="/bin/bash")
    '#!/bin/bash\\n# This is a test'

    :param platform_header:
    :param executable:
    :return:
    """
    executable = executable or _DEFAULT_EXECUTABLE
    shebang = f'#!{executable}'
    return '\n'.join(
        [
            shebang,
            platform_header]
    )


def as_body(body: str) -> str:
    """
    >>> as_body(body="echo 'OK!'")
    "echo 'OK!'"

    :param body:
    :return:
    """
    return body


def as_tailer() -> str:
    """
    >>> as_tailer()
    ''

    :return:
    """
    return ''
