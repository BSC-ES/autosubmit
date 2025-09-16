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

"""Autosubmit template scripts written in Python 2."""

from textwrap import dedent

_DEFAULT_EXECUTABLE = "/usr/bin/env python2"
"""The default executable used when none provided."""

_AS_PY2_HEADER = dedent("""\
        ###################
        # Autosubmit header
        ###################
        import locale
        import time
        try:
            try:
                locale.setlocale(locale.LC_ALL,'C.utf8')
            except Exception as e:
                try:
                    locale.setlocale(locale.LC_ALL, 'C.UTF-8')
                except Exception as e:
                    try:
                        locale.setlocale(locale.LC_ALL, 'en_GB')
                    except Exception as e:
                        locale.setlocale(locale.LC_ALL, 'es_ES')
        except Exception as e:
            locale.setlocale(locale.LC_ALL, 'C')
        job_name_ptrn = '%CURRENT_LOGDIR%/%JOBNAME%'
        stat_file = open(job_name_ptrn + '_STAT_%FAIL_COUNT%', 'w')
        stat_file.write(f'{int(time.time())}\\n')
        stat_file.close()
        ###################
        # Autosubmit Checkpoint
        ###################
        # Creates a new checkpoint file upton call based on the current numbers of calls to the function
        AS_CHECKPOINT_CALLS = 0
        def as_checkpoint():
            global AS_CHECKPOINT_CALLS
            global job_name_ptrn
            AS_CHECKPOINT_CALLS = AS_CHECKPOINT_CALLS + 1
            open(job_name_ptrn + '_CHECKPOINT_' + str(AS_CHECKPOINT_CALLS), 'w').close()      
        %EXTENDED_HEADER%

""")
"""Autosubmit Python 2 header."""

_AS_PY2_TAILER = dedent("""\
        %EXTENDED_TAILER%
        ###################
        # Autosubmit tailer
        ###################

        open(job_name_ptrn + '_COMPLETED', 'a').close()
        exit(0)

        """)
"""Autosubmit Python 2 tailer."""


def as_header(platform_header: str, executable: str) -> str:
    executable = executable or _DEFAULT_EXECUTABLE
    shebang = f'#!{executable}'

    return '\n'.join(
        [
            shebang,
            platform_header,
            _AS_PY2_HEADER]
    )


def as_body(body: str) -> str:
    return dedent(
        f"""
        ###################
        # Autosubmit job
        ###################

        try:
            {body}
        finally:
            stat_file = open(job_name_ptrn + '_STAT_%FAIL_COUNT%', 'a')
            stat_file.write(f'{{int(time.time())}}\\n')
            stat_file.close()

        # Now, we let the execution of the tailer happen, where the _COMPLETED
        # file will be created.
        """)


def as_tailer() -> str:
    return _AS_PY2_TAILER
