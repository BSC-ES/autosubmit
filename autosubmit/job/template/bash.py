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

"""Autosubmit template scripts written in Bash."""

from textwrap import dedent

_DEFAULT_EXECUTABLE = "/bin/bash\n"
"""The default executable used when none provided."""

_AS_BASH_HEADER = dedent("""\
        ###################
        # Autosubmit header
        ###################
set -xvu
declare locale_to_set
        locale_to_set=$(locale -a | grep ^C.)
export job_name_ptrn='%CURRENT_LOGDIR%/%JOBNAME%'

r=0
bash -e <<'__AS_CMD__'
set -xve
        if [ -z "$locale_to_set" ] ; then
            # locale installed...
            export LC_ALL=$locale_to_set
        else
            # locale not installed...
            locale_to_set=$(locale -a | grep ^en_GB.utf8)
            if [ -z "$locale_to_set" ] ; then
                export LC_ALL=$locale_to_set
            else
                export LC_ALL=C
            fi 
        fi
        echo $(date +%s) > ${job_name_ptrn}_STAT_%FAIL_COUNT%

        ################### 
        # AS CHECKPOINT FUNCTION
        ###################
        # Creates a new checkpoint file upon call based on the current numbers of calls to the function
        function as_checkpoint {
            AS_CHECKPOINT_CALLS=$((AS_CHECKPOINT_CALLS+1))
            touch ${job_name_ptrn}_CHECKPOINT_${AS_CHECKPOINT_CALLS}
        }
AS_CHECKPOINT_CALLS=0
        %EXTENDED_HEADER%
set -u
        """)
"""Autosubmit Bash header."""

_AS_BASH_TAILER = dedent("""\
__AS_CMD__
r=$?

# Write the finish time in the job _STAT_
echo $(date +%s) >> ${job_name_ptrn}_STAT_%FAIL_COUNT%

# If the user-provided script failed, we exit here with the same exit code;
# otherwise, we let the execution of the tailer happen, where the _COMPLETED
# file will be created.
if [ $r -ne 0 ]; then
    exit $r
fi
        %EXTENDED_TAILER%
        ###################
        # Autosubmit tailer
        ###################
        set -xuve
        touch ${job_name_ptrn}_COMPLETED
        exit 0
    
        """)
"""Autosubmit Bash tailer."""


def as_header(platform_header: str, executable: str) -> str:
    executable = executable or _DEFAULT_EXECUTABLE
    shebang = f'#!{executable}'

    return '\n'.join(
        [
            shebang,
            platform_header,
            _AS_BASH_HEADER]
    )


def as_body(body: str) -> str:
    return dedent(f"""\
###################
# Autosubmit job
###################
{body}

""")


def as_tailer() -> str:
    return _AS_BASH_TAILER
