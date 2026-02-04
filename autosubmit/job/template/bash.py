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
set -xuve -o pipefail
declare locale_to_set
locale_to_set=$(locale -a | grep ^C. || true)
export job_name_ptrn='%CURRENT_LOGDIR%/%JOBNAME%'

if [ -n "$locale_to_set" ] ; then
    # locale installed...
    export LC_ALL="$locale_to_set"
else
    # locale not installed...
    locale_to_set=$(locale -a | grep -i '^en_GB.utf8' || true)
    if [ -n "$locale_to_set" ] ; then
        export LC_ALL="$locale_to_set"
    else
        export LC_ALL=C
    fi 
fi
echo "$(date +%s)" > "${job_name_ptrn}_STAT_%FAIL_COUNT%"

################### 
# AS TRAP FUNCTION
###################
# This function will be called on EXIT, ensuring the STAT file is always created
function as_exit_handler {
    local exit_code=$?
    
    if [ "$exit_code" -eq 0 ]; then
        touch "${job_name_ptrn}_COMPLETED"
        # If the user-provided script failed, we exit here with the same exit code;
        # otherwise, we let the execution of the tailer happen, where the _COMPLETED
        # file will be created.
    fi
    
    # Write the finish time in the job _STAT_
    echo "$(date +%s)" >> "${job_name_ptrn}_STAT_%FAIL_COUNT%"
    
    exit $exit_code
}

########################
# AS CHECKPOINT FUNCTION
########################
# Creates a new checkpoint file upon call based on the current numbers of calls to the function
function as_checkpoint {
    AS_CHECKPOINT_CALLS=$((AS_CHECKPOINT_CALLS+1))
    touch "${job_name_ptrn}_CHECKPOINT_${AS_CHECKPOINT_CALLS}"
}
AS_CHECKPOINT_CALLS=0

# Set up the exit trap to ensure exit code always runs
trap as_exit_handler EXIT

%EXTENDED_HEADER%
""")
"""Autosubmit Bash header."""

_AS_BASH_TAILER = dedent("""\
###################
# Autosubmit tailer
###################
%EXTENDED_TAILER%
# Job completed successfully
# The exit trap will handle the tailer
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
################
# Autosubmit job
################
{body}
""")


def as_tailer() -> str:
    return _AS_BASH_TAILER
