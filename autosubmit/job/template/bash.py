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
# AS TRAP FUNCTIONS
###################
# Used in trap functions to control the exit code of the script.
AS_EXIT_CODE=0

# as_signals_handler
#
# This function is intended to be used as a trap handler and
# terminates the script with a signal-derived exit code.
#
# List of captured signals:
# Ref: https://services.criann.fr/en/services/hpc/cluster-myria/guide/signals-sent-by-slurm/
#
# - SIGHUP,  terminal/session closed
# - SIGINT,  user interrupt, e.g., CTRL+C
# - SIGQUIT, quiet/core dump, e.g., CTRL-\
# - SIGPIPE, broken pipe
# - SIGTERM, graceful termination request, e.g., Slurm wallclock (then a SIGKILL shortly afterwards)
# - SIGXCPU, CPU time limit exceeded, e.g., job exceeded Slurm/PBS CPU time
# - SIGXFSZ, File size limit exceeded, e.g., Kernel detected a process is exceeding `ulimit -f`/quota
#
# We do NOT capture SIGUSR1 and SIGUSR2. These are user signals that can be enabled in
# Slurm via `--signal`. But since these will be executed seconds before a SIGTERM, it
# is safer to capture SIGTERM only (imagine if we receive the SIGUSR1, and in 2 seconds
# the SIM job completes successfully -- we do not want to capture it and mark as an error).
#
# We also do not capture SIGPIPE. SIGPIPE would work with our `-o pipefail`, but it
# could trigger the trap function if any command in the user template raises a pipe
# error, even if the user disabled the pipe fail. The `as_exit_handler` function
# handles actual broken pipe errors.
#
# Arguments:
#   $1 - the Unix signal
#
# Globals:
#   AS_EXIT_CODE
#
# Side effects:
#   Exits the script with the computed exit code (use with care if calling this directly).
#
# Exit codes:
#   1            if a signal was received but we do not have a match case 
#   128 + signal based on the signal received
function as_signals_handler() {
    local signal="$1"
    # Convert signal to standard Unix exit code:
    # exit code = 128 + signal number
    # Ref: https://www.ditig.com/linux-exit-status-codes#avoid-confusion-with-signal-numbers
    case "$signal" in
        SIGHUP)     AS_EXIT_CODE=129 ;;
        SIGINT)     AS_EXIT_CODE=130 ;;
        SIGQUIT)    AS_EXIT_CODE=131 ;;
        SIGTERM)    AS_EXIT_CODE=143 ;;
        SIGXCPU)    AS_EXIT_CODE=152 ;;
        SIGXFSZ)    AS_EXIT_CODE=153 ;;   
        *)          AS_EXIT_CODE=1   ;;
    esac
    # echo "Caught signal ${signal}, exiting with code ${AS_EXIT_CODE}" >&2  # For debugging...
    exit $AS_EXIT_CODE
}

# This function will be called on EXIT, ensuring the STAT file is always created
function as_exit_handler {
    # This is the exit code of the last command, which may be 0 (zero) even if
    # we received a signal (e.g., SIGTERM from Slurm due to wallclock limit).
    local last_cmd_code=$?
    
    # If AS_EXIT_CODE was set by the signal handler, use that. 
    # Otherwise, use the last command's exit code.
    if [ "$AS_EXIT_CODE" -eq 0 ]; then
        AS_EXIT_CODE=$last_cmd_code
    fi
    
    # Write the finish time in the job _STAT_
    echo "$(date +%s)" >> "${job_name_ptrn}_STAT_%FAIL_COUNT%"
    
    if [ "$AS_EXIT_CODE" -eq 0 ]; then
        touch "${job_name_ptrn}_COMPLETED"
        # If the user-provided script failed, we exit here with the same exit code.
    fi
    
    trap - EXIT
    exit $AS_EXIT_CODE
}

# Set up the signals trap to ensure the job is killed on signals
trap 'as_signals_handler SIGHUP'  SIGHUP
trap 'as_signals_handler SIGINT'  SIGINT
trap 'as_signals_handler SIGQUIT' SIGQUIT
trap 'as_signals_handler SIGTERM' SIGTERM
trap 'as_signals_handler SIGXCPU' SIGXCPU
trap 'as_signals_handler SIGXFSZ' SIGXFSZ

# Set up the exit trap to ensure exit code always runs
trap as_exit_handler EXIT

########################
# AS CHECKPOINT FUNCTION
########################
# Creates a new checkpoint file upon call based on the current numbers of calls to the function
function as_checkpoint {
    AS_CHECKPOINT_CALLS=$((AS_CHECKPOINT_CALLS+1))
    touch "${job_name_ptrn}_CHECKPOINT_${AS_CHECKPOINT_CALLS}"
}
AS_CHECKPOINT_CALLS=0

%EXTENDED_HEADER%
""")
"""Autosubmit Bash header."""

_AS_BASH_TAILER = dedent("""\
###################
# Autosubmit tailer
###################
# Job completed successfully

%EXTENDED_TAILER%

# https://support.schedmd.com/show_bug.cgi?id=9715
wait

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
