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

"""The cat-log code.

Used to write logs to streams.
"""

import subprocess
from contextlib import suppress
from pathlib import Path

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.log.log import AutosubmitCritical, Log

__all__ = ["cat_log"]


def cat_log(
    exp_or_job_id: str, file: None | str, mode: None | str, inspect: bool = False
) -> bool:
    """The cat-log command allows users to view Autosubmit logs using the command-line.

    It is possible to use ``autosubmit cat-log`` for Workflow and for Job logs. It decides
    whether to show Workflow or Job logs based on the ``ID`` given. Shorter ID's, such as
    ``a000` are considered Workflow ID's, so it will display logs for that workflow. For
    longer ID's, such as ``a000_20220401_fc0_1_GSV``, the command will display logs for
    that specific job.

    Users can choose the log file using the ``FILE`` parameter, to display an error or
    output log file, for instance.

    Finally, the ``MODE`` parameter allows users to choose whether to display the complete
    file contents (similar to the ``cat`` command) or to start tailing its output (akin to
    ``tail -f``).

    :param exp_or_job_id: A workflow or job ID.
    :param file: the type of the file to be printed (not the file path!).
    :param mode: the mode to print the file (e.g. cat, tail).
    :param inspect: when True it will use job files in tmp/ instead of tmp/LOG_a000/.
    """

    def view_file(log_file: Path, mode: str):
        if mode == "c":
            cmd = ["cat", str(log_file)]
            subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=None)
            return 0
        elif mode == "t":
            cmd = [
                "tail",
                "--lines=+1",
                "--retry",
                "--follow=name",
                str(workflow_log_file),
            ]
            proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL)
            with suppress(KeyboardInterrupt):
                return proc.wait() == 0

    MODES = {"c": "cat", "t": "tail"}
    FILES = {"o": "output", "j": "job", "e": "error", "s": "status"}
    if file is None:
        file = "o"
    if file not in FILES:
        raise AutosubmitCritical(
            f"Invalid cat-log file {file}. Expected one of {[f for f in FILES]}", 7011
        )
    if mode is None:
        mode = "c"
    if mode not in MODES:
        raise AutosubmitCritical(
            f"Invalid cat-log mode {mode}. Expected one of {[m for m in MODES]}", 7011
        )

    is_workflow = "_" not in exp_or_job_id

    expid = exp_or_job_id if is_workflow else exp_or_job_id[:4]

    # Workflow folder.
    # e.g. ~/autosubmit/a000
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR, expid)
    # Directory with workflow temporary/volatile files. Contains the output of commands such as inspect,
    # and also STAT/COMPLETED files for each workflow task.
    # e.g. ~/autosubmit/a000/tmp
    tmp_path = exp_path / BasicConfig.LOCAL_TMP_DIR
    # Directory with logs for Autosubmit executed commands (create, run, etc.) and jobs statuses files.
    # e.g. ~/autosubmit/a000/tmp/ASLOGS
    aslogs_path = tmp_path / BasicConfig.LOCAL_ASLOG_DIR
    # Directory with the logs of the workflow run, for each workflow task. Includes the generated
    # .cmd files, and STAT/COMPLETED files for the run. The files with similar names in the parent
    # directory are generated with inspect, while these are with the run subcommand.
    # e.g. ~/autosubmit/a000/tmp/LOG_a000
    exp_logs_path = tmp_path / f"LOG_{expid}"

    if is_workflow:
        if file not in ["o", "e", "s"]:
            raise AutosubmitCritical(
                f"Invalid arguments for cat-log: workflow logs only support o(output), "
                f"e(error), and s(status). Requested: {mode}",
                7011,
            )

        if file in ["e", "o"]:
            search_pattern = "*_run_err.log" if file == "e" else "*_run.log"
            workflow_log_files = sorted(aslogs_path.glob(search_pattern))
        else:
            search_pattern = f"{expid}_*.txt"
            status_files_path = exp_path / "status"
            workflow_log_files = sorted(status_files_path.glob(search_pattern))

        if not workflow_log_files:
            Log.info("No logs found.")
            return True

        workflow_log_file = workflow_log_files[-1]
        if not workflow_log_file.is_file():
            raise AutosubmitCritical(
                f"The workflow log file found is not a file: {workflow_log_file}", 7011
            )

        return view_file(workflow_log_file, mode) == 0
    else:
        job_logs_path = tmp_path if inspect else exp_logs_path
        if file == "j":
            workflow_log_file = job_logs_path / f"{exp_or_job_id}.cmd"
        elif file == "s":
            workflow_log_file = job_logs_path / f"{exp_or_job_id}_TOTAL_STATS"
        else:
            search_pattern = f"{exp_or_job_id}.*.{'err' if file == 'e' else 'out'}"
            workflow_log_files = sorted(job_logs_path.glob(search_pattern))
            if not workflow_log_files:
                Log.info("No logs found.")
                return True
            workflow_log_file = workflow_log_files[-1]

        if not workflow_log_file.exists():
            Log.info("No logs found.")
            return True

        if not workflow_log_file.is_file():
            raise AutosubmitCritical(
                f"The job log file {file} found is not a file: {workflow_log_file}",
                7011,
            )

        return view_file(workflow_log_file, mode) == 0
