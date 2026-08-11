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
from enum import Enum
from pathlib import Path

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.log.log import AutosubmitCritical, Log

__all__ = ["cat_log"]


class LogFile(str, Enum):
    """cat-log file type."""

    OUTPUT = "o"
    """Job or workflow output."""

    JOB = "j"
    """Job command file."""

    ERROR = "e"
    """Job or workflow error output."""

    STATUS = "s"
    """Job status file."""


class ViewMode(str, Enum):
    """cat-log command visualisation mode."""

    CAT = "c"
    """Use ``cat`` to display the logs."""

    TAIL = "t"
    """Use ``tail`` to display the logs."""


def _parse_log_file(file: str | None) -> LogFile:
    """Parse the log file argument.

    :param file: The log file type supplied by the user.
    :return: The corresponding :class:`LogFile`.
    :raises AutosubmitCritical: If the supplied file type is invalid.
    """
    if file is None:
        return LogFile.OUTPUT

    try:
        return LogFile(file)
    except ValueError:
        raise AutosubmitCritical(
            f"Invalid cat-log file {file}. Expected one of "
            f"{[log_file.value for log_file in LogFile]}",
            7011,
        ) from None


def _parse_view_mode(mode: str | None) -> ViewMode:
    """Parse the visualisation mode argument.

    :param mode: The visualisation mode supplied by the user.
    :return: The corresponding :class:`ViewMode`.
    :raises AutosubmitCritical: If the supplied mode is invalid.
    """
    if mode is None:
        return ViewMode.CAT

    try:
        return ViewMode(mode)
    except ValueError:
        raise AutosubmitCritical(
            f"Invalid cat-log mode {mode}. Expected one of "
            f"{[view_mode.value for view_mode in ViewMode]}",
            7011,
        ) from None


def _find_workflow_log(
    expid: str,
    exp_path: Path,
    aslogs_path: Path,
    log_file: LogFile,
) -> Path | None:
    """Find the latest workflow log of the requested type.

    :param expid: The workflow identifier.
    :param exp_path: The workflow directory.
    :param aslogs_path: The directory containing Autosubmit logs.
    :param log_file: The type of log to find.
    :return: The latest matching log file, or ``None`` if no log is found.
    :raises AutosubmitCritical: If a job log type is requested for a workflow.
    """
    if log_file is LogFile.JOB:
        raise AutosubmitCritical(
            "Invalid arguments for cat-log: workflow logs only support "
            "o(output), e(error), and s(status). "
            f"Requested: {log_file.value}",
            7011,
        )

    if log_file is LogFile.ERROR:
        search_pattern = "*_run_err.log"
        workflow_log_files = sorted(aslogs_path.glob(search_pattern))
    elif log_file is LogFile.OUTPUT:
        search_pattern = "*_run.log"
        workflow_log_files = sorted(aslogs_path.glob(search_pattern))
    else:
        search_pattern = f"{expid}_*.txt"
        status_files_path = exp_path / "status"
        workflow_log_files = sorted(status_files_path.glob(search_pattern))

    if not workflow_log_files:
        return None

    return workflow_log_files[-1]


def _find_job_log(
    exp_or_job_id: str,
    job_logs_path: Path,
    log_file: LogFile,
) -> Path | None:
    """Find the latest job log of the requested type.

    :param exp_or_job_id: The workflow or job identifier.
    :param job_logs_path: The directory containing the job logs.
    :param log_file: The type of log to find.
    :return: The latest matching log file, or ``None`` if no log is found.
    """
    if log_file is LogFile.JOB:
        return job_logs_path / f"{exp_or_job_id}.cmd"

    if log_file is LogFile.STATUS:
        return job_logs_path / f"{exp_or_job_id}_TOTAL_STATS"

    extension = "err" if log_file is LogFile.ERROR else "out"
    search_pattern = f"{exp_or_job_id}.*.{extension}"
    workflow_log_files = sorted(job_logs_path.glob(search_pattern))

    if not workflow_log_files:
        return None

    return workflow_log_files[-1]


def _validate_workflow_log(log_file: Path) -> Path:
    """Validate a workflow log file.

    :param log_file: The workflow log file to validate.
    :return: The validated log file.
    :raises AutosubmitCritical: If the path is not a regular file.
    """
    if not log_file.is_file():
        raise AutosubmitCritical(
            f"The workflow log file found is not a file: {log_file}",
            7011,
        )

    return log_file


def _validate_job_log(log_file: Path, file: LogFile) -> Path:
    """Validate a job log file.

    :param log_file: The job log file to validate.
    :param file: The type of job log being validated.
    :return: The validated log file.
    :raises AutosubmitCritical: If the path exists but is not a regular file.
    """
    if not log_file.exists():
        return log_file

    if not log_file.is_file():
        raise AutosubmitCritical(
            f"The job log file {file} found is not a file: {log_file}",
            7011,
        )

    return log_file


def _view_file(log_file: Path, mode: ViewMode) -> int:
    """Display the log file using the selected visualisation mode.

    :param log_file: The log file to display.
    :param mode: The visualisation mode to use.
    :return: The exit code of the underlying command.
    :raises ValueError: If the selected visualisation mode is invalid.
    """
    if mode is ViewMode.CAT:
        return subprocess.run(
            ["cat", str(log_file)],
            stdin=subprocess.DEVNULL,
        ).returncode

    if mode is ViewMode.TAIL:
        proc = subprocess.Popen(
            [
                "tail",
                "--lines=+1",
                "--retry",
                "--follow=name",
                str(log_file),
            ],
            stdin=subprocess.DEVNULL,
        )
        try:
            return proc.wait()
        except KeyboardInterrupt:
            proc.terminate()
            proc.wait()
            # If the user sends a CTRL+C, instead of ``proc.returncode``,
            # we return ``0`` (success); as ``tail`` never failed.
            return 0

    raise ValueError(f"Invalid cat-log visualisation mode: {mode}")


def _get_workflow_log(
    expid: str,
    exp_path: Path,
    aslogs_path: Path,
    log_file: LogFile,
) -> Path | None:
    """Find and validate the requested workflow log.

    :param expid: The workflow identifier.
    :param exp_path: The workflow directory.
    :param aslogs_path: The directory containing Autosubmit logs.
    :param log_file: The type of log to find.
    :return: The requested workflow log, or ``None`` if no log is found.
    :raises AutosubmitCritical: If the requested log type is invalid or the
        selected path is not a regular file.
    """
    workflow_log_file = _find_workflow_log(
        expid,
        exp_path,
        aslogs_path,
        log_file,
    )

    if workflow_log_file is None:
        return None

    return _validate_workflow_log(workflow_log_file)


def _get_job_log(
    exp_or_job_id: str,
    tmp_path: Path,
    exp_logs_path: Path,
    log_file: LogFile,
    inspect: bool,
) -> Path | None:
    """Find and validate the requested job log.

    :param exp_or_job_id: The workflow or job identifier.
    :param tmp_path: The workflow temporary directory.
    :param exp_logs_path: The workflow logs directory.
    :param log_file: The type of log to find.
    :param inspect: Whether to use the temporary directory for job logs.
    :return: The requested job log, or ``None`` if no log is found.
    :raises AutosubmitCritical: If the selected path exists but is not a
        regular file.
    """
    job_logs_path = tmp_path if inspect else exp_logs_path

    job_log_file = _find_job_log(
        exp_or_job_id,
        job_logs_path,
        log_file,
    )

    if job_log_file is None:
        return None

    return _validate_job_log(job_log_file, log_file)


def cat_log(
    exp_or_job_id: str,
    file: str | None,
    mode: str | None,
    inspect: bool = False,
) -> bool:
    """The cat-log command allows users to view Autosubmit logs using the command-line.

    It is possible to use ``autosubmit cat-log`` for Workflow and for Job logs. It decides
    whether to show Workflow or Job logs based on the ``ID`` given. Shorter ID's, such as
    ``a000`` are considered Workflow ID's, so it will display logs for that workflow. For
    longer ID's, such as ``a000_20220401_fc0_1_GSV``, the command will display logs for
    that specific job.

    Users can choose the log file using the ``FILE`` parameter, to display an error or
    output log file, for instance.

    Finally, the ``MODE`` parameter allows users to choose whether to display the complete
    file contents (similar to the ``cat`` command) or to start tailing its output (akin to
    ``tail -f``).

    :param exp_or_job_id: A workflow or job ID.
    :param file: The type of the file to be printed (not the file path!).
    :param mode: The mode to print the file (e.g. cat, tail).
    :param inspect: When True, use job files in tmp/ instead of tmp/LOG_a000/.
    :return: ``True`` if the log was successfully displayed, otherwise ``False``.
    :raises AutosubmitCritical: If the file or mode arguments are invalid, or
        if the selected log path is not a regular file.
    """
    log_file = _parse_log_file(file)
    view_mode = _parse_view_mode(mode)

    is_workflow = "_" not in exp_or_job_id
    expid = exp_or_job_id if is_workflow else exp_or_job_id[:4]

    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR, expid)
    tmp_path = exp_path / BasicConfig.LOCAL_TMP_DIR
    aslogs_path = tmp_path / BasicConfig.LOCAL_ASLOG_DIR
    exp_logs_path = tmp_path / f"LOG_{expid}"

    if is_workflow:
        log_path = _get_workflow_log(
            expid,
            exp_path,
            aslogs_path,
            log_file,
        )
    else:
        log_path = _get_job_log(
            exp_or_job_id,
            tmp_path,
            exp_logs_path,
            log_file,
            inspect,
        )

    if log_path is None:
        Log.info("No logs found.")
        return True

    return _view_file(log_path, view_mode) == 0
