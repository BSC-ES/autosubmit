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

"""Job validation."""

import json
import re
from typing import TYPE_CHECKING

from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.job.job_common import Status, separate_section_entries
from autosubmit.log.log import AutosubmitCritical
from autosubmit.utils import create_json

if TYPE_CHECKING:
    from autosubmit.job.job_list import JobList

__all__ = ["validate_job_filters"]


def _validate_section(as_conf: AutosubmitConfig, filter_section: str) -> None:
    """Validate the ``-ft`` section filter.

    Each entry must be an exact section name, optionally followed by a split
    selector in brackets. The filter is comma-separated and supports ``ANY``
    as a wildcard.

    :param as_conf: Autosubmit configuration object
    :param filter_section: string with the sections separated by comma
    :return None if the filter is valid
    :raises AutosubmitCritical: if the filter is invalid, with a message describing the errors found
    """
    section_validation_message = "\n## Section Validation Message ##"
    section_entries = separate_section_entries(filter_section)

    if not section_entries:
        section_validation_message += "\n\tEmpty input. No changes performed."
    else:
        valid_sections = {str(section).upper() for section in as_conf.jobs_data}
        section_validation_message = _validate_section_split_formula(
            ", ".join(section_entries), section_validation_message, valid_sections
        )

    if section_validation_message != "\n## Section Validation Message ##":
        raise AutosubmitCritical(
            "Error in the supplied input for -ft.",
            7011,
            section_validation_message,
        )


def _validate_list(as_conf, job_list, filter_list):
    """
    Validate the ``-fl`` job filter.

    Each entry must be an exact job name. The filter is space-separated and supports ``ANY`` as a wildcard.

    :param as_conf: Autosubmit configuration object
    :param job_list: JobList object containing the jobs to validate against
    :param filter_list: string with the jobs separated by space
    :return None if the filter is valid
    :raises AutosubmitCritical: if the filter is invalid, with a message describing the errors found
    """
    job_validation_error = False
    job_error = False
    job_not_foundList = []
    job_validation_message = "\n## Job Validation Message ##"
    jobs = []
    countStart = filter_list.count("[")
    countEnd = filter_list.count("]")
    if countStart > 1 or countEnd > 1:
        job_validation_error = True
        job_validation_message += "\n\tList of jobs has a format error. Perhaps you were trying to use -fc instead."

    if job_validation_error is False:
        for job in job_list.get_job_list():
            jobs.append(job.name)
        if len(str(filter_list).strip()) > 0:
            if len(filter_list.split()) > 0:
                for sentJob in filter_list.split():
                    # Provided job does not exist, or it is not the keyword 'Any'
                    if sentJob not in jobs and (sentJob.upper() != "ANY"):
                        job_error = True
                        job_not_foundList.append(sentJob)
        else:
            job_validation_error = True
            job_validation_message += "\n\tEmpty input. No changes performed."

    if job_validation_error is True or job_error is True:
        if job_error is True:
            job_validation_message += (
                "\n\tSpecified job(s) : ["
                + str(job_not_foundList)
                + "] not found in the experiment "
                + str(as_conf.expid)
                + ". \n\tProcess stopped. Review the format of the provided input. Comparison is case sensitive."
                + "\n\tRemember that this option expects job names separated by a blank space as input."
            )
        raise AutosubmitCritical(
            "Error in the supplied input for -fl.", 7011, job_validation_message
        )


def _validate_status(job_list, filter_status):
    """
    Validate the ``-fs`` status filter.

    Each entry must be an exact status name. The filter is space-separated and supports ``ANY`` as a wildcard.

    :param job_list: JobList object containing the jobs to validate against
    :param filter_status: string with the statuses separated by space
    :return None if the filter is valid
    :raises AutosubmitCritical: if the filter is invalid, with a message describing the errors found
    """
    status_validation_error = False
    status_validation_message = "\n## Status Validation Message ##"
    # Trying to identify chunk formula
    countStart = filter_status.count("[")
    countEnd = filter_status.count("]")
    if countStart > 1 or countEnd > 1:
        status_validation_error = True
        status_validation_message += "\n\tList of status provided has a format error. Perhaps you were trying to use -fc instead."
    # If everything is fine until this point
    if status_validation_error is False:
        status_filter = filter_status.split()
        status_reference = Status()
        status_list = []
        for job in job_list.get_job_list():
            reference = status_reference.VALUE_TO_KEY[job.status]
            if reference not in status_list:
                status_list.append(reference)
        for status in status_filter:
            if status.upper() == "ANY":
                continue
            if status not in status_list:
                status_validation_error = True
                status_validation_message += (
                    "\n\t There are no jobs with status "
                    + status
                    + " in this experiment."
                )
    if status_validation_error is True:
        raise AutosubmitCritical(
            "Error in the supplied input for -fs.", 7011, status_validation_message
        )


def _validate_chunk_formula(chunk_formula: str, validation_message: str) -> str:
    """Validate chunk formula syntax.

    [ 19900101 [ fc0 [ Any ] fc1 [1 2] ] 19950101 [ fc0 [1-10] ] ]

    :param chunk_formula: Chunk formula string.
    :param validation_message: Message to append validation errors to.
    :return: Updated validation message with any errors found.
    """

    if not chunk_formula:
        validation_message += "\n\tMissing chunk formula before the first comma."
        if "[" not in chunk_formula or "]" not in chunk_formula:
            validation_message += "\n\tMissing chunk formula brackets."
        return validation_message

    brackets_left = chunk_formula.count("[")
    brackets_right = chunk_formula.count("]")
    if brackets_left != brackets_right:
        validation_message += "\n\tUnbalanced brackets in chunk formula."

    try:
        json_data = json.loads(create_json(chunk_formula))
    except Exception as e:
        validation_message += "\n\tMust follow chunk formula structure: [ DATE [ MEMBER [ CHUNKS ] ... ] ... ]"
        validation_message += f"\n\tJSON Error: {str(e)}"
        return validation_message

    dates = "sds"
    members = "ms"
    chunks = "cs"
    date = "sd"
    member = "m"

    sections = []
    json_validation_message = ""
    for date_entry in json_data[dates]:
        try:
            date_str = date_entry[date]
        except KeyError:
            json_validation_message += "\n\tMissing DATE in chunk formula."
            continue
        for member_entry in date_entry[members]:
            try:
                member_str = member_entry[member]
            except KeyError:
                json_validation_message += "\n\tMissing MEMBER in chunk formula."
                continue
            try:
                chunks_str = (
                    str(len(member_entry[chunks]))
                    if "ANY" not in member_entry[chunks]
                    else "ANY"
                )
            except KeyError:
                json_validation_message += "\n\tMissing CHUNKS in chunk formula."
                continue
            section_str = f"{date_str} [ {member_str} [ {chunks_str} ] ]"
            sections.append(section_str)

    if json_validation_message:
        validation_message += "\n\tMust follow chunk formula structure: [ DATE [ MEMBER [ CHUNKS ] ... ] ... ]"
        validation_message += json_validation_message

    return validation_message


def _validate_section_split_formula(
    section_split_formula: str,
    validation_message: str,
    valid_sections: set[str] | None = None,
) -> str:
    """Validate section/split formula syntax.

    Expects to receive the second part of the -ftcs filter. ex: SIM [ Any ], SIM2 [1 2], SIM3.

    :param section_split_formula: section_split_formula string.
    :param validation_message: Message to append validation errors to.
    :param valid_sections: Set of valid section names.
    :return: Updated validation message with any errors found.
    """

    if not section_split_formula:
        return validation_message

    for section in [
        section.strip()
        for section in section_split_formula.split(",")
        if section.strip()
    ]:
        section_name = section.strip().split("[")[0].strip().upper()
        if (
            valid_sections is not None
            and section_name not in valid_sections
            and section_name != "ANY"
        ):
            validation_message += f"\n\tSpecified section not found: {section_name}."

        if "[" not in section and "]" not in section:
            if len(section.split()) > 1:
                validation_message += f"\n\tMalformed section/split entry: {section}. "
                continue
        else:
            brackets_left = section.count("[")
            brackets_right = section.count("]")
            if brackets_left != brackets_right:
                validation_message += (
                    "\n\tUnbalanced brackets in section/split formula."
                )

            if brackets_left > 1:
                validation_message += f"\n\tToo many opening brackets '[' in section/split entry: {section}."
            if brackets_right > 1:
                validation_message += f"\n\tToo many closing brackets ']' in section/split entry: {section}."
            if brackets_left == 1 and brackets_right == 1:
                splits = section.split("[")[-1].split("]")[0].strip().upper()
                if any(char in splits for char in (":", "-")):
                    start_end = re.split(r"[:\-]", splits)
                    if len(start_end) < 2:
                        validation_message += f"\n\tIncomplete split range in section/split entry: {splits}."
                    elif not (
                        start_end[0].strip().isdigit()
                        and start_end[-1].strip().isdigit()
                    ):
                        validation_message += f"\n\tNon-integer split range in section/split entry: {splits}."
                elif not all(
                    p.strip().isdigit() or p.strip().upper() == "ANY"
                    for p in splits.split()
                ):
                    validation_message += (
                        f"\n\tNon-integer split in section/split entry: {splits}."
                    )

    return validation_message


def _validate_chunk_section_split(
    filter_string: str, valid_sections: set[str] | None = None
) -> None:
    """Validate a chunk/section/split filter string for commands using -fc/-ftc/-ftcs.

    Validate that the filter string contains a chunk formula and, optionally,
    a comma-separated list of section names. Section names are checked
    case-insensitively against the keys in ``as_conf.jobs_data``.
    Splits are also optional and must be included in the Section part of the formula.
    [ chunk_splits_formula, section1 [splits], section2 [splits], ... ]
    Example:
    [ 19900101 [ fc0 [ Any ] fc1 [1 2] ] 19950101 [ fc0 [1-10] ], SIM [ Any ], SIM2 [1 2] ]

    :param filter_string: Filter string with form '<chunk_split_formula>[,<SECTION>[<splits>],...]'.
    :param valid_sections: Set of valid section names for validation. If None, section names are not validated.
    :return: None if the filter string is valid.
    :raises AutosubmitCritical: If the input is malformed or references unknown sections.
    """

    validation_message = "## -fc // -ftc // -ftcs Validation Message ##"
    filter_string = filter_string.upper().strip()

    level = 0
    for i, ch in enumerate(filter_string):
        if ch == "[":
            level += 1
        elif ch == "]":
            level -= 1
            if level < 0:
                validation_message += f"\n\tUnexpected ']' at position {i}."
    if level != 0:
        validation_message += "\n\tUnbalanced brackets in filter string."

    filter_string_parts = filter_string.split(",")
    chunk_formula = filter_string_parts[0].strip()
    section_split_formula = (
        ",".join(filter_string_parts[1:]) if len(filter_string_parts) > 1 else ""
    )

    validation_message = _validate_chunk_formula(chunk_formula, validation_message)
    validation_message = _validate_section_split_formula(
        section_split_formula, validation_message, valid_sections=valid_sections
    )

    if validation_message != "## -fc // -ftc // -ftcs Validation Message ##":
        raise AutosubmitCritical(
            "Error in the supplied input for -fc // -ftc // -ftcs.",
            7011,
            validation_message,
        )


def validate_job_filters(
    as_conf: AutosubmitConfig,
    job_list: "JobList",
    filter_list: str | None,
    filter_chunk_section_split: str | None,
    filter_status: str | None,
    filter_section: str | None,
) -> None:
    """Validate filters provided to the setstatus and recovery command.

    Each non-empty filter is validated by its corresponding helper. Raises
    AutosubmitCritical (code 7014) if all filters are empty or whitespace-only.

    :param as_conf: Autosubmit configuration object.
    :param job_list: JobList object containing jobs to validate against.
    :param filter_list: Job name list filter (``-fl``).
    :param filter_chunk_section_split: Chunk/section/split filter (``-fc``, ``-ftc``, ``-ftcs``).
    :param filter_status: Status filter (``-fs``).
    :param filter_section: Section filter (``-ft``).
    :return: None if all provided filters are valid.
    :raises AutosubmitCritical: If no non-empty filter is provided or if any validator fails.
    """
    all_empty = True
    if filter_section:
        _validate_section(as_conf, filter_section)
        all_empty = False

    if filter_list:
        _validate_list(as_conf, job_list, filter_list)
        all_empty = False

    if filter_status:
        _validate_status(job_list, filter_status)
        all_empty = False

    if filter_chunk_section_split:
        valid_sections = {str(section).upper() for section in as_conf.jobs_data}
        _validate_chunk_section_split(
            filter_chunk_section_split, valid_sections=valid_sections
        )
        all_empty = False

    if all_empty:
        raise AutosubmitCritical(
            "At least one filter must be provided and must be not empty when using -fs, -ft, -fc, -ftc or -ftcs.",
            7014,
        )
