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

import inspect

from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.job.job import Job
from autosubmit.job.job_list import JobList
from autosubmit.platforms.platform import Platform

CLASSES = (
    Job,
    JobList,
    Platform,
    AutosubmitConfig,
)

extra_parameters = {
    "EDGE": {
        "CHUNK_END_DATE": "Chunk end date",
        "CHUNK_END_DATE_LAST": "End date of the last chunk, i.e. the experiment end boundary. Available to any date-aware job (notably RUNNING: date jobs).",
        "CHUNK_END_DAY": "Chunk end day.",
        "CHUNK_END_HOUR": "Chunk end hour.",
        "CHUNK_END_IN_DAYS": "Days passed from the start of the simulation until the end of the chunk.",
        "CHUNK_END_MONTH": "Chunk end month.",
        "CHUNK_END_YEAR": "Chunk end year.",
        "CHUNK_FIRST": "True if the current chunk is the first, false otherwise.",
        "CHUNK_LAST": "True if the current chunk is the last, false otherwise.",
        "CHUNK_SECOND_TO_LAST_DATE": "Chunk second to last date.",
        "CHUNK_SECOND_TO_LAST_DAY": "Chunk second to last day.",
        "CHUNK_SECOND_TO_LAST_HOUR": "Chunk second to last hour.",
        "CHUNK_SECOND_TO_LAST_MONTH": "Chunk second to last month.",
        "CHUNK_SECOND_TO_LAST_YEAR": "Chunk second to last year.",
        "CHUNK_START_DATE": "Chunk start date.",
        "CHUNK_START_DAY": "Chunk start day.",
        "CHUNK_START_HOUR": "Chunk start hour.",
        "CHUNK_START_MONTH": "Chunk start month.",
        "CHUNK_START_YEAR": "Chunk start year.",
        "DAY_BEFORE": "Day before the start date.",
        "LDATE": "Last date of the experiment (the run's final day, parallel to SDATE).",
        "NOTIFY_ON": "Determine the job statuses you want to be notified.",
        "PREV": "Days since start date at the chunk's start.",
        "RUN_DAYS": "Chunk length in days",
    },
    "CONFIG": {
        "AUTOSUBMIT_VERSION": "Current version of Autosubmit.",
        "MAXWAITINGJOBS": "Maximum number of jobs permitted in the waiting status.",
        "TOTALJOBS": "Total number of jobs in the workflow.",
        "STATUS": "Sets the status of the job",
    },
    "DEFAULT":{
        "CUSTOM_CONFIG": "Custom configuration location.",
        "HPCARCH": "Default HPC platform name.",
    },
    "EXPERIMENT": {
        "CALENDAR": "Calendar used for the experiment. Can be standard or noleap.",
        "CHUNKSIZE": "Size of each chunk.",
        "CHUNKSIZEUNIT": "Unit of the chunk size. Can be hour, day, month, or year.",
        "DATELIST": "List of start dates",
        "MEMBERS": "List of members.",
        "NUMCHUNKS": "Number of chunks of the experiment.",
    },
    "PROJECT": {
        "PROJECT_DESTINATION" : "Folder to hold the project sources.",
        "PROJECT_TYPE" : "Type of the project.",
    },
}
def get_parameters() -> dict[str, dict[str, str]]:
    """Collect documented properties, grouped by their Autosubmit group."""
    parameters: dict[str, dict[str, str]] = {}

    for cls in CLASSES:
        for name, attribute in vars(cls).items():
            if not isinstance(attribute, property) or attribute.fget is None:
                continue

            doc = inspect.getdoc(attribute.fget)
            if not doc:
                continue

            group, variable_name, description = parse_property_docstring(doc)

            if group is None:
                continue

            if variable_name != "":
                parameters.setdefault(group, {})[variable_name.upper()] = description
            else:
                parameters.setdefault(group, {})[name.upper()] = description

    for key in extra_parameters:
        if key in parameters:
            parameters[key].update(extra_parameters[key])
        else:
            parameters[key] = extra_parameters[key]

    return parameters


def parse_property_docstring(doc: str) -> tuple[str, str , str]:
    """Extract the Autosubmit group and description from a property docstring."""
    group: str = ""
    variable_name: str = ""
    description_lines: list[str] = []

    for line in doc.splitlines():
        stripped = line.strip()

        if stripped.startswith(":autosubmit-group:"):
            if group == "":
                group = stripped.partition(":autosubmit-group:")[2].strip().upper()
            continue

        if stripped.startswith(":autosubmit-name:"):
            if variable_name == "":
                variable_name = stripped.partition(":autosubmit-name:")[2].strip().upper()
            continue

        description_lines.append(line)

    description = next(
        (line.strip() for line in description_lines if line.strip()),
        "",
    )

    return group, variable_name, description
