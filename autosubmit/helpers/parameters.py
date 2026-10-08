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
from collections import defaultdict

from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.job.job import Job
from autosubmit.job.job_list import JobList
from autosubmit.platforms.platform import Platform

CLASSES = {
    Job,
    JobList,
    Platform,
    AutosubmitConfig,
}


def get_parameters() -> dict[str, dict[str, str]]:
    parameters: dict[str, dict[str, str]] = defaultdict(dict)

    for cls in CLASSES:
        for name, attribute in vars(cls).items():
            if not isinstance(attribute, property):
                continue

            doc = inspect.getdoc(attribute.fget)

            if not doc:
                continue

            group = extract_group(doc)

            if group is None:
                continue

            description = remove_group(doc)

            parameters[group][name] = description.splitlines()[0].strip()

    return dict(parameters)


def extract_group(doc: str) -> str | None:
    for line in doc.splitlines():
        if line.strip().startswith(":autosubmit-group:"):
            return line.split(":", 2)[2].strip().upper()

    return None


def remove_group(doc: str) -> str:
    lines = [
        line
        for line in doc.splitlines()
        if not line.strip().startswith(":autosubmit-group:")
    ]

    return "\n".join(lines).strip()
