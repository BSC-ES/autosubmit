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

from pathlib import Path
from typing import Callable, Optional

from ruamel.yaml import YAML

from autosubmit.config.basicconfig import BasicConfig


def as_conf_default_values(autosubmit_version: str, exp_id: str, hpc: str = "", minimal_configuration: bool = False,
                           git_repo: str = "", git_branch: str = "main", git_as_conf: str = "") -> None:
    """Replace default values in as_conf files.

    :param autosubmit_version: autosubmit version
    :param exp_id: experiment id
    :param hpc: platform
    :param minimal_configuration: minimal configuration
    :param git_repo: path to project git repository
    :param git_branch: main branch
    :param git_as_conf: path to as_conf file in git repository
    :return: None
    """
    # open and replace values
    yaml = YAML(typ='rt')
    for as_conf_file in Path(BasicConfig.LOCAL_ROOT_DIR, f"{exp_id}/conf").iterdir():
        as_conf_file_name = as_conf_file.name.lower()
        if as_conf_file_name.endswith(('.yml', '.yaml')):
            with open(as_conf_file, 'r+') as file:
                yaml_data = yaml.load(file)
                if 'CONFIG' in yaml_data:
                    yaml_data['CONFIG']['AUTOSUBMIT_VERSION'] = autosubmit_version

                if 'MAIL' in yaml_data:
                    yaml_data['MAIL']['NOTIFICATIONS'] = False
                    yaml_data['MAIL']['TO'] = ""

                if 'DEFAULT' in yaml_data:
                    yaml_data['DEFAULT']['EXPID'] = exp_id
                    if hpc != "":
                        yaml_data['DEFAULT']['HPCARCH'] = hpc
                    elif not yaml_data['DEFAULT']['HPCARCH']:
                        yaml_data['DEFAULT']['HPCARCH'] = "local"

                if 'LOCAL' in yaml_data:
                    yaml_data['LOCAL']['PROJECT_PATH'] = ""

                if 'GIT' in yaml_data:
                    if git_repo != "":
                        yaml_data['GIT']['PROJECT_ORIGIN'] = f'{git_repo}'
                    if git_branch != "":
                        yaml_data['GIT']['PROJECT_BRANCH'] = f'{git_branch}'
                
                if 'PROJECT' in yaml_data:
                    if git_repo != "":
                        yaml_data['PROJECT']['PROJECT_TYPE'] = 'git'
                        destination = yaml_data['PROJECT'].get('PROJECT_DESTINATION', '')
                        # Overwrite only if empty
                        if not str(destination).strip():
                            yaml_data['PROJECT']['PROJECT_DESTINATION'] = 'git_project'

                if 'DEFAULT' in yaml_data and git_repo and git_as_conf:
                    yaml_data['DEFAULT']['CUSTOM_CONFIG'] = f"%PROJDIR%/{git_as_conf}"

            yaml.dump(yaml_data, as_conf_file)

def separate_section_entries(filter_entries: str) -> list[str]:
    """Separate section entries with optional splits separated by comma into a list.

    :param filter_entries: string with the entries separated by comma
    :return: list of entries
    """
    text = filter_entries.strip()
    if not text:
        return []

    entries = []
    for entry in text.split(","):
        entry = entry.strip()
        if not entry:
            continue
        entries.append(entry.upper())
    return entries

def expand_values(raw_value: str, known_values: list[str]) -> set[str]:
    """Expand ranges, colon, dash, space-separated values.

    'ANY' expands to known_values if given.
    :param raw_value: string with the values to expand
    :param known_values: list of known valuses to expand 'ANY' to
    :return: set of expanded values
    """
    set_known_values: set[str] = set(known_values) if known_values else set()

    if raw_value is None:
        return set_known_values

    value = str(raw_value).strip().upper()
    if not value or value == "ANY":
        return set_known_values

    expanded_values: set[str] = set()
    for token in value.split():
        if "-" in token or ":" in token:
            sep = "-" if "-" in token else ":"
            start, end = token.split(sep, 1)
            expanded_values.update(str(i) for i in range(int(start), int(end) + 1))
        else:
            expanded_values.add(token)
    return expanded_values


def apply_job_filters(
    job_list,  # should be type JobList, avoid circular imports
    base_job_names: set[str],
    filter_section: Optional[str],
    filter_chunk: Optional[str],
    filter_status: Optional[str],
    filter_list: Optional[str],
    filter_sections_splits_fn: Callable,
    filter_chunks_fn: Callable,
    status_from_str_fn: Callable,
) -> set[str]:
    """Apply filters and return selected job names.

    All provided filters are combined using intersection (AND). Jobs must match all filters.
    :param job_list: job list object
    :type job_list: JobList
    :param base_job_names: set of job names before filtering
    :type base_job_names: set[str]
    :param filter_section: section filter
    :type filter_section: Optional[str]
    :param filter_chunk: chunk filter
    :type filter_chunk: Optional[str]
    :param filter_status: status filter
    :type filter_status: Optional[str]
    :param filter_list: list filter
    :type filter_list: Optional[str]
    :param filter_sections_splits_fn: function to filter sections and splits
    :type filter_sections_splits_fn: Callable
    :param filter_chunks_fn: function to filter chunks
    :type filter_chunks_fn: Callable
    :param status_from_str_fn: function to convert status from string
    :type status_from_str_fn: Callable
    :return: set of selected job names
    :rtype: set[str]
    """
    jobs_scope = job_list.get_job_list()
    selected_job_names = set(base_job_names)

    if filter_section:
        ft_entries = separate_section_entries(filter_section)
        if not (len(ft_entries) == 1 and ft_entries[0].upper() == "ANY"):
            section_filtered_jobs = filter_sections_splits_fn(ft_entries, jobs_scope)
            selected_job_names &= {
                job.name for job in jobs_scope if job in section_filtered_jobs
            }

    if filter_chunk:
        chunk_filtered_jobs = filter_chunks_fn(job_list, filter_chunk)
        selected_job_names &= {job.name for job in chunk_filtered_jobs}

    if filter_status:
        status_list = filter_status.split()
        if not (len(status_list) == 1 and status_list[0].upper() == "ANY"):
            allowed_statuses = {status_from_str_fn(s) for s in status_list}
            selected_job_names &= {
                job.name for job in jobs_scope if job.status in allowed_statuses
            }

    if filter_list:
        jobs = filter_list.split()
        if not (len(jobs) == 1 and jobs[0].upper() == "ANY"):
            selected_job_names &= {job.name for job in jobs_scope if job.name in jobs}

    return selected_job_names
