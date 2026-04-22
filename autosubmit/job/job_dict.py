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

import datetime
import re

from bscearth.utils.date import date2str

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_utils import calendar_chunk_section
from autosubmit.log.log import AutosubmitCritical


class DicJobs:
    """
    Class to create and build jobs from conf file and to find jobs by start date, member and chunk

    :param date_list: start dates
    :type date_list: list
    :param member_list: members
    :type member_list: list
    :param chunk_list chunks
    :type chunk_list: list
    :param date_format: H/M/D (hour, month, day)
    :type date_format: str
    :param default_retrials: 0 by default
    :type default_retrials: int
    :param as_conf: Comes from config parser, contains all experiment yml info
    :type as_conf: as_conf
    """

    def __init__(self, date_list, member_list, chunk_list, date_format, default_retrials, as_conf):
        self._date_list = date_list
        self._member_list = member_list
        self._chunk_list = chunk_list
        self._date_format = date_format
        self.default_retrials = default_retrials
        self._dic = dict()
        self.as_conf = as_conf
        self.experiment_data = as_conf.experiment_data
        self.recreate_jobs = False
        self.changes = {}
        self._job_list = {}

    @property
    def job_list(self):
        return self._job_list

    @job_list.setter
    def job_list(self, job_list):
        self._job_list = {job.name: job for job in job_list}

    def read_section(self, section, priority, default_job_type):
        """
        Read a section from jobs conf and creates all jobs for it

        :param default_job_type: default type for jobs
        :type default_job_type: str
        :param section: section to read, and it's info
        :type section: tuple(str,dict)
        :param priority: priority for the jobs
        :type priority: int
        """
        parameters = self.experiment_data["JOBS"]
        splits = parameters[section].get("SPLITS", -1)
        running = str(parameters[section].get('RUNNING', "once")).lower()

        if splits == "auto" and running != "chunk":
            raise AutosubmitCritical("SPLITS=auto is only allowed for running=chunk")
        elif splits != "auto":
            splits = int(splits)
        frequency = int(parameters[section].get("FREQUENCY", 1))
        if running == 'once':
            self._create_jobs_once(section, priority, default_job_type, splits)
        elif running == 'date':
            self._create_jobs_startdate(section, priority, frequency, default_job_type, splits)
        elif running == 'member':
            self._create_jobs_member(section, priority, frequency, default_job_type, splits)
        elif running == 'chunk':
            synchronize = str(parameters[section].get("SYNCHRONIZE", ""))
            delay = int(parameters[section].get("DELAY", -1))
            self._create_jobs_chunk(section, priority, frequency, default_job_type, synchronize, delay, splits)

    def _create_jobs_startdate(self, section, priority, frequency, default_job_type, splits=-1):
        """
        Create jobs to be run once per start date

        :param section: section to read
        :type section: str
        :param priority: priority for the jobs
        :type priority: int
        :param frequency: if greater than 1, only creates one job each frequency startdates. Always creates one job
                          for the last
        :type frequency: int
        """
        self._dic[section] = dict()
        count = 0
        for date in self._date_list:
            count += 1
            if count % frequency == 0 or count == len(self._date_list):
                self._dic[section][date] = []
                self._create_jobs_split(splits, section, date, None, None, priority, default_job_type,
                                        self._dic[section][date])

    def _create_jobs_member(self, section, priority, frequency, default_job_type, splits=-1):
        """
        Create jobs to be run once per member

        :param section: section to read
        :type section: str
        :param priority: priority for the jobs
        :type priority: int
        :param frequency: if greater than 1, only creates one job each frequency members. Always creates one job
                          for the last
        :type frequency: int
        :type excluded_members: list
        :param excluded_members: if member index is listed there, the job won't run for this member.

        """
        self._dic[section] = dict()
        for date in self._date_list:
            self._dic[section][date] = dict()
            count = 0
            for member in self._member_list:
                count += 1
                if count % frequency == 0 or count == len(self._member_list):
                    self._dic[section][date][member] = []
                    self._create_jobs_split(splits, section, date, member, None, priority, default_job_type,
                                            self._dic[section][date][member])

    def _create_jobs_once(self, section, priority, default_job_type, splits=0):
        """
        Create jobs to be run once

        :param section: section to read
        :type section: str
        :param priority: priority for the jobs
        :type priority: int
        """
        self._dic[section] = []
        self._create_jobs_split(splits, section, None, None, None, priority, default_job_type, self._dic[section])

    def _create_jobs_chunk(self, section, priority, frequency, default_job_type, synchronize=None, delay=0, splits=0):
        """
        Create jobs to be run once per chunk

        :param synchronize:
        :param section: section to read
        :type section: str
        :param priority: priority for the jobs
        :type priority: int
        :param frequency: if greater than 1, only creates one job each frequency chunks. Always creates one job
                          for the last
        :type frequency: int
        :param delay: if this parameter is set, the job is only created for the chunks greater than the delay
        :type delay: int
        """
        self._dic[section] = dict()
        # Temporally creation for unified jobs in case of synchronize
        tmp_dic = dict()
        if synchronize is not None and len(str(synchronize)) > 0:
            count = 0
            for chunk in self._chunk_list:
                count += 1
                if delay == -1 or delay < chunk:
                    if count % frequency == 0 or count == len(self._chunk_list):
                        if synchronize == 'date':
                            tmp_dic[chunk] = []
                            self._create_jobs_split(splits, section, None, None, chunk, priority,
                                                    default_job_type, tmp_dic[chunk])
                        elif synchronize == 'member':
                            tmp_dic[chunk] = dict()
                            for date in self._date_list:
                                tmp_dic[chunk][date] = []
                                self._create_jobs_split(splits, section, date, None, chunk, priority,
                                                        default_job_type, tmp_dic[chunk][date])
        # Real dic jobs assignment/creation
        for date in self._date_list:
            self._dic[section][date] = dict()
            for member in (member for member in self._member_list):
                self._dic[section][date][member] = dict()
                count = 0
                for chunk in (chunk for chunk in self._chunk_list):
                    if splits == "auto":
                        real_splits = calendar_chunk_section(self.experiment_data, section, date, chunk)
                    else:
                        real_splits = splits
                    count += 1
                    if delay == -1 or delay < chunk:
                        if count % frequency == 0 or count == len(self._chunk_list):
                            if synchronize == 'date':
                                if chunk in tmp_dic:
                                    self._dic[section][date][member][chunk] = tmp_dic[chunk]
                            elif synchronize == 'member':
                                if chunk in tmp_dic:
                                    self._dic[section][date][member][chunk] = tmp_dic[chunk][date]
                            else:
                                self._dic[section][date][member][chunk] = []
                                self._create_jobs_split(real_splits, section, date, member, chunk, priority,
                                                        default_job_type,
                                                        self._dic[section][date][member][chunk])

    def _create_jobs_split(self, splits, section, date, member, chunk, priority, default_job_type, section_data):
        splits_list = [-1] if splits <= 0 else range(1, splits + 1)
        for split in splits_list:
            self.build_job(section, priority, date, member, chunk, default_job_type, section_data, splits, split)

    def update_jobs_filtered(self, current_jobs, next_level_jobs):
        if type(next_level_jobs) is dict:
            for key in next_level_jobs.keys():
                if key not in current_jobs:
                    current_jobs[key] = next_level_jobs[key]
                else:
                    current_jobs[key] = self.update_jobs_filtered(current_jobs[key], next_level_jobs[key])
        elif type(next_level_jobs) is list:
            current_jobs.extend(next_level_jobs)
        else:
            current_jobs.append(next_level_jobs)
        return current_jobs

    def _collect_by_keys(self, jobs: dict, keys, final_jobs_list: list, jobs_aux: dict) -> dict:
        """Collect jobs from ``jobs`` for the given ``keys`` into the appropriate output.

        Flat jobs (list or Job) are appended to ``final_jobs_list``; nested dicts are merged
        into ``jobs_aux`` via `update_jobs_filtered`.

        :param jobs: Current level of the job dict.
        :param keys: Iterable of keys to look up in ``jobs``.
        :param final_jobs_list: Accumulator for resolved Job objects.
        :param jobs_aux: Accumulator for unresolved nested dicts.
        :return: Updated ``jobs_aux``.
        """
        for key in keys:
            value = jobs.get(key, None)
            if value is None:
                continue
            if type(value) is list:
                final_jobs_list.extend(value)
            elif type(value) is Job:
                final_jobs_list.append(value)
            elif type(value) is dict:
                jobs_aux = self.update_jobs_filtered(jobs_aux, value)
        return jobs_aux

    def _filter_level(
        self,
        jobs: dict,
        filter_value: str,
        explicit_keys,
        natural_key,
        use_all_keys: bool,
        final_jobs_list: list,
    ) -> dict:
        """Apply a single DATES_TO / MEMBERS_TO / CHUNKS_TO filter and return the next-level dict.

        The four sub-cases are: ``none`` (skip all), ``all`` (take every key), an explicit key
        list, and the natural-key fallback (either all keys when ``use_all_keys`` is True, or just
        ``natural_key``).

        :param jobs: Current level of the job dict (already key-normalized where needed).
        :param filter_value: Raw filter string (e.g. ``"all"``, ``"none"``, ``"20000101,20000201"``).
        :param explicit_keys: Pre-parsed, typed keys derived from ``filter_value`` when it is neither
            ``"none"`` nor ``"all"``.
        :param natural_key: The job's own key at this level (date, member, or chunk).
        :param use_all_keys: When True and no filter is set, iterate every key (e.g. ``running==once``).
        :param final_jobs_list: Accumulator for resolved Job objects; mutated in place.
        :return: The merged nested dict for the next traversal level (empty when this level is terminal).
        """
        jobs_aux: dict = {}
        if filter_value:
            filter_lower = filter_value.lower()
            if "none" in filter_lower:
                return {}
            elif "all" in filter_lower:
                return self._collect_by_keys(jobs, jobs.keys(), final_jobs_list, jobs_aux)
            else:
                return self._collect_by_keys(jobs, explicit_keys, final_jobs_list, jobs_aux)
        else:
            if use_all_keys:
                return self._collect_by_keys(jobs, jobs.keys(), final_jobs_list, jobs_aux)
            elif natural_key is not None and jobs.get(natural_key, None):
                return self._collect_by_keys(jobs, [natural_key], final_jobs_list, jobs_aux)
            return {}

    def _apply_splits_filter(
        self,
        final_jobs_list: list,
        job: "Job",
        split_filter: str,
        filters_to_of_parent: dict,
    ) -> list:
        """Filter ``final_jobs_list`` according to the SPLITS_TO expression.

        Handles five modes: ``none`` (no-split jobs only), ``all`` (everything),
        ``previous`` or ``previous-N`` (N splits back, default 1),
        numbered/``natural`` splits, 1-to-1 ``*`` mapping, and grouped ``*\\N``
        N-to-1 / 1-to-N mapping.

        :param final_jobs_list: Candidate jobs before splits filtering.
        :param job: The current job whose split number drives natural/previous resolution.
        :param split_filter: Raw SPLITS_TO string value.
        :param filters_to_of_parent: The parent's filters_to dict, used to detect ``previous``
            in the parent's SPLITS_TO for grouped-mapping edge cases.
        :return: Filtered list of Job objects.
        """
        parts = split_filter.split(",")
        one_to_one_splits = ",".join(s for s in parts if "*" in s).lower()
        normal_splits = ",".join(s for s in parts if "*" not in s).lower()

        skip_one_to_one = False
        if "none" in normal_splits:
            result_normal = [
                f for f in final_jobs_list
                if (f.split is None or f.split in (-1, 0)) and f.name != job.name
            ]
            skip_one_to_one = True
        elif "all" in normal_splits:
            result_normal = final_jobs_list
            skip_one_to_one = True
        elif "previous" in normal_splits:
            previous_match = re.search(r"previous(?:-(\d+))?", normal_splits)
            steps_back = int(previous_match.group(1)) if previous_match and previous_match.group(1) else 1
            result_normal = [
                f for f in final_jobs_list
                if job.split is not None and job.split > steps_back
                and f.split == job.split - steps_back and f.name != job.name
            ]
            skip_one_to_one = True
        elif not normal_splits:
            result_normal = []
        else:
            resolved = normal_splits.replace("natural", str(job.split))
            resolved_set = resolved.split(",")
            result_normal = [
                f for f in final_jobs_list
                if (f.split is None or f.split in (-1, 0) or str(f.split) in resolved_set)
                and f.name != job.name
            ]

        result_special: list = []
        if "*" in one_to_one_splits and not skip_one_to_one:
            if not final_jobs_list:
                return list(set(result_normal))
            easier_to_filter = "," + one_to_one_splits + ","
            slice_matches = re.findall(r"\\[0-9]+", easier_to_filter)
            if slice_matches:  # N-to-1 or 1-to-N grouped mapping (*\N)
                split_slice = int(slice_matches[0].split("\\")[1])
                is_n_to_1 = int(job.splits) <= int(final_jobs_list[0].splits)
                if is_n_to_1:
                    split_index = (job.split - 1) * split_slice
                    end = min(split_index + split_slice, len(final_jobs_list))
                    result_special = final_jobs_list[split_index:end]
                    if "previous" in filters_to_of_parent.get("SPLITS_TO", ""):
                        result_special = [result_special[-1]] if result_special else []
                else:
                    parent_index = min((job.split - 1) // split_slice, len(final_jobs_list) - 1)
                    result_special = final_jobs_list[parent_index] if final_jobs_list else []
                    if "previous" in filters_to_of_parent.get("SPLITS_TO", ""):
                        if type(result_special) is list:
                            result_special = [result_special[-1]] if result_special else []
            else:  # 1-to-1 mapping (*)
                split_index = job.split - 1
                result_special = [final_jobs_list[split_index]] if split_index < len(final_jobs_list) else []

        if type(result_special) is not list:
            result_special = [result_special]
        if type(result_normal) is not list:
            result_normal = [result_normal]
        return list(set(result_normal + result_special))

    def get_jobs_filtered(self, section, job, filters_to, natural_date, natural_member, natural_chunk,
                          filters_to_of_parent):
        """Return jobs for ``section`` that match the given dependency filters.

        Traverses the internal job dict level by level (date → member → chunk) applying the
        DATES_TO, MEMBERS_TO, and CHUNKS_TO filters, then applies SPLITS_TO on the resulting list.

        :param section: Section name to look up in the internal dict.
        :param job: The requesting job; its date/member/chunk/split drive natural-key resolution.
        :param filters_to: Dependency filter dict (DATES_TO, MEMBERS_TO, CHUNKS_TO, SPLITS_TO).
        :param natural_date: The date key that matches ``job.date`` in the dict.
        :param natural_member: The member key that matches ``job.member`` in the dict.
        :param natural_chunk: The chunk key that matches ``job.chunk`` in the dict.
        :param filters_to_of_parent: The parent's filters_to dict, forwarded to split-filter logic.
        :return: Deduplicated list of matching Job objects.
        """
        jobs = self._dic.get(section, {})
        final_jobs_list: list = []

        # date
        if len(jobs) > 0:
            if type(jobs) is list:
                final_jobs_list.extend(jobs)
                jobs = {}
            else:
                date_filter = filters_to.get("DATES_TO", None)
                explicit_dates = (
                    [datetime.datetime.strptime(d, "%Y%m%d") for d in date_filter.split(",")]
                    if date_filter and "none" not in date_filter.lower() and "all" not in date_filter.lower()
                    else []
                )
                jobs = self._filter_level(
                    jobs, date_filter, explicit_dates, natural_date,
                    use_all_keys=(job.running == "once"),
                    final_jobs_list=final_jobs_list,
                )

        # member
        if len(jobs) > 0:
            if type(jobs) is list:
                final_jobs_list.extend(jobs)
                jobs = {}
            else:
                # Normalize member keys to uppercase.
                jobs = {k.upper(): v for k, v in jobs.items()}
                member_filter = filters_to.get("MEMBERS_TO", None)
                explicit_members = (
                    [m.strip().upper() for m in member_filter.split(",")]
                    if member_filter and "none" not in member_filter.lower() and "all" not in member_filter.lower()
                    else []
                )
                natural_member_key = natural_member.upper() if natural_member else None
                jobs = self._filter_level(
                    jobs, member_filter, explicit_members, natural_member_key,
                    use_all_keys=(job.running == "once" or not job.member),
                    final_jobs_list=final_jobs_list,
                )

        # chunk
        if len(jobs) > 0:
            if type(jobs) is list:
                final_jobs_list.extend(jobs)
                jobs = {}
            else:
                chunk_filter = filters_to.get("CHUNKS_TO", None)
                explicit_chunks = (
                    [int(c.strip()) for c in chunk_filter.split(",")]
                    if chunk_filter and "none" not in chunk_filter.lower() and "all" not in chunk_filter.lower()
                    else []
                )
                jobs = self._filter_level(
                    jobs, chunk_filter, explicit_chunks, natural_chunk,
                    use_all_keys=(job.running == "once" or not job.chunk),
                    final_jobs_list=final_jobs_list,
                )
                # Remaining jobs at chunk level are always flat (list or Job).
                if len(jobs) > 0 and not isinstance(jobs, dict):
                    final_jobs_list.extend(jobs if type(jobs) is list else [jobs])

        #  splits
        split_filter = filters_to.get("SPLITS_TO", None)
        if final_jobs_list and split_filter:
            final_jobs_list = self._apply_splits_filter(
                final_jobs_list, job, split_filter, filters_to_of_parent
            )

        if type(final_jobs_list) is not list:
            return [final_jobs_list]
        return list(set(final_jobs_list))

    def get_jobs(self, section, date=None, member=None, chunk=None, sort_string=False):
        """
        Return all the jobs matching section, date, member and chunk provided. If any parameter is none, returns all
        the jobs without checking that parameter value. If a job has one parameter to None, is returned if all the
        others match parameters passed

        :param section: section to return
        :type section: str
        :param date: stardate to return
        :type date: str
        :param member: member to return
        :type member: str
        :param chunk: chunk to return
        :type chunk: int
        :return: jobs matching parameters passed
        :rtype: list
        """
        jobs = list()

        if section not in self._dic:
            return jobs

        dic = self._dic[section]
        # once jobs
        if type(dic) is list:
            jobs = dic
        elif type(dic) is not dict:
            jobs.append(dic)
        else:
            if date is not None and len(str(date)) > 0:
                self._get_date(jobs, dic, date, member, chunk)
            else:
                for d in self._date_list:
                    self._get_date(jobs, dic, d, member, chunk)
        if len(jobs) > 0 and isinstance(jobs[0], list):
            try:
                jobs_flattened = [job for jobs_to_flatten in jobs for job in jobs_to_flatten]
                jobs = jobs_flattened
            except TypeError:
                pass
        if sort_string:
            # I want to have first chunks then member then date to easily filter later on
            if len(jobs) > 0:
                if jobs[0].chunk is not None:
                    jobs = sorted(jobs, key=lambda x: x.chunk)
                elif jobs[0].member is not None:
                    jobs = sorted(jobs, key=lambda x: x.member)
                elif jobs[0].date is not None:
                    jobs = sorted(jobs, key=lambda x: x.date)

        return jobs

    def _get_date(self, jobs, dic, date, member, chunk):
        if date not in dic:
            return jobs
        dic = dic[date]
        if type(dic) is list:
            for job in dic:
                jobs.append(job)
        elif type(dic) is not dict:
            jobs.append(dic)
        else:
            if member is not None and len(str(member)) > 0:
                self._get_member(jobs, dic, member, chunk)
            else:
                for m in self._member_list:
                    self._get_member(jobs, dic, m, chunk)

        return jobs

    def _get_member(self, jobs, dic, member, chunk):
        if member not in dic:
            return jobs
        dic = dic[member]
        if type(dic) is not dict:
            jobs.append(dic)
        else:
            if chunk is not None and len(str(chunk)) > 0:
                if chunk in dic:
                    jobs.append(dic[chunk])
            else:
                for c in self._chunk_list:
                    if c not in dic:
                        continue
                    jobs.append(dic[c])
        return jobs

    def build_job(self, section, priority, date, member, chunk, default_job_type, section_data, splits=1, split=-1):
        name = self.experiment_data.get("DEFAULT", {}).get("EXPID", "")
        if date:
            name += "_" + date2str(date, self._date_format)
        if member:
            name += "_" + member
        if chunk:
            name += "_{0}".format(chunk)
        if split > 0:
            name += "_{0}".format(split)
        name += "_" + section
        if not self._job_list.get(name, None):
            job = Job(name, 0, Status.WAITING, priority)
            job.type = default_job_type
            job.section = section
            job.date = date
            job.date_format = self._date_format
            job.member = member
            job.chunk = chunk
            job.split = split
            job.splits = splits
        else:
            job = Job(loaded_data=self._job_list[name])

        self.changes["NEWJOBS"] = True
        # job.adjust_loaded_parameters()
        job.update_dict_parameters(self.as_conf)
        section_data.append(job)
