#!/usr/bin/env python3
import copy

# Copyright 2017-2020 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.


from bscearth.utils.date import date2str

from autosubmit.job.job import Job
from autosubmit.job.job_utils import get_split_size_unit, get_split_size, calendar_chunk_section
from autosubmit.job.job_common import Status
import datetime

import re
from log.log import AutosubmitCritical


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

    def get_jobs_filtered(self, section, job, filters_to, natural_date, natural_member, natural_chunk,
                          filters_to_of_parent):
        #  datetime.strptime("20020201", "%Y%m%d")
        jobs = self._dic.get(section, {})
        final_jobs_list = []
        # values replace original dict
        jobs_aux = {}
        if len(jobs) > 0:
            if type(jobs) is list:
                final_jobs_list.extend(jobs)
                jobs = {}
            else:
                if filters_to.get('DATES_TO', None):
                    if "none" in filters_to['DATES_TO'].lower():
                        jobs_aux = {}
                    elif "all" in filters_to['DATES_TO'].lower():
                        for date in jobs.keys():
                            if jobs.get(date, None):
                                if type(jobs.get(date, None)) is list:
                                    for aux_job in jobs[date]:
                                        final_jobs_list.append(aux_job)
                                elif type(jobs.get(date, None)) is Job:
                                    final_jobs_list.append(jobs[date])
                                elif type(jobs.get(date, None)) is dict:
                                    jobs_aux = self.update_jobs_filtered(jobs_aux, jobs[date])
                    else:
                        for date in filters_to.get('DATES_TO', "").split(","):
                            if jobs.get(datetime.datetime.strptime(date, "%Y%m%d"), None):
                                if type(jobs.get(datetime.datetime.strptime(date, "%Y%m%d"), None)) is list:
                                    for aux_job in jobs[datetime.datetime.strptime(date, "%Y%m%d")]:
                                        final_jobs_list.append(aux_job)
                                elif type(jobs.get(datetime.datetime.strptime(date, "%Y%m%d"), None)) is Job:
                                    final_jobs_list.append(jobs[datetime.datetime.strptime(date, "%Y%m%d")])
                                elif type(jobs.get(datetime.datetime.strptime(date, "%Y%m%d"), None)) is dict:
                                    jobs_aux = self.update_jobs_filtered(jobs_aux, jobs[
                                        datetime.datetime.strptime(date, "%Y%m%d")])
                else:
                    if job.running == "once":
                        for key in jobs.keys():
                            if type(jobs.get(key, None)) is list:  # TODO
                                for aux_job in jobs[key]:
                                    final_jobs_list.append(aux_job)
                            elif type(jobs.get(key, None)) is Job:  # TODO
                                final_jobs_list.append(jobs[key])
                            elif type(jobs.get(key, None)) is dict:
                                jobs_aux = self.update_jobs_filtered(jobs_aux, jobs[key])
                    elif jobs.get(job.date, None):
                        if type(jobs.get(natural_date, None)) is list:  # TODO
                            for aux_job in jobs[natural_date]:
                                final_jobs_list.append(aux_job)
                        elif type(jobs.get(natural_date, None)) is Job:  # TODO
                            final_jobs_list.append(jobs[natural_date])
                        elif type(jobs.get(natural_date, None)) is dict:
                            jobs_aux = self.update_jobs_filtered(jobs_aux, jobs[natural_date])
                    else:
                        jobs_aux = {}
                jobs = jobs_aux
        if len(jobs) > 0:
            if type(jobs) is list:  # TODO check the other todo, maybe this is not necessary, https://earth.bsc.es/gitlab/es/autosubmit/-/merge_requests/387#note_243751
                final_jobs_list.extend(jobs)
                jobs = {}
            else:
                # pass keys to uppercase to normalize the member name as it can be whatever the user wants
                jobs = {k.upper(): v for k, v in jobs.items()}
                jobs_aux = {}
                if filters_to.get('MEMBERS_TO', None):
                    if "none" in filters_to['MEMBERS_TO'].lower():
                        jobs_aux = {}
                    elif "all" in filters_to['MEMBERS_TO'].lower():
                        for member in jobs.keys():
                            if jobs.get(member.upper(), None):
                                if type(jobs.get(member.upper(), None)) is list:
                                    for aux_job in jobs[member.upper()]:
                                        final_jobs_list.append(aux_job)
                                elif type(jobs.get(member.upper(), None)) is Job:
                                    final_jobs_list.append(jobs[member.upper()])
                                elif type(jobs.get(member.upper(), None)) is dict:
                                    jobs_aux = self.update_jobs_filtered(jobs_aux, jobs[member.upper()])

                    else:
                        for member in filters_to.get('MEMBERS_TO', "").split(","):
                            if jobs.get(member.upper(), None):
                                if type(jobs.get(member.upper(), None)) is list:
                                    for aux_job in jobs[member.upper()]:
                                        final_jobs_list.append(aux_job)
                                elif type(jobs.get(member.upper(), None)) is Job:
                                    final_jobs_list.append(jobs[member.upper()])
                                elif type(jobs.get(member.upper(), None)) is dict:
                                    jobs_aux = self.update_jobs_filtered(jobs_aux, jobs[member.upper()])
                else:
                    if job.running == "once" or not job.member:
                        for key in jobs.keys():
                            if type(jobs.get(key, None)) is list:
                                for aux_job in jobs[key.upper()]:
                                    final_jobs_list.append(aux_job)
                            elif type(jobs.get(key.upper(), None)) is Job:
                                final_jobs_list.append(jobs[key])
                            elif type(jobs.get(key.upper(), None)) is dict:
                                jobs_aux = self.update_jobs_filtered(jobs_aux, jobs[key.upper()])

                    elif jobs.get(job.member.upper(), None):
                        if type(jobs.get(natural_member.upper(), None)) is list:
                            for aux_job in jobs[natural_member.upper()]:
                                final_jobs_list.append(aux_job)
                        elif type(jobs.get(natural_member.upper(), None)) is Job:
                            final_jobs_list.append(jobs[natural_member.upper()])
                        elif type(jobs.get(natural_member.upper(), None)) is dict:
                            jobs_aux = self.update_jobs_filtered(jobs_aux, jobs[natural_member.upper()])
                    else:
                        jobs_aux = {}
                jobs = jobs_aux
        if len(jobs) > 0:
            if type(jobs) is list:
                final_jobs_list.extend(jobs)
            else:
                if filters_to.get('CHUNKS_TO', None):
                    if "none" in filters_to['CHUNKS_TO'].lower():
                        pass
                    elif "all" in filters_to['CHUNKS_TO'].lower():
                        for chunk in jobs.keys():
                            if type(jobs.get(chunk, None)) is list:
                                for aux_job in jobs[chunk]:
                                    final_jobs_list.append(aux_job)
                            elif type(jobs.get(chunk, None)) is Job:
                                final_jobs_list.append(jobs[chunk])
                    else:
                        for chunk in filters_to.get('CHUNKS_TO', "").split(","):
                            chunk = int(chunk)
                            if type(jobs.get(chunk, None)) is list:
                                for aux_job in jobs[chunk]:
                                    final_jobs_list.append(aux_job)
                            elif type(jobs.get(chunk, None)) is Job:
                                final_jobs_list.append(jobs[chunk])
                else:
                    if job.running == "once" or not job.chunk:
                        for chunk in jobs.keys():
                            if type(jobs.get(chunk, None)) is list:
                                final_jobs_list += [aux_job for aux_job in jobs[chunk]]
                            elif type(jobs.get(chunk, None)) is Job:
                                final_jobs_list.append(jobs[chunk])
                    elif jobs.get(job.chunk, None):
                        if type(jobs.get(natural_chunk, None)) is list:
                            final_jobs_list += [aux_job for aux_job in jobs[natural_chunk]]
                        elif type(jobs.get(natural_chunk, None)) is Job:
                            final_jobs_list.append(jobs[natural_chunk])

        if len(final_jobs_list) > 0:
            split_filter = filters_to.get("SPLITS_TO", None)
            if split_filter:
                split_filter = split_filter.split(",")
                one_to_one_splits = [split for split in split_filter if "*" in split]
                one_to_one_splits = ",".join(one_to_one_splits).lower()
                normal_splits = [split for split in split_filter if "*" not in split]
                normal_splits = ",".join(normal_splits).lower()
                skip_one_to_one = False
                if "none" in normal_splits:
                    final_jobs_list_normal = [f_job for f_job in final_jobs_list if (
                            f_job.split is None or f_job.split == -1 or f_job.split == 0) and f_job.name != job.name]
                    skip_one_to_one = True
                elif "all" in normal_splits:
                    final_jobs_list_normal = final_jobs_list
                    skip_one_to_one = True
                elif "previous" in normal_splits:
                    final_jobs_list_normal = [f_job for f_job in final_jobs_list if (
                            f_job.split is None or job.split is None or f_job.split == job.split - 1) and f_job.name != job.name]
                    skip_one_to_one = True
                else:
                    final_jobs_list_normal = [f_job for f_job in final_jobs_list if (
                            f_job.split is None or f_job.split == -1 or f_job.split == 0 or str(f_job.split) in
                            normal_splits.split(',')) and f_job.name != job.name]
                final_jobs_list_special = []
                if "*" in one_to_one_splits and not skip_one_to_one:
                    easier_to_filter = "," + one_to_one_splits + ","
                    matches = re.findall(rf"\\[0-9]+", easier_to_filter)
                    if len(matches) > 0:  # get *\\

                        split_slice = int(matches[0].split("\\")[1])
                        if int(job.splits) <= int(final_jobs_list[0].splits):  # get  N-1 ( child - parent )
                            # (parent) -> (child)
                            # 1 -> 1,2
                            # 2 -> 3,4
                            # 3 -> 5 # but 5 is not enough to make another group, so it must be included in the previous one ( did in part two )
                            matches = re.findall(rf",{(job.split - 1) * split_slice + 1}\*\\?[0-9]*,", easier_to_filter)
                        else:  # get 1-N ( child - parent )
                            # (parent) -> (child)
                            # 1,2 -> 1
                            # 3,4 -> 2
                            # 5 -> 3 # but 5 is not enough to make another group, so it must be included in the previous one
                            group = (job.split - 1) // split_slice + 1
                            matches = re.findall(rf",{group}\*\\?[0-9]*,", easier_to_filter)
                            if len(matches) == 0:
                                matches = re.findall(rf",{group - 1}\*\\?[0-9]*,", easier_to_filter)
                    else:  # get * (1-1)
                        split_slice = 1
                        # get current index 1-1
                        matches = re.findall(rf",{job.split}\*\\?[0-9]*,", easier_to_filter)
                    if len(matches) > 0:
                        if int(job.splits) <= int(final_jobs_list[0].splits):  # get 1-1,N-1 (part 1)
                            my_complete_slice = matches[0].strip(",").split("*")
                            split_index = int(my_complete_slice[0]) - 1
                            end = split_index + split_slice
                            if split_slice > 1:
                                if len(final_jobs_list) < end + split_slice:
                                    end = len(final_jobs_list)
                            final_jobs_list_special = final_jobs_list[split_index:end]
                            if "previous" in filters_to_of_parent.get("SPLITS_TO", ""):
                                if type(final_jobs_list_special) is list:
                                    final_jobs_list_special = [final_jobs_list_special[-1]]
                                else:
                                    final_jobs_list_special = final_jobs_list_special
                        else:  # get 1-N (part 2)
                            my_complete_slice = matches[0].strip(",").split("*")
                            split_index = int(my_complete_slice[0]) - 1
                            final_jobs_list_special = final_jobs_list[split_index]
                            if "previous" in filters_to_of_parent.get("SPLITS_TO", ""):
                                if type(final_jobs_list_special) is list:
                                    final_jobs_list_special = [final_jobs_list_special[-1]]
                                else:
                                    final_jobs_list_special = final_jobs_list_special
                    else:
                        final_jobs_list_special = []
                if type(final_jobs_list_special) is not list:
                    final_jobs_list_special = [final_jobs_list_special]
                if type(final_jobs_list_normal) is not list:
                    final_jobs_list_normal = [final_jobs_list_normal]
                final_jobs_list = list(set(final_jobs_list_normal + final_jobs_list_special))
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
            except TypeError as e:
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
