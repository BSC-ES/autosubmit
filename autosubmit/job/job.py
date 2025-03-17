#!/usr/bin/env python3

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

"""
Main module for Autosubmit. Only contains an interface class to all functionality implemented on Autosubmit
"""

from collections import OrderedDict
from pathlib import Path

from autosubmit.job import job_utils
import copy
import datetime
import json
import locale
import os
import re
import textwrap
import time
from bscearth.utils.date import date2str, parse_date, previous_day, chunk_end_date, chunk_start_date, Log, subs_dates
from functools import reduce
from threading import Thread
from time import sleep
from typing import List, Union, Dict, Any, Tuple

from autosubmit.helpers.parameters import autosubmit_parameter, autosubmit_parameters
from autosubmit.history.experiment_history import ExperimentHistory
from autosubmit.job.job_common import StatisticsSnippetBash, StatisticsSnippetPython
from autosubmit.job.job_common import StatisticsSnippetR, StatisticsSnippetEmpty
from autosubmit.job.job_common import Status, Type, increase_wallclock_by_chunk
from autosubmit.job.job_utils import get_job_package_code, get_split_size_unit, get_split_size
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter
from autosubmit.platforms.platform import Platform
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from log.log import Log, AutosubmitCritical

Log.get_logger("Autosubmit")

# A wrapper for encapsulate threads , TODO: Python 3+ to be replaced by the < from concurrent.futures >

EXCLUDED = ["_platform", "_children", "_parents", "submitter"]


def threaded(fn):
    def wrapper(*args, **kwargs):
        thread = Thread(target=fn, args=args, kwargs=kwargs)
        thread.name = "JOB_" + str(args[0].name)
        thread.start()
        return thread
    return wrapper


# This decorator contains groups of parameters, with each
# parameter described. This is only for parameters which
# are not properties of Job. Otherwise, please use the
# ``autosubmit_parameter`` (singular!) decorator for the
# ``@property`` annotated members. The variable groups
# are cumulative, so you can add to ``job``, for instance,
# in multiple files as long as the variable names are
# unique per group.
@autosubmit_parameters(
    parameters={
        'chunk': {
            'day_before': 'Day before the start date.',
            'chunk_end_in_days': 'Days passed from the start of the simulation until the end of the chunk.',
            'chunk_start_date': 'Chunk start date.',
            'chunk_start_year': 'Chunk start year.',
            'chunk_start_month': 'Chunk start month.',
            'chunk_start_day': 'Chunk start day.',
            'chunk_start_hour': 'Chunk start hour.',
            'chunk_end_date': 'Chunk end date.',
            'chunk_end_year': 'Chunk end year.',
            'chunk_end_month': 'Chunk end month.',
            'chunk_end_day': 'Chunk end day.',
            'chunk_end_hour': 'Chunk end hour.',
            'chunk_second_to_last_date': 'Chunk second to last date.',
            'chunk_second_to_last_year': 'Chunk second to last year.',
            'chunk_second_to_last_month': 'Chunk second to last month.',
            'chunk_second_to_last_day': 'Chunk second to last day.',
            'chunk_second_to_last_hour': 'Chunk second to last hour.',
            'prev': 'Days since start date at the chunk\'s start.',
            'chunk_first': 'True if the current chunk is the first, false otherwise.',
            'chunk_last': 'True if the current chunk is the last, false otherwise.',
            'run_days': 'Chunk length in days.',
            'notify_on': 'Determine the job statuses you want to be notified.'
        },
        'config': {
            'config.autosubmit_version': 'Current version of Autosubmit.',
            'config.totaljobs': 'Total number of jobs in the workflow.',
            'config.maxwaitingjobs': 'Maximum number of jobs permitted in the waiting status.'
        },
        'experiment': {
            'experiment.datelist': 'List of start dates',
            'experiment.calendar': 'Calendar used for the experiment. Can be standard or noleap.',
            'experiment.chunksize': 'Size of each chunk.',
            'experiment.numchunks': 'Number of chunks of the experiment.',
            'experiment.chunksizeunit': 'Unit of the chunk size. Can be hour, day, month, or year.',
            'experiment.members': 'List of members.'
        },
        'default': {
            'default.expid': 'Job experiment ID.',
            'default.hpcarch': 'Default HPC platform name.',
            'default.custom_config': 'Custom configuration location.',
        },
        'job': {
            'rootdir': 'Experiment folder path.',
            'projdir': 'Project folder path.',
            'nummembers': 'Number of members of the experiment.'
        },
        'project': {
            'project.project_type': 'Type of the project.',
            'project.project_destination': 'Folder to hold the project sources.'
        }
    }
)
class Job(object):
    """
    Class to handle all the tasks with Jobs at HPC.

    A job is created by default with a name, a jobid, a status and a type.
    It can have children and parents. The inheritance reflects the dependency between jobs.
    If Job2 must wait until Job1 is completed then Job2 is a child of Job1.
    Inversely Job1 is a parent of Job2
    """

    __slots__ = (
        'rerun_only', 'script_name_wrapper',
        'delay_end', 'wrapper_type', '_wrapper_queue',
        '_platform', '_queue', '_partition', 'retry_delay', '_section',
        '_wallclock', 'wchunkinc', '_tasks', '_nodes',
        '_threads', '_processors', '_memory', '_memory_per_task', '_chunk',
        '_member', 'date', 'date_split', '_splits', '_split', '_delay',
        '_frequency', '_synchronize', 'skippable', 'repacked', '_long_name',
        'date_format', 'type', '_name',
        'undefined_variables', 'log_retries', 'id',
        'file', 'additional_files', 'executable', '_local_logs',
        '_remote_logs', 'script_name', 'stat_file', '_status', 'prev_status', 'old_status',
        'new_status', 'priority', '_parents', '_children', '_fail_count', 'expid',
        'parameters', '_tmp_path', '_log_path', 'write_start', '_platform', 'check',
        'check_warnings', '_packed', 'hold', 'distance_weight', 'level', '_export',
        '_dependencies', 'running', 'start_time', 'ext_header_path', 'ext_tailer_path',
        'edge_info', 'total_jobs', 'max_waiting_jobs', 'exclusive', '_retrials',
        'current_checkpoint_step', 'max_checkpoint_step', 'reservation',
        'delete_when_edgeless', 'het', 'updated_log',
        'submit_time_timestamp', 'start_time_timestamp', 'finish_time_timestamp',
        '_script', '_log_recovery_retries', 'ready_date', 'wrapper_name',
        'is_wrapper', '_wallclock_in_seconds', '_notify_on', '_processors_per_node',
        'log_avaliable', 'ec_queue', 'platform_name', '_serial_platform',
        'submitter', '_shape', '_x11', '_x11_options', '_hyperthreading',
        '_scratch_free_space', '_delay_retrials', '_custom_directives',
        'end_time_timestamp', '_log_recovered', 'packed_during_building', 'workflow_commit'
    )

    def __setstate__(self, state):
        for slot, value in state.items():
            setattr(self, slot, value)

    def __getstate__(self):
        return dict([(k, getattr(self, k, None)) for k in self.__slots__ if k not in EXCLUDED])

    CHECK_ON_SUBMISSION = 'on_submission'

    # TODO
    # This is crashing the code
    # I added it for the assertions of unit testing... since job obj != job obj when it was saved & load
    # since it points to another section of the memory.
    # Unfortunately, this is crashing the code everywhere else

    # def __eq__(self, other):
    #     return self.name == other.name and self.id == other.id

    def __str__(self):
        return "{0} STATUS: {1}".format(self.name, self.status)

    def __repr__(self):
        return "{0} STATUS: {1}".format(self.name, self.status)

    def __init__(self, name=None, job_id=None, status=None, priority=None, loaded_data=None):

        if loaded_data:
            name = loaded_data['_name']
            job_id = loaded_data['id']
            status = loaded_data['_status']
            priority = loaded_data['priority']

        self.rerun_only = False
        self.script_name_wrapper = None
        self.delay_end = None
        self.wrapper_type = None
        self._wrapper_queue = None
        self._platform = None
        self._queue = None
        self._partition = None
        self.retry_delay = None
        #: (str): Type of the job, as given on job configuration file. (job: TASKTYPE)
        self._section = None # type: str
        self._wallclock = None # type: str
        self.wchunkinc = None
        self._tasks = None
        self._nodes = None
        self._threads = None
        self._processors = None
        self._memory = None
        self._memory_per_task = None
        self._chunk = None
        self._member = None
        self.date = None
        self.date_split = None
        self._splits = None
        self._split = None
        self._delay = None
        self._frequency = None
        self._synchronize = None
        self.skippable = False
        self.repacked = 0
        self._name = name
        self._long_name = None
        self.date_format = ''
        self.type = Type.BASH
        self.undefined_variables = None
        self.log_retries = 5
        self.id = job_id
        self.file = None
        self.additional_files = []
        self.executable = None
        self._local_logs = ('', '')
        self._remote_logs = ('', '')
        self.script_name = self.name + ".cmd"
        self.stat_file = self.script_name[:-4] + "_STAT_0"
        self._status = None
        self.status = status
        self.prev_status = status
        self.old_status = self.status
        self.new_status = status
        self.priority = priority
        self._parents = set()
        self._children = set()
        #: (int) Number of failed attempts to run this job. (FAIL_COUNT)
        self._fail_count = 0
        self.expid = name.split('_')[0] # type: str
        self._tmp_path = os.path.join(
            BasicConfig.LOCAL_ROOT_DIR, self.expid, BasicConfig.LOCAL_TMP_DIR)
        self._log_path = Path(f"{self._tmp_path}/LOG_{self.expid}")
        self.write_start = False
        self._platform = None
        self.check = 'true'
        self.check_warnings = False
        self.packed = False
        self.hold = False # type: bool
        self.distance_weight = 0
        self.level = 0
        self._export = "none"
        self._dependencies = []
        self.running = None
        self.start_time = None
        self.ext_header_path = None
        self.ext_tailer_path = None
        self.edge_info = dict()
        self.total_jobs = None
        self.max_waiting_jobs = None
        self.exclusive = ""
        self._retrials = 0
        # internal
        self.current_checkpoint_step = 0
        self.max_checkpoint_step = 0
        self.reservation = ""
        self.delete_when_edgeless = False
        # hetjobs
        self.het = None
        self.updated_log = False
        self._log_recovered = False
        self.log_recovered = False
        self.submit_time_timestamp = None # for wrappers, all jobs inside a wrapper are submitted at the same time
        self.start_time_timestamp = None
        self.finish_time_timestamp = None # for wrappers, with inner_retrials, the submission time should be the last finish_time of the previous retrial
        self._script = None # Inline code to be executed
        self._log_recovery_retries = None
        self.ready_date = None
        self.wrapper_name = None
        self.is_wrapper = False
        self._wallclock_in_seconds = None
        self._notify_on = None
        self._processors_per_node = None
        self.log_avaliable = None
        self.ec_queue = None
        self.platform_name = None
        self._serial_platform = None
        self.submitter = None
        self._shape = None
        self._x11 = None
        self._x11_options = None
        self._hyperthreading = None
        self._scratch_free_space = None
        self._delay_retrials = None
        self._custom_directives = None
        self.end_time_timestamp = None
        self.packed_during_building = False
        self.workflow_commit = None
        if loaded_data:
            self.__setstate__(loaded_data)
            self.status = Status.WAITING if self.status in [Status.DELAYED,
                                                            Status.PREPARED,
                                                            Status.READY] else \
                self.status

    def clean_attributes(self):
        if self.status == Status.FAILED and self.fail_count >= self.retrials:
            return None
        self.rerun_only = False
        self.script_name_wrapper = None
        self.delay_end = None
        self.wrapper_type = None
        self._wrapper_queue = None
        self._queue = None
        self._partition = None
        self.retry_delay = None
        self._wallclock = None
        self.wchunkinc = None
        self._tasks = None
        self._nodes = None
        self._threads = None
        self._processors = None
        self._memory = None
        self._memory_per_task = None
        self.undefined_variables = None
        self.executable = None
        self.packed = False
        self.hold = False
        self.export = None
        self.start_time = None
        self.total_jobs = None
        self.max_waiting_jobs = None
        self.exclusive = None
        self.current_checkpoint_step = None
        self.max_checkpoint_step = None
        self.reservation = None
        self.het = None
        self.updated_log = False
        self._script = None
        self._log_recovery_retries = None
        self.wrapper_name = None
        self.is_wrapper = False
        self._wallclock_in_seconds = None
        self._notify_on = None
        self._processors_per_node = None
        self._shape = None
        self._x11 = False
        self._x11_options = None
        self._hyperthreading = None
        self._scratch_free_space = None
        self._delay_retrials = None
        self._custom_directives = None
        self.packed_during_building = False
        # Tentative
        self.dependencies = None
        self.local_logs = None
        self.remote_logs = None
        self.script_name = None
        self.stat_file = None

    def _init_runtime_parameters(self):
        # hetjobs
        self.het = {'HETSIZE': 0}
        self._tasks = '0'
        self._nodes = ""
        self._threads = '1'
        self._processors = '1'
        self._memory = ''
        self._memory_per_task = ''
        self.start_time_timestamp = time.time()
        self.processors_per_node = ""
        self.script_name = self.name + ".cmd"
        self.stat_file = self.script_name[:-4] + "_STAT_"
        self.reservation = ""
        self.current_checkpoint_step = 0
        self.max_checkpoint_step = 0
        self.exclusive = ""
        self.export = ""
        self.local_logs = ('', '')
        self.remote_logs = ('', '')
        self.dependencies = ""
        self.packed_during_building = False
        self.packed = False

    @property
    def wallclock_in_seconds(self):
        return self._wallclock_in_seconds

    @property
    @autosubmit_parameter(name='x11')
    def x11(self):
        """Whether to use X11 forwarding"""
        return self._x11

    @x11.setter
    def x11(self, value):
        self._x11 = value

    @property
    @autosubmit_parameter(name='x11_options')
    def x11_options(self):
        """Allows to set salloc parameters for x11"""
        return self._x11_options
    @x11_options.setter
    def x11_options(self, value):
        self._x11_options = value

    @property
    @autosubmit_parameter(name='tasktype')
    def section(self):
        """Type of the job, as given on job configuration file."""
        return self._section

    @section.setter
    def section(self, value):
        self._section = value

    @property
    @autosubmit_parameter(name='jobname')
    def name(self):
        """Current job full name."""
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    @autosubmit_parameter(name='script')
    def script(self):
        """Allows to launch inline code instead of using the file parameter"""
        return self._script
    @script.setter
    def script(self, value):
        self._script = value

    @property
    @autosubmit_parameter(name='fail_count')
    def fail_count(self):
        """Number of failed attempts to run this job."""
        return self._fail_count

    @fail_count.setter
    def fail_count(self, value):
        self._fail_count = value

    @property
    @autosubmit_parameter(name='retrials')
    def retrials(self):
        """Max amount of retrials to run this job."""
        return self._retrials

    @retrials.setter
    def retrials(self, value):
        if value is not None:
            self._retrials = int(value)

    @property
    @autosubmit_parameter(name='checkpoint')
    def checkpoint(self):
        '''Generates a checkpoint step for this job based on job.type.'''
        if self.type == Type.PYTHON:
            return "checkpoint()"
        elif self.type == Type.R:
            return "checkpoint()"
        else:  # bash
            return "as_checkpoint"

    def get_checkpoint_files(self):
        """
        Check if there is a file on the remote host that contains the checkpoint
        """
        return self.platform.get_checkpoint_files(self)

    @property
    @autosubmit_parameter(name='sdate')
    def sdate(self):
        """Current start date."""
        return date2str(self.date, self.date_format)

    @property
    @autosubmit_parameter(name='member')
    def member(self):
        """Current member."""
        return self._member

    @member.setter
    def member(self, value):
        self._member = value

    @property
    @autosubmit_parameter(name='chunk')
    def chunk(self):
        """Current chunk."""
        return self._chunk

    @chunk.setter
    def chunk(self, value):
        self._chunk = value

    @property
    @autosubmit_parameter(name='split')
    def split(self):
        """Current split."""
        return self._split

    @split.setter
    def split(self, value):
        self._split = value

    @property
    @autosubmit_parameter(name='delay')
    def delay(self):
        """Current delay."""
        return self._delay

    @delay.setter
    def delay(self, value):
        self._delay = value

    @property
    @autosubmit_parameter(name='wallclock')
    def wallclock(self):
        """Duration for which nodes used by job will remain allocated."""
        return self._wallclock

    @wallclock.setter
    def wallclock(self, value):
        if value:
            self._wallclock = value
            if not self._wallclock_in_seconds or self.status not in [Status.RUNNING, Status.QUEUING, Status.SUBMITTED]:
                # Should always take the max_wallclock set in the platform, this is set as fallback
                # (and local platform doesn't have a max_wallclock defined)
                wallclock_parsed = self.parse_time(self._wallclock)
                self._wallclock_in_seconds = self._time_in_seconds_and_margin(wallclock_parsed)

    @property
    @autosubmit_parameter(name='hyperthreading')
    def hyperthreading(self):
        """Detects if hyperthreading is enabled or not."""
        return self._hyperthreading

    @hyperthreading.setter
    def hyperthreading(self, value):
        self._hyperthreading = value

    @property
    @autosubmit_parameter(name='nodes')
    def nodes(self):
        """Number of nodes that the job will use."""
        return self._nodes

    @nodes.setter
    def nodes(self, value):
        self._nodes = value

    @property
    @autosubmit_parameter(name=['numthreads', 'threads', 'cpus_per_task'])
    def threads(self):
        """Number of threads that the job will use."""
        return self._threads

    @threads.setter
    def threads(self, value):
        self._threads = value

    @property
    @autosubmit_parameter(name=['numtask', 'tasks', 'tasks_per_node'])
    def tasks(self):
        """Number of tasks that the job will use."""
        return self._tasks

    @tasks.setter
    def tasks(self, value):
        self._tasks = value

    @property
    @autosubmit_parameter(name='scratch_free_space')
    def scratch_free_space(self):
        """Percentage of free space required on the ``scratch``."""
        return self._scratch_free_space

    @scratch_free_space.setter
    def scratch_free_space(self, value):
        self._scratch_free_space = value

    @property
    @autosubmit_parameter(name='memory')
    def memory(self):
        """Memory requested for the job."""
        return self._memory

    @memory.setter
    def memory(self, value):
        self._memory = value

    @property
    @autosubmit_parameter(name='memory_per_task')
    def memory_per_task(self):
        """Memory requested per task."""
        return self._memory_per_task

    @memory_per_task.setter
    def memory_per_task(self, value):
        self._memory_per_task = value

    @property
    @autosubmit_parameter(name='frequency')
    def frequency(self):
        """TODO."""
        return self._frequency

    @frequency.setter
    def frequency(self, value):
        self._frequency = value

    @property
    @autosubmit_parameter(name='synchronize')
    def synchronize(self):
        """TODO."""
        return self._synchronize

    @synchronize.setter
    def synchronize(self, value):
        self._synchronize = value

    @property
    @autosubmit_parameter(name='dependencies')
    def dependencies(self):
        """Current job dependencies."""
        return self._dependencies

    @dependencies.setter
    def dependencies(self, value):
        self._dependencies = value

    @property
    @autosubmit_parameter(name='delay_retrials')
    def delay_retrials(self):
        """TODO"""
        return self._delay_retrials

    @delay_retrials.setter
    def delay_retrials(self, value):
        self._delay_retrials = value

    @property
    @autosubmit_parameter(name='packed')
    def packed(self):
        """TODO"""
        return self._packed

    @packed.setter
    def packed(self, value):
        self._packed = value

    @property
    @autosubmit_parameter(name='export')
    def export(self):
        """TODO."""
        return self._export

    @export.setter
    def export(self, value):
        self._export = value

    @property
    @autosubmit_parameter(name='custom_directives')
    def custom_directives(self):
        """List of custom directives."""
        return self._custom_directives

    @custom_directives.setter
    def custom_directives(self, value):
        self._custom_directives = value
    @property
    @autosubmit_parameter(name='splits')
    def splits(self):
        """Max number of splits."""
        return self._splits
    @splits.setter
    def splits(self, value):
        self._splits = value

    @property
    @autosubmit_parameter(name='notify_on')
    def notify_on(self):
        """Send mail notification on job status change."""
        return self._notify_on

    @notify_on.setter
    def notify_on(self, value):
        self._notify_on = value

    def read_header_tailer_script(self, script_path: str, as_conf: AutosubmitConfig, is_header: bool):
        """
        Opens and reads a script. If it is not a BASH script it will fail :(

        Will strip away the line with the hash bang (#!)

        :param script_path: relative to the experiment directory path to the script
        :param as_conf: Autosubmit configuration file
        :param is_header: boolean indicating if it is header extended script
        """
        if not script_path:
            return ''
        found_hashbang = False
        script_name = script_path.rsplit("/")[-1]  # pick the name of the script for a more verbose error
        # the value might be None string if the key has been set, but with no value
        if not script_name:
            return ''
        script = ''


        # adjusts the error message to the type of the script
        if is_header:
            error_message_type = "header"
        else:
            error_message_type = "tailer"

        try:
            # find the absolute path
            script_file = open(os.path.join(as_conf.get_project_dir(), script_path), 'r')
        except Exception as e:  # log
            # We stop Autosubmit if we don't find the script
            raise AutosubmitCritical("Extended {1} script: failed to fetch {0} \n".format(str(e),
                                                                                          error_message_type), 7014)

        for line in script_file:
            if line[:2] != "#!":
                script += line
            else:
                found_hashbang = True
                # check if the type of the script matches the one in the extended
                if "bash" in line:
                    if self.type != Type.BASH:
                        raise AutosubmitCritical(
                            "Extended {2} script: script {0} seems Bash but job {1} isn't\n".format(script_name,
                                                                                                    self.script_name,
                                                                                                    error_message_type),
                            7011)
                elif "Rscript" in line:
                    if self.type != Type.R:
                        raise AutosubmitCritical(
                            "Extended {2} script: script {0} seems Rscript but job {1} isn't\n".format(script_name,
                                                                                                       self.script_name,
                                                                                                       error_message_type),
                            7011)
                elif "python" in line:
                    if self.type not in (Type.PYTHON, Type.PYTHON2, Type.PYTHON3):
                        raise AutosubmitCritical(
                            "Extended {2} script: script {0} seems Python but job {1} isn't\n".format(script_name,
                                                                                                      self.script_name,
                                                                                                      error_message_type),
                            7011)
                else:
                    raise AutosubmitCritical(
                        "Extended {2} script: couldn't figure out script {0} type\n".format(script_name,
                                                                                           self.script_name,
                                                                                           error_message_type), 7011)

        if not found_hashbang:
            raise AutosubmitCritical(
                "Extended {2} script: couldn't figure out script {0} type\n".format(script_name,
                                                                                   self.script_name,
                                                                                   error_message_type), 7011)

        if is_header:
            script = "\n###############\n# Header script\n###############\n" + script
        else:
            script = "\n###############\n# Tailer script\n###############\n" + script

        return script

    @property
    def parents(self):
        """
        Returns parent jobs list

        :return: parent jobs
        :rtype: set
        """
        return self._parents

    @parents.setter
    def parents(self, parents):
        """
        Sets the parents job list
        """
        self._parents = parents

    @property
    @autosubmit_parameter(name='status')
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        """
        Sets the status of the job
        """
        self._status = status

    @property
    @autosubmit_parameter(name='log_recovered')
    def log_recovered(self):
        return self._log_recovered

    @log_recovered.setter
    def log_recovered(self, log_recovered):
        """
        Sets the log_recovered
        """
        self._log_recovered = log_recovered


    @property
    def status_str(self):
        """
        String representation of the current status
        """
        return Status.VALUE_TO_KEY.get(self.status, "UNKNOWN")

    @property
    def children_names_str(self):
        """
        Comma separated list of children's names
        """
        return ",".join([str(child.name) for child in self._children])

    @property
    def is_serial(self):
        return not self.nodes and (not self.processors or str(self.processors) == '1')

    @property
    def platform(self):
        """
        Returns the platform to be used by the job. Chooses between serial and parallel platforms

        :return: HPCPlatform object for the job to use
        :rtype: HPCPlatform
        """
        if self.is_serial and self._platform:
            return self._platform.serial_platform
        else:
            return self._platform

    @platform.setter
    def platform(self, value):
        """
        Sets the HPC platforms to be used by the job.

        :param value: platforms to set
        :type value: HPCPlatform
        """
        self._platform = value

    @property
    @autosubmit_parameter(name="current_queue")
    def queue(self):
        """
        Returns the queue to be used by the job. Chooses between serial and parallel platforms.

        :return HPCPlatform object for the job to use
        :rtype: HPCPlatform
        """
        if self._queue is not None and len(str(self._queue)) > 0:
            return self._queue
        if self.is_serial:
            return self._platform.serial_platform.serial_queue
        else:
            return self._platform.queue

    @queue.setter
    def queue(self, value):
        """
        Sets the queue to be used by the job.

        :param value: queue to set
        :type value: HPCPlatform
        """
        self._queue = value
    @property
    def partition(self):
        """
        Returns the queue to be used by the job. Chooses between serial and parallel platforms

        :return HPCPlatform object for the job to use
        :rtype: HPCPlatform
        """
        if self._partition is not None and len(str(self._partition)) > 0:
            return self._partition
        if self.is_serial:
            return self._platform.serial_platform.serial_partition
        else:
            return self._platform.partition

    @partition.setter
    def partition(self, value):
        """
        Sets the partion to be used by the job.

        :param value: partion to set
        :type value: HPCPlatform
        """
        self._partition = value

    @property
    def shape(self):
        """
        Returns the shape of the job. Chooses between serial and parallel platforms

        :return HPCPlatform object for the job to use
        :rtype: HPCPlatform
        """
        return self._shape

    @shape.setter
    def shape(self, value):
        """
        Sets the shape to be used by the job.

        :param value: shape to set
        :type value: HPCPlatform
        """
        self._shape = value

    @property
    def children(self):
        """
        Returns a list containing all children of the job

        :return: child jobs
        :rtype: set
        """
        return self._children

    @children.setter
    def children(self, children):
        """
        Sets the children job list
        """
        self._children = children

    @property
    def long_name(self):
        """
        Job's long name. If not set, returns name

        :return: long name
        :rtype: str
        """
        if hasattr(self, '_long_name'):
            return self._long_name
        else:
            return self.name

    @long_name.setter
    def long_name(self, value):
        """
        Sets long name for the job

        :param value: long name to set
        :type value: str
        """
        self._long_name = value

    @property
    def local_logs(self):
        return self._local_logs

    @local_logs.setter
    def local_logs(self, value):
        self._local_logs = value

    @property
    def remote_logs(self):
        return self._remote_logs

    @remote_logs.setter
    def remote_logs(self, value):
        self._remote_logs = value

    @property
    def total_processors(self):
        """
        Number of processors requested by job.
        Reduces ':' separated format  if necessary.
        """
        if ':' in str(self.processors):
            return reduce(lambda x, y: int(x) + int(y), self.processors.split(':'))
        elif self.processors == "" or self.processors == "1":
            if not self.nodes or int(self.nodes) <= 1:
                return 1
            else:
                return ""
        return int(self.processors)

    @property
    def total_wallclock(self):
        if self.wallclock:
            hours, minutes = self.wallclock.split(':')
            return float(minutes) / 60 + float(hours)
        return 0

    @property
    @autosubmit_parameter(name=['numproc', 'processors'])
    def processors(self):
        """Number of processors that the job will use."""
        return self._processors

    @processors.setter
    def processors(self, value):
        self._processors = value

    @property
    @autosubmit_parameter(name=['processors_per_node'])
    def processors_per_node(self):
        """Number of processors per node that the job can use."""
        return self._processors_per_node

    @processors_per_node.setter
    def processors_per_node(self, value):
        """Number of processors per node that the job can use."""
        self._processors_per_node = value

    def set_ready_date(self) -> None:
        """
        Sets the ready start date for the job
        """
        self.updated_log = False
        self.ready_date = int(time.strftime("%Y%m%d%H%M%S"))

    def inc_fail_count(self):
        """
        Increments fail count
        """
        self.fail_count += 1

    # Maybe should be renamed to the plural?
    def add_parent(self, *parents):
        """
        Add parents for the job. It also adds current job as a child for all the new parents

        :param parents: job's parents to add
        :type parents: Job
        """
        for parent in parents:
            num_parents = 1
            if isinstance(parent, list):
                num_parents = len(parent)
            for i in range(num_parents):
                new_parent = parent[i] if isinstance(parent, list) else parent
                self._parents.add(new_parent)
                new_parent.__add_child(self)

    def add_children(self, children):
        """
        Add children for the job. It also adds current job as a parent for all the new children

        :param children: job's children to add
        :type children: list of Job objects
        """
        for child in (child for child in children if child.name != self.name):
            self.__add_child(child)
            child._parents.add(self)
    def __add_child(self, new_child):
        """
        Adds a new child to the job

        :param new_child: new child to add
        :type new_child: Job
        """
        self.children.add(new_child)

    def add_edge_info(self, parent, special_conditions):
        """
        Adds edge information to the job

        :param parent: parent job
        :type parent: Job
        :param special_conditions: special variables
        :type special_conditions: dict
        """
        if special_conditions["STATUS"] not in self.edge_info:
            self.edge_info[special_conditions["STATUS"]] = {}

        self.edge_info[special_conditions["STATUS"]][parent.name] = (parent,special_conditions.get("FROM_STEP", 0))

    def delete_parent(self, parent):
        """
        Remove a parent from the job

        :param parent: parent to remove
        :type parent: Job
        """
        self.parents.remove(parent)

    def delete_child(self, child):
        """
        Removes a child from the job

        :param child: child to remove
        :type child: Job
        """
        # careful it is only possible to remove one child at a time
        self.children.remove(child)

    def has_children(self):
        """
        Returns true if job has any children, else return false

        :return: true if job has any children, otherwise return false
        :rtype: bool
        """
        return self.children.__len__()

    def has_parents(self):
        """
        Returns true if job has any parents, else return false

        :return: true if job has any parent, otherwise return false
        :rtype: bool
        """
        return self.parents.__len__()

    def _get_from_stat(self, index: int, fail_count: int =-1) -> int:
        """
        Returns value from given row index position in STAT file associated to job.

        Args:
            index (int): Row position to retrieve.
            fail_count (int, optional): Fail count to determine the STAT file name. Default to self.stat_file for non-wrapped jobs.

        Returns:
            int: Value in the index position, or 0 if the file or index does not exist.
        """
        if fail_count == -1:
            logname = os.path.join(self._tmp_path, self.stat_file)
        else:
            fail_count = str(fail_count)
            logname = os.path.join(self._tmp_path, self.name + '_STAT_' + fail_count)
        if os.path.exists(logname):
            lines = open(logname).readlines()
            if len(lines) >= index + 1:
                return int(lines[index])
            else:
                return 0
        else:
            return 0

    def _get_from_total_stats(self, index):
        """
        Returns list of values from given column index position in TOTAL_STATS file associated to job

        :param index: column position to retrieve
        :type index: int
        :return: list of values in column index position
        :rtype: list[datetime.datetime]
        """
        log_name = os.path.join(self._tmp_path, self.name + '_TOTAL_STATS')
        lst = []
        if os.path.exists(log_name):
            f = open(log_name)
            lines = f.readlines()
            for line in lines:
                fields = line.split()
                if len(fields) >= index + 1:
                    lst.append(parse_date(fields[index]))
        return lst

    def check_end_time(self, fail_count=-1):
        """
        Returns end time from stat file

        :return: date and time
        :rtype: str
        """
        return self._get_from_stat(1, fail_count)

    def check_start_time(self, fail_count=-1):
        """
        Returns job's start time

        :return: start time
        :rtype: str
        """
        return self._get_from_stat(0,fail_count)

    def check_retrials_end_time(self):
        """
        Returns list of end datetime for retrials from total stats file

        :return: date and time
        :rtype: list[int]
        """
        return self._get_from_total_stats(2)

    def check_retrials_start_time(self):
        """
        Returns list of start datetime for retrials from total stats file

        :return: date and time
        :rtype: list[int]
        """
        return self._get_from_total_stats(1)

    def get_last_retrials(self):
        # type: () -> List[Union[datetime.datetime, str]]
        """
        Returns the retrials of a job, including the last COMPLETED run. The selection stops, and does not include, when the previous COMPLETED job is located or the list of registers is exhausted.

        :return: list of dates of retrial [submit, start, finish] in datetime format
        :rtype: list of list
        """
        log_name = os.path.join(self._tmp_path, self.name + '_TOTAL_STATS')
        retrials_list = []
        if os.path.exists(log_name):
            already_completed = False
            # Read lines of the TOTAL_STATS file starting from last
            for retrial in reversed(open(log_name).readlines()):
                retrial_fields = retrial.split()
                if Job.is_a_completed_retrial(retrial_fields):
                    # It's a COMPLETED run
                    if already_completed:
                        break
                    already_completed = True
                retrial_dates = list(map(lambda y: parse_date(y) if y != 'COMPLETED' and y != 'FAILED' else y,
                                    retrial_fields))
                # Inserting list [submit, start, finish] of datetime at the beginning of the list. Restores ordering.
                retrials_list.insert(0, retrial_dates)
        return retrials_list

    def get_new_remotelog_name(self, count = -1):
        """
        Checks if remote log file exists on remote host
        if it exists, remote_log variable is updated
        :param
        """
        if count == -1:
            count = self._fail_count
        try:
            remote_logs = (f"{self.script_name}.out.{count}", f"{self.script_name}.err.{count}")
        except BaseException as e:
            remote_logs = ""
            Log.printlog(f"Trace {e} \n Failed to retrieve log file for job {self.name}", 6000)
        return remote_logs

    def check_remote_log_exists(self):
        try:
            out_exist = self.platform.check_file_exists(self.remote_logs[0], False, sleeptime=0, max_retries=1)
        except IOError:
            Log.debug(f'Output log {self.remote_logs[0]} still does not exist')
            out_exist = False
        try:
            err_exist = self.platform.check_file_exists(self.remote_logs[1], False, sleeptime=0, max_retries=1)
        except IOError:
            Log.debug(f'Error log {self.remote_logs[1]} still does not exist')
            err_exist = False
        return out_exist or err_exist

    def retrieve_external_retrials_logfiles(self):
        log_recovered = False
        self.remote_logs = self.get_new_remotelog_name()
        if not self.remote_logs:
            self.log_recovered = False
        else:
            if self.check_remote_log_exists():
                try:
                    self.synchronize_logs(self.platform, self.remote_logs, self.local_logs)
                    remote_logs = copy.deepcopy(self.local_logs)
                    self.platform.get_logs_files(self.expid, remote_logs)
                    log_recovered = True
                except BaseException:
                    log_recovered = False
        return log_recovered

    def retrieve_internal_retrials_logfiles(self) -> Tuple[int, bool]:
        """
        Retrieves internal retrials log files for the given platform.
        This function is used when the job is inside a vertical wrapper.

        Returns:
            int: The last retrial index where logs were successfully retrieved.
        """
        log_recovered = False
        last_retrial = 0
        try:
            for i in range(0, int(self.retrials + 1)):
                # Update local logs to give a name to the recovered log
                self.update_local_logs(count=i, update_submit_time=False)

                # Backup the remote log name in case that the log couldn't be recovered.
                backup_log = copy.copy(self.remote_logs)
                self.remote_logs = self.get_new_remotelog_name(i)
                if self.check_remote_log_exists():
                    self.synchronize_logs(self.platform, self.remote_logs, self.local_logs)
                    remote_logs = copy.deepcopy(self.local_logs)
                    self.platform.get_logs_files(self.expid, remote_logs)
                    log_recovered = True
                    last_retrial = i
                else:
                    self.remote_logs = backup_log
                    break
        except:
            pass

        if log_recovered:
            self.platform.processed_wrapper_logs.add(self.wrapper_name)

        return last_retrial, log_recovered

    def update_stat_file(self):
        self.stat_file = f"{self.script_name[:-4]}_STAT_"

    def write_stats(self, last_retrial: int) -> None:
        """

        Gathers the stat file, writes statistics into the job_data.db, and updates the total_stat file.
        Considers whether the job is a vertical wrapper and the number of retrials to gather.

        Args:
            last_retrial (int): The last retrial count.

        Returns:
            None

        """
        # Write stats for vertical wrappers
        if self.wrapper_type == "vertical":  # Disable AS retrials for vertical wrappers to use internal ones
            for i in range(0, int(last_retrial + 1)):
                self.platform.get_stat_file(self, count=i)
                self.write_vertical_time(i)
                self.inc_fail_count()

                # Update the logs with Autosubmit Job ID Brand
                try:
                    for local_log in self.local_logs:
                        self.platform.write_jobid(self.id, os.path.join(
                            self._tmp_path, 'LOG_' + str(self.expid), local_log))
                except BaseException as e:
                    Log.printlog("Trace {0} \n Failed to write the {1} e=6001".format(str(e), self.name))
        else:
            # Update local logs without updating the submit time
            self.update_local_logs(update_submit_time=False)
            self.platform.get_stat_file(self)
            self.write_submit_time()
            self.write_start_time()
            self.write_end_time(self.status == Status.COMPLETED)
            # Update the logs with Autosubmit Job ID Brand
            try:
                for local_log in self.local_logs:
                    self.platform.write_jobid(self.id, os.path.join(
                        self._tmp_path, 'LOG_' + str(self.expid), local_log))
            except BaseException as e:
                Log.printlog("Trace {0} \n Failed to write the {1} e=6001".format(str(e), self.name))

    def retrieve_logfiles(self, raise_error: bool = False) -> Dict[str, int]:
        """
        Retrieves log files from remote host.

        Args:
            raise_error (bool): If True, raises an error if the log files are not retrieved.

        Returns:
            Dict[str, int]: Dictionary with finish timestamps per job.
        """
        backup_logname = copy.copy(self.local_logs)
        if self.wrapper_type == "vertical":
            last_retrial, log_recovered = self.retrieve_internal_retrials_logfiles()
        else:
            log_recovered = self.retrieve_external_retrials_logfiles()
            last_retrial = 0
        if not log_recovered:
            self.local_logs = backup_logname
            if raise_error and self.wrapper_name not in self.platform.processed_wrapper_logs:
                raise AutosubmitCritical("Failed to retrieve logs for job {0}".format(self.name), 6000)
        else:
            self.write_stats(last_retrial)
            if self.wrapper_type == "vertical":
                for retrial in range(0, last_retrial + 1):
                    Log.result(f"{self.platform.name}(log_recovery) Successfully recovered log for job '{self.name}' and retry '{retrial}'.")
            else:
                Log.result(f"{self.platform.name}(log_recovery) Successfully recovered log for job '{self.name}' and retry '{self.fail_count}'.")
        self.log_recovered = log_recovered

    def _max_possible_wallclock(self):
        if self.platform and self.platform.max_wallclock:
            wallclock = self.parse_time(self.platform.max_wallclock)
            if wallclock:
                return int(wallclock.total_seconds())
        return None

    def _time_in_seconds_and_margin(self, wallclock: datetime.timedelta) -> int:
        """
        Calculate the total wallclock time in seconds and the wallclock time with a margin.

        This method increases the given wallclock time by 30%.
        It then converts the total wallclock time to seconds and returns both the total
        wallclock time in seconds and the wallclock time with the margin as a timedelta.

        :param wallclock: The original wallclock time.
        :type wallclock: datetime.timedelta

        :return int: The total wallclock time in seconds.
        """
        total = int(wallclock.total_seconds() * 1.30)
        total_platform = self._max_possible_wallclock()
        if not total_platform:
            total_platform = total
        if total > total_platform:
            Log.warning(f"Job {self.name} has a wallclock time '{total} seconds' higher than the maximum allowed by the platform '{total_platform} seconds' "
                        f"Setting wallclock time to the maximum allowed by the platform.")
            total = total_platform
        wallclock_delta = datetime.timedelta(seconds=total)
        return int(wallclock_delta.total_seconds())

    @staticmethod
    def parse_time(wallclock):
        if type(wallclock) != str:  # TODO This is a workaround for the time being, just defined for tests passing without more issues
            return datetime.timedelta(24*60*60)
        regex = re.compile(r'(((?P<hours>\d+):)((?P<minutes>\d+)))(:(?P<seconds>\d+))?')
        parts = regex.match(wallclock)
        if not parts:
            return None
        parts = parts.groupdict()
        time_params = {}
        for name, param in parts.items():
            if param:
                time_params[name] = int(param)
        return datetime.timedelta(**time_params)

    # TODO Duplicated for wrappers and jobs to fix in 4.1.X but in wrappers is called _is_over_wallclock for unknown reasons
    def is_over_wallclock(self):
        """
        Check if the job is over the wallclock time, it is an alternative method to avoid platform issues
        :return:
        """
        elapsed = datetime.datetime.now() - self.start_time
        if int(elapsed.total_seconds()) > self.wallclock_in_seconds:
            Log.warning(f"Job {self.name} is over wallclock time, Autosubmit will check if it is completed")
            return True
        return False

    def update_status(self, as_conf, failed_file=False):
        """
        Updates job status, checking COMPLETED file if needed

        :param as_conf:
        :param failed_file: boolean, if True, checks if the job failed
        :return:
        """
        self.log_avaliable = False
        previous_status = self.status
        self.prev_status = previous_status
        new_status = self.new_status
        if new_status == Status.COMPLETED:
            Log.debug(
                "{0} job seems to have completed: checking...".format(self.name))
            if not self._platform.get_completed_files(self.name, wrapper_failed=self.packed):
                log_name = os.path.join(
                    self._tmp_path, self.name + '_COMPLETED')

            self.check_completion()
        else:
            self.status = new_status
        if self.status == Status.RUNNING:
            Log.info("Job {0} is RUNNING", self.name)
        elif self.status == Status.QUEUING:
            Log.info("Job {0} is QUEUING", self.name)
        elif self.status == Status.HELD:
            Log.info("Job {0} is HELD", self.name)
        elif self.status == Status.COMPLETED:
            Log.result("Job {0} is COMPLETED", self.name)
        elif self.status == Status.FAILED:
            if not failed_file:
                if self.status == Status.COMPLETED:
                    Log.result("Job {0} is COMPLETED", self.name)
                else:
                    self.update_children_status()
        elif self.status == Status.UNKNOWN:
            Log.printlog("Job {0} is UNKNOWN. Checking completed files to confirm the failure...".format(
                self.name), 3000)
            self._platform.get_completed_files(
                self.name, wrapper_failed=self.packed)
            self.check_completion(Status.UNKNOWN)
            if self.status == Status.UNKNOWN:
                Log.printlog("Job {0} is UNKNOWN. Checking completed files to confirm the failure...".format(
                    self.name), 6009)
            elif self.status == Status.COMPLETED:
                Log.result("Job {0} is COMPLETED", self.name)
        elif self.status == Status.SUBMITTED:
            # after checking the jobs , no job should have the status "submitted"
            Log.printlog("Job {0} in SUBMITTED status. This should never happen on this step..".format(
                self.name), 6008)

        # Updating logs
        if self.status in [Status.COMPLETED, Status.FAILED, Status.UNKNOWN]:
            if str(as_conf.platforms_data.get(self.platform.name, {}).get('DISABLE_RECOVERY_THREADS', "false")).lower() == "true":
                self.retrieve_logfiles(self.platform)
            else:
                self.platform.add_job_to_log_recover(self)
        return self.status

    @staticmethod
    def _get_submitter(as_conf):
        """
        Returns the submitter corresponding to the communication defined on Autosubmit's config file

        :return: submitter
        :rtype: Submitter
        """
        #communications_library = as_conf.get_communications_library()
        # if communications_library == 'paramiko':
        return ParamikoSubmitter()
        # communications library not known
        # raise AutosubmitCritical(
        #    'You have defined a not valid communications library on the configuration file', 7014)

    def update_children_status(self):
        children = list(self.children)
        for child in children:
            if child.level == 0 and child.status in [Status.SUBMITTED, Status.RUNNING, Status.QUEUING, Status.UNKNOWN]:
                child.status = Status.FAILED
                children += list(child.children)

    def check_completion(self, default_status=Status.FAILED, over_wallclock=False):
        """
        Check the presence of *COMPLETED* file.
        Change status to COMPLETED if *COMPLETED* file exists and to FAILED otherwise.
        :param over_wallclock:
        :param default_status: status to set if job is not completed. By default, it is FAILED
        :type default_status: Status
        """
        completed_file = os.path.join(self._tmp_path, self.name + '_COMPLETED')
        completed_file_location = os.path.join(self._tmp_path, f"LOG_{self.expid}", self.name + '_COMPLETED')
        # I'm not fan of this but, it is the only way of doing it without a rework.
        if os.path.exists(completed_file) or os.path.exists(completed_file_location):
            if not over_wallclock:
                self.status = Status.COMPLETED
            else:
                return Status.COMPLETED
        else:
            Log.warning(f"Couldn't find {self.name} COMPLETED file")
            if not over_wallclock:
                self.status = default_status
            else:
                return default_status

    def update_current_parameters(self, as_conf: AutosubmitConfig, parameters: dict) -> dict:
        """
        Update the %CURRENT_*% parameters with the current platform and jobs.

        :param as_conf: The Autosubmit configuration object.
        :type as_conf: AutosubmitConfig
        :param parameters: The dictionary to update with current parameters.
        :type parameters: dict
        :return: The updated parameter's dictionary.
        :rtype: dict
        """
        for key, value in as_conf.platforms_data.get(self.platform_name, {}).items():
            parameters[f"CURRENT_{key.upper()}"] = value

        for key, value in as_conf.jobs_data[self.section].items():
            parameters[f"CURRENT_{key.upper()}"] = value

        return parameters

    def update_platform_parameters(self, as_conf, parameters):
        if not self.platform:
            submitter = job_utils._get_submitter(as_conf)
            submitter.load_platforms(as_conf)
            if not self.platform_name:
                self.platform_name = as_conf.experiment_data.get("DEFAULT", {}).get("HPCARCH", "LOCAL")
            job_platform = submitter.platforms.get(self.platform_name)
            self.platform = job_platform
        parameters['CURRENT_ARCH'] = self.platform.name
        parameters['CURRENT_HOST'] = self.platform.host
        parameters['CURRENT_USER'] = self.platform.user
        parameters['CURRENT_PROJ'] = self.platform.project
        parameters['CURRENT_BUDG'] = self.platform.budget
        parameters['CURRENT_RESERVATION'] = self.platform.reservation
        parameters['CURRENT_EXCLUSIVITY'] = self.platform.exclusivity
        parameters['CURRENT_HYPERTHREADING'] = self.platform.hyperthreading
        parameters['CURRENT_TYPE'] = self.platform.type
        parameters['CURRENT_SCRATCH_DIR'] = self.platform.scratch
        parameters['CURRENT_PROJ_DIR'] = self.platform.project_dir
        parameters['CURRENT_ROOTDIR'] = self.platform.root_dir
        parameters['CURRENT_LOGDIR'] = self.platform.get_files_path()
        return parameters

    def process_scheduler_parameters(self, job_platform, chunk):
        """
        Parsers yaml data stored in the dictionary
        and calculates the components of the heterogeneous job if any
        :return:
        """
        hetsize = 0
        if type(self.processors) is list:
            hetsize = (len(self.processors))
        else:
            hetsize = 1
        if type(self.nodes) is list:
            hetsize = max(hetsize,len(self.nodes))
        self.het['HETSIZE'] = hetsize
        self.het['PROCESSORS'] = list()
        self.het['NODES'] = list()
        self.het['NUMTHREADS'] = self.het['THREADS'] = list()
        self.het['TASKS'] = list()
        self.het['MEMORY'] = list()
        self.het['MEMORY_PER_TASK'] = list()
        self.het['RESERVATION'] = list()
        self.het['EXCLUSIVE'] = list()
        self.het['HYPERTHREADING'] = list()
        self.het['EXECUTABLE'] = list()
        self.het['CURRENT_QUEUE'] = list()
        self.het['PARTITION'] = list()
        self.het['CURRENT_PROJ'] = list()
        self.het['CUSTOM_DIRECTIVES'] = list()
        if type(self.processors) is list:
            self.het['PROCESSORS'] = list()
            for x in self.processors:
                self.het['PROCESSORS'].append(str(x))
            # Sum processors, each element can be a str or int
            self.processors = str(sum([int(x) for x in self.processors]))
        else:
            self.processors = str(self.processors)
        if type(self.nodes) is list:
            # add it to heap dict as it were originally
            self.het['NODES'] = list()
            for x in self.nodes:
                self.het['NODES'].append(str(x))
            # Sum nodes, each element can be a str or int
            self.nodes = str(sum([int(x) for x in self.nodes]))
        else:
            self.nodes = str(self.nodes)
        if type(self.threads) is list:
            # Get the max threads, each element can be a str or int
            self.het['NUMTHREADS'] = list()
            if len(self.threads) == 1:
                for x in range(self.het['HETSIZE']):
                    self.het['NUMTHREADS'].append(self.threads)
            else:
                for x in self.threads:
                    self.het['NUMTHREADS'].append(str(x))

            self.threads = str(max([int(x) for x in self.threads]))

        else:
            self.threads = str(self.threads)
        if type(self.tasks) is list:
            # Get the max tasks, each element can be a str or int
            self.het['TASKS'] = list()
            if len(self.tasks) == 1:
                if int(job_platform.processors_per_node) > 1 and int(self.tasks) > int(job_platform.processors_per_node):
                    self.tasks = job_platform.processors_per_node
                for task in range(self.het['HETSIZE']):
                    if int(job_platform.processors_per_node) > 1 and int(task) > int(
                            job_platform.processors_per_node):
                        self.het['TASKS'].append(str(job_platform.processors_per_node))
                    else:
                        self.het['TASKS'].append(str(self.tasks))
                self.tasks = str(max([int(x) for x in self.tasks]))
            else:
                for task in self.tasks:
                    if int(job_platform.processors_per_node) > 1 and int(task) > int(
                            job_platform.processors_per_node):
                        task = job_platform.processors_per_node
                    self.het['TASKS'].append(str(task))
        else:
            if job_platform.processors_per_node and int(job_platform.processors_per_node) > 1 and int(self.tasks) > int(job_platform.processors_per_node):
                self.tasks = job_platform.processors_per_node
            self.tasks = str(self.tasks)

        if type(self.memory) is list:
            # Get the max memory, each element can be a str or int
            self.het['MEMORY'] = list()
            if len(self.memory) == 1:
                for x in range(self.het['HETSIZE']):
                    self.het['MEMORY'].append(self.memory)
            else:
                for x in self.memory:
                    self.het['MEMORY'].append(str(x))
            self.memory = str(max([int(x) for x in self.memory]))
        else:
            self.memory = str(self.memory)
        if type(self.memory_per_task) is list:
            # Get the max memory per task, each element can be a str or int
            self.het['MEMORY_PER_TASK'] = list()
            if len(self.memory_per_task) == 1:
                for x in range(self.het['HETSIZE']):
                    self.het['MEMORY_PER_TASK'].append(self.memory_per_task)

            else:
                for x in self.memory_per_task:
                    self.het['MEMORY_PER_TASK'].append(str(x))
            self.memory_per_task = str(max([int(x) for x in self.memory_per_task]))

        else:
            self.memory_per_task = str(self.memory_per_task)
        if type(self.reservation) is list:
            # Get the reservation name, each element can be a str
            self.het['RESERVATION'] = list()
            if len(self.reservation) == 1:
                for x in range(self.het['HETSIZE']):
                    self.het['RESERVATION'].append(self.reservation)
            else:
                for x in self.reservation:
                    self.het['RESERVATION'].append(str(x))
            self.reservation = str(self.het['RESERVATION'][0])
        else:
            self.reservation = str(self.reservation)
        if type(self.exclusive) is list:
            # Get the exclusive, each element can be only be bool
            self.het['EXCLUSIVE'] = list()
            if len(self.exclusive) == 1:
                for x in range(self.het['HETSIZE']):
                    self.het['EXCLUSIVE'].append(self.exclusive)
            else:
                for x in self.exclusive:
                    self.het['EXCLUSIVE'].append(x)
            self.exclusive = self.het['EXCLUSIVE'][0]
        else:
            self.exclusive = self.exclusive
        if type(self.hyperthreading) is list:
            # Get the hyperthreading, each element can be only be bool
            self.het['HYPERTHREADING'] = list()
            if len(self.hyperthreading) == 1:
                for x in range(self.het['HETSIZE']):
                    self.het['HYPERTHREADING'].append(self.hyperthreading)
            else:
                for x in self.hyperthreading:
                    self.het['HYPERTHREADING'].append(x)
            self.exclusive = self.het['HYPERTHREADING'][0]
        else:
            self.hyperthreading = self.hyperthreading
        if type(self.executable) is list:
            # Get the executable, each element can be only be bool
            self.het['EXECUTABLE'] = list()
            if len(self.executable) == 1:
                for x in range(self.het['HETSIZE']):
                    self.het['EXECUTABLE'].append(self.executable)
            else:
                for x in self.executable:
                    self.het['EXECUTABLE'].append(x)
            self.executable = str(self.het['EXECUTABLE'][0])
        else:
            self.executable = self.executable
        if type(self.queue) is list:
            # Get the queue, each element can be only be bool
            self.het['CURRENT_QUEUE'] = list()
            if len(self.queue) == 1:
                for x in range(self.het['HETSIZE']):
                    self.het['CURRENT_QUEUE'].append(self.queue)
            else:
                for x in self.queue:
                    self.het['CURRENT_QUEUE'].append(x)
            self.queue = self.het['CURRENT_QUEUE'][0]
        else:
            self.queue = self.queue
        if type(self.partition) is list:
            # Get the partition, each element can be only be bool
            self.het['PARTITION'] = list()
            if len(self.partition) == 1:
                for x in range(self.het['HETSIZE']):
                    self.het['PARTITION'].append(self.partition)
            else:
                for x in self.partition:
                    self.het['PARTITION'].append(x)
            self.partition = self.het['PARTITION'][0]
        else:
            self.partition = self.partition

        self.het['CUSTOM_DIRECTIVES'] = list()
        if type(self.custom_directives) is list:
            self.custom_directives = json.dumps(self.custom_directives)
        self.custom_directives = self.custom_directives.replace("\'", "\"").strip("[]").strip(", ")
        if self.custom_directives == '':
            if job_platform.custom_directives is None:
                job_platform.custom_directives = ''
            if type(job_platform.custom_directives) is list:
                self.custom_directives = json.dumps(job_platform.custom_directives)
                self.custom_directives = self.custom_directives.replace("\'", "\"").strip("[]").strip(", ")
            else:
                self.custom_directives = job_platform.custom_directives.replace("\'", "\"").strip("[]").strip(", ")
        if self.custom_directives != '':
            if self.custom_directives[0] != "\"":
                self.custom_directives = "\"" + self.custom_directives
            if self.custom_directives[-1] != "\"":
                self.custom_directives = self.custom_directives + "\""
            self.custom_directives = "[" + self.custom_directives + "]"
            custom_directives = self.custom_directives.split("],")
            if len(custom_directives) > 1:
                for custom_directive in custom_directives:
                    if custom_directive[-1] != "]":
                        custom_directive = custom_directive + "]"
                    self.het['CUSTOM_DIRECTIVES'].append(json.loads(custom_directive))
                self.custom_directives = self.het['CUSTOM_DIRECTIVES'][0]
            else:
                if type(self.custom_directives) is str:  # TODO This is a workaround for the time being, just defined for tests passing without more issues
                    self.custom_directives = json.loads(self.custom_directives)
            if len(self.het['CUSTOM_DIRECTIVES']) < self.het['HETSIZE']:
                for x in range(self.het['HETSIZE'] - len(self.het['CUSTOM_DIRECTIVES'])):
                    self.het['CUSTOM_DIRECTIVES'].append(self.custom_directives )
        else:
            self.custom_directives = []

            for x in range(self.het['HETSIZE']):
                self.het['CUSTOM_DIRECTIVES'].append(self.custom_directives)
        # Ignore the heterogeneous parameters if the cores or nodes are no specefied as a list
        if self.het['HETSIZE'] == 1:
            self.het = dict()
        if not self.wallclock:
            if job_platform.type.lower() not in ['ps', "local"]:
                self.wallclock = "01:59"
            elif job_platform.type.lower() in ['ps', 'local']:
                self.wallclock = "00:00"
        # Increasing according to chunk
        self.wallclock = increase_wallclock_by_chunk(
            self.wallclock, self.wchunkinc, chunk)

    def update_platform_associated_parameters(self, as_conf, parameters, chunk, set_attributes):
        if set_attributes:
            self.x11_options = str(parameters.get("CURRENT_X11_OPTIONS", ""))
            self.ec_queue = str(parameters.get("CURRENT_EC_QUEUE", ""))
            self.executable = parameters.get("CURRENT_EXECUTABLE", "")
            self.total_jobs = parameters.get("CURRENT_TOTALJOBS",
                                             parameters.get("CURRENT_TOTAL_JOBS", self.platform.total_jobs))
            self.max_waiting_jobs = parameters.get("CURRENT_MAXWAITINGJOBS", parameters.get("CURRENT_MAX_WAITING_JOBS",
                                                                                            self.platform.max_waiting_jobs))
            self.processors = parameters.get("CURRENT_PROCESSORS", "1")
            self.shape = parameters.get("CURRENT_SHAPE", "")
            self.processors_per_node = parameters.get("CURRENT_PROCESSORS_PER_NODE", "1")
            self.nodes = parameters.get("CURRENT_NODES", "")
            self.exclusive = parameters.get("CURRENT_EXCLUSIVE", False)
            self.threads = parameters.get("CURRENT_THREADS", "1")
            self.tasks = parameters.get("CURRENT_TASKS", "0")
            self.reservation = parameters.get("CURRENT_RESERVATION", "")
            self.hyperthreading = parameters.get("CURRENT_HYPERTHREADING", "none")
            self.queue = parameters.get("CURRENT_QUEUE", "")
            self.partition = parameters.get("CURRENT_PARTITION", "")
            self.scratch_free_space = int(parameters.get("CURRENT_SCRATCH_FREE_SPACE", 0))
            self.memory = parameters.get("CURRENT_MEMORY", "")
            self.memory_per_task = parameters.get("CURRENT_MEMORY_PER_TASK", parameters.get("CURRENT_MEMORY_PER_TASK", ""))
            self.wallclock = parameters.get("CURRENT_WALLCLOCK", parameters.get("CURRENT_MAX_WALLCLOCK", parameters.get("CONFIG.JOB_WALLCLOCK", "24:00")))
            self.custom_directives = parameters.get("CURRENT_CUSTOM_DIRECTIVES", "")
            self.process_scheduler_parameters(self.platform, chunk)
            if self.het.get('HETSIZE', 1) > 1:
                for name, components_value in self.het.items():
                    if name != "HETSIZE":
                        for indx, component in enumerate(components_value):
                            if indx == 0:
                                parameters[name.upper()] = component
                            parameters[f'{name.upper()}_{indx}'] = component
        parameters['TOTALJOBS'] = self.total_jobs
        parameters['MAXWAITINGJOBS'] = self.max_waiting_jobs
        parameters['PROCESSORS_PER_NODE'] = self.processors_per_node
        parameters['EXECUTABLE'] = self.executable
        parameters['EXCLUSIVE'] = self.exclusive
        parameters['EC_QUEUE'] = self.ec_queue
        parameters['NUMPROC'] = self.processors
        parameters['PROCESSORS'] = self.processors
        parameters['MEMORY'] = self.memory
        parameters['MEMORY_PER_TASK'] = self.memory_per_task
        parameters['NUMTHREADS'] = self.threads
        parameters['THREADS'] = self.threads
        parameters['CPUS_PER_TASK'] = self.threads
        parameters['NUMTASK'] = self._tasks
        parameters['TASKS'] = self._tasks
        parameters['NODES'] = self.nodes
        parameters['TASKS_PER_NODE'] = self._tasks
        parameters['WALLCLOCK'] = self.wallclock
        parameters['TASKTYPE'] = self.section
        parameters['SCRATCH_FREE_SPACE'] = self.scratch_free_space
        parameters['CUSTOM_DIRECTIVES'] = self.custom_directives
        parameters['HYPERTHREADING'] = self.hyperthreading
        # we open the files and offload the whole script as a string
        # memory issues if the script is too long? Add a check to avoid problems...
        if as_conf.get_project_type() != "none":
            parameters['EXTENDED_HEADER'] = self.read_header_tailer_script(self.ext_header_path, as_conf, True)
            parameters['EXTENDED_TAILER'] = self.read_header_tailer_script(self.ext_tailer_path, as_conf, False)
        elif self.ext_header_path or self.ext_tailer_path:
            Log.warning("An extended header or tailer is defined in {0}, but it is ignored in dummy projects.",
                        self._section)
        else:
            parameters['EXTENDED_HEADER'] = ""
            parameters['EXTENDED_TAILER'] = ""
        parameters['CURRENT_QUEUE'] = self.queue
        parameters['RESERVATION'] = self.reservation
        parameters['CURRENT_EC_QUEUE'] = self.ec_queue
        parameters['PARTITION'] = self.partition

        return parameters

    def update_wrapper_parameters(self,as_conf, parameters):
        wrappers = as_conf.experiment_data.get("WRAPPERS", {})
        if len(wrappers) > 0:
            parameters['WRAPPER'] = as_conf.get_wrapper_type()
            parameters['WRAPPER' + "_POLICY"] = as_conf.get_wrapper_policy()
            parameters['WRAPPER' + "_METHOD"] = as_conf.get_wrapper_method().lower()
            parameters['WRAPPER' + "_JOBS"] = as_conf.get_wrapper_jobs()
            parameters['WRAPPER' + "_EXTENSIBLE"] = as_conf.get_extensible_wallclock()

        for wrapper_section, wrapper_val in wrappers.items():
            if type(wrapper_val) is not dict:
                continue
            parameters[wrapper_section] = as_conf.get_wrapper_type(
                as_conf.experiment_data["WRAPPERS"].get(wrapper_section))
            parameters[wrapper_section + "_POLICY"] = as_conf.get_wrapper_policy(
                as_conf.experiment_data["WRAPPERS"].get(wrapper_section))
            parameters[wrapper_section + "_METHOD"] = as_conf.get_wrapper_method(
                as_conf.experiment_data["WRAPPERS"].get(wrapper_section)).lower()
            parameters[wrapper_section + "_JOBS"] = as_conf.get_wrapper_jobs(
                as_conf.experiment_data["WRAPPERS"].get(wrapper_section))
            parameters[wrapper_section + "_EXTENSIBLE"] = int(
                as_conf.get_extensible_wallclock(as_conf.experiment_data["WRAPPERS"].get(wrapper_section)))
        return parameters

    def update_dict_parameters(self,as_conf):
        self.retrials = as_conf.jobs_data.get(self.section,{}).get("RETRIALS", as_conf.experiment_data.get("CONFIG",{}).get("RETRIALS", 0))
        for wrapper_data in ( wrapper for wrapper in as_conf.experiment_data.get("WRAPPERS",{}).values() if type(wrapper) is dict):
            jobs_in_wrapper = wrapper_data.get("JOBS_IN_WRAPPER", "").upper()
            if "," in jobs_in_wrapper:
                jobs_in_wrapper = jobs_in_wrapper.split(",")
            else:
                jobs_in_wrapper = jobs_in_wrapper.split(" ")
            if self.section.upper() in jobs_in_wrapper:
                self.retrials = wrapper_data.get("RETRIALS", self.retrials)
        if not self.splits:
            self.splits = as_conf.jobs_data.get(self.section,{}).get("SPLITS", None)
        self.delete_when_edgeless = as_conf.jobs_data.get(self.section,{}).get("DELETE_WHEN_EDGELESS", True)
        self.dependencies = str(as_conf.jobs_data.get(self.section,{}).get("DEPENDENCIES",""))
        self.running = as_conf.jobs_data.get(self.section,{}).get("RUNNING", "once")
        self.platform_name = as_conf.jobs_data.get(self.section,{}).get("PLATFORM", as_conf.experiment_data.get("DEFAULT",{}).get("HPCARCH", None))
        self.file = as_conf.jobs_data.get(self.section,{}).get("FILE", None)
        self.additional_files = as_conf.jobs_data.get(self.section,{}).get("ADDITIONAL_FILES", [])

        type_ = str(as_conf.jobs_data.get(self.section,{}).get("TYPE", "bash")).lower()
        if type_ == "bash":
            self.type = Type.BASH
        elif type_ == "python" or type_ == "python3":
            self.type = Type.PYTHON
        elif type_ == "r":
            self.type = Type.R
        elif type_ == "python2":
            self.type = Type.PYTHON2
        else:
            self.type = Type.BASH
        self.ext_header_path = as_conf.jobs_data.get(self.section,{}).get('EXTENDED_HEADER_PATH', None)
        self.ext_tailer_path = as_conf.jobs_data.get(self.section,{}).get('EXTENDED_TAILER_PATH', None)
        if self.platform_name:
            self.platform_name = self.platform_name.upper()

    def update_check_variables(self,as_conf):
        job_data = as_conf.jobs_data.get(self.section, {})
        job_platform_name = job_data.get("PLATFORM", as_conf.experiment_data.get("DEFAULT",{}).get("HPCARCH", None))
        job_platform = job_data.get("PLATFORMS",{}).get(job_platform_name, {})
        self.check = job_data.get("CHECK", True)
        self.check_warnings = job_data.get("CHECK_WARNINGS", False)
        self.total_jobs = job_data.get("TOTALJOBS",job_data.get("TOTALJOBS", job_platform.get("TOTALJOBS", job_platform.get("TOTAL_JOBS", -1))))
        self.max_waiting_jobs = job_data.get("MAXWAITINGJOBS",job_data.get("MAXWAITINGJOBS", job_platform.get("MAXWAITINGJOBS", job_platform.get("MAX_WAITING_JOBS", -1))))

    def calendar_split(self, as_conf: AutosubmitConfig, parameters: dict, set_attributes: bool) -> None:
        """
        Calculate the calendar splits for the job.

        This method processes the calendar splits based on the provided parameters and the Autosubmit configuration.

        :param as_conf: The Autosubmit configuration object.
        :type as_conf: AutosubmitConfig
        :param parameters: The dictionary containing job parameters.
        :type parameters: dict
        :param set_attributes: Flag indicating whether to set attributes directly.
        :type set_attributes: bool
        :return: None
        """
        # Calendar struct type numbered ( year, month, day, hour )


        job_data = as_conf.jobs_data.get(self.section,{})
        if job_data.get("SPLITS", None) and self.running != "once": # once jobs has no date
            # total_split = int(self.splits)
            split_unit = get_split_size_unit(as_conf.experiment_data, self.section)
            cal = str(parameters.get('EXPERIMENT.CALENDAR', "standard")).lower()
            split_length = get_split_size(as_conf.experiment_data, self.section)
            start_date = parameters.get('CHUNK_START_DATE', None)
            if set_attributes and start_date:
                self.date_split = datetime.datetime.strptime(start_date, "%Y%m%d")
            split_start = chunk_start_date(self.date_split, int(self.split), split_length, split_unit, cal)
            split_end = chunk_end_date(split_start, split_length, split_unit, cal)
            if split_unit == 'hour':
                split_end_1 = split_end
            else:
                split_end_1 = previous_day(split_end, cal)

            parameters['SPLIT'] = self.split
            parameters['SPLITSCALENDAR'] = cal
            parameters['SPLITSIZE'] = split_length
            parameters['SPLITSIZEUNIT'] = split_unit

            parameters['SPLIT_START_DATE'] = date2str(
                split_start, self.date_format)
            parameters['SPLIT_START_YEAR'] = str(split_start.year)
            parameters['SPLIT_START_MONTH'] = str(split_start.month).zfill(2)
            parameters['SPLIT_START_DAY'] = str(split_start.day).zfill(2)
            parameters['SPLIT_START_HOUR'] = str(split_start.hour).zfill(2)

            parameters['SPLIT_SECOND_TO_LAST_DATE'] = date2str(
                split_end_1, self.date_format)
            parameters['SPLIT_SECOND_TO_LAST_YEAR'] = str(split_end_1.year)
            parameters['SPLIT_SECOND_TO_LAST_MONTH'] = str(split_end_1.month).zfill(2)
            parameters['SPLIT_SECOND_TO_LAST_DAY'] = str(split_end_1.day).zfill(2)
            parameters['SPLIT_SECOND_TO_LAST_HOUR'] = str(split_end_1.hour).zfill(2)

            parameters['SPLIT_END_DATE'] = date2str(
                split_end, self.date_format)
            parameters['SPLIT_END_YEAR'] = str(split_end.year)
            parameters['SPLIT_END_MONTH'] = str(split_end.month).zfill(2)
            parameters['SPLIT_END_DAY'] = str(split_end.day).zfill(2)
            parameters['SPLIT_END_HOUR'] = str(split_end.hour).zfill(2)
            if int(self.split) == 1:
                parameters['SPLIT_FIRST'] = 'TRUE'
            else:
                parameters['SPLIT_FIRST'] = 'FALSE'

            # if int(total_split) == int(self.split):
            #     parameters['SPLIT_LAST'] = 'TRUE'
            # else:
            #     parameters['SPLIT_LAST'] = 'FALSE'

        return parameters

    def calendar_chunk(self, parameters):
        """
        Calendar for chunks

        :param parameters:
        :return:
        """
        if self.date is not None and len(str(self.date)) > 0:
            if self.chunk is None and len(str(self.chunk)) > 0:
                chunk = 1
            else:
                chunk = self.chunk

            parameters['CHUNK'] = chunk
            total_chunk = int(parameters.get('EXPERIMENT.NUMCHUNKS', 1))
            chunk_length = int(parameters.get('EXPERIMENT.CHUNKSIZE', 1))
            chunk_unit = str(parameters.get('EXPERIMENT.CHUNKSIZEUNIT', "day")).lower()
            cal = str(parameters.get('EXPERIMENT.CALENDAR', "")).lower()
            chunk_start = chunk_start_date(
                self.date, chunk, chunk_length, chunk_unit, cal)
            chunk_end = chunk_end_date(
                chunk_start, chunk_length, chunk_unit, cal)
            if chunk_unit == 'hour':
                chunk_end_1 = chunk_end
            else:
                chunk_end_1 = previous_day(chunk_end, cal)

            parameters['DAY_BEFORE'] = date2str(
                previous_day(self.date, cal), self.date_format)

            parameters['RUN_DAYS'] = str(
                subs_dates(chunk_start, chunk_end, cal))
            parameters['CHUNK_END_IN_DAYS'] = str(
                subs_dates(self.date, chunk_end, cal))

            parameters['CHUNK_START_DATE'] = date2str(
                chunk_start, self.date_format)
            parameters['CHUNK_START_YEAR'] = str(chunk_start.year)
            parameters['CHUNK_START_MONTH'] = str(chunk_start.month).zfill(2)
            parameters['CHUNK_START_DAY'] = str(chunk_start.day).zfill(2)
            parameters['CHUNK_START_HOUR'] = str(chunk_start.hour).zfill(2)

            parameters['CHUNK_SECOND_TO_LAST_DATE'] = date2str(
                chunk_end_1, self.date_format)
            parameters['CHUNK_SECOND_TO_LAST_YEAR'] = str(chunk_end_1.year)
            parameters['CHUNK_SECOND_TO_LAST_MONTH'] = str(chunk_end_1.month).zfill(2)
            parameters['CHUNK_SECOND_TO_LAST_DAY'] = str(chunk_end_1.day).zfill(2)
            parameters['CHUNK_SECOND_TO_LAST_HOUR'] = str(chunk_end_1.hour).zfill(2)

            parameters['CHUNK_END_DATE'] = date2str(
                chunk_end, self.date_format)
            parameters['CHUNK_END_YEAR'] = str(chunk_end.year)
            parameters['CHUNK_END_MONTH'] = str(chunk_end.month).zfill(2)
            parameters['CHUNK_END_DAY'] = str(chunk_end.day).zfill(2)
            parameters['CHUNK_END_HOUR'] = str(chunk_end.hour).zfill(2)

            parameters['PREV'] = str(subs_dates(self.date, chunk_start, cal))

            if chunk == 1:
                parameters['CHUNK_FIRST'] = 'TRUE'
            else:
                parameters['CHUNK_FIRST'] = 'FALSE'

            if total_chunk == chunk:
                parameters['CHUNK_LAST'] = 'TRUE'
            else:
                parameters['CHUNK_LAST'] = 'FALSE'
        return parameters

    def update_job_parameters(self, as_conf, parameters, set_attributes):
        if set_attributes:
            if self.splits == "auto":
                self.splits = parameters.get("CURRENT_SPLITS", None)
            self.delete_when_edgeless = parameters.get("CURRENT_DELETE_WHEN_EDGELESS", True)
            self.check = parameters.get("CURRENT_CHECK", False)
            self.check_warnings = parameters.get("CURRENT_CHECK_WARNINGS", False)
            self.shape = parameters.get("CURRENT_SHAPE", "")
            self.script = parameters.get("CURRENT_SCRIPT", "")
            self.x11 = False if str(parameters.get("CURRENT_X11", False)).lower() == "false" else True
            self.notify_on = parameters.get("CURRENT_NOTIFY_ON", [])
            self.update_stat_file()
            if self.checkpoint:  # To activate placeholder sustitution per <empty> in the template
                parameters["AS_CHECKPOINT"] = self.checkpoint
            self.wchunkinc = as_conf.get_wchunkinc(self.section)

        parameters['JOBNAME'] = self.name
        parameters['FAIL_COUNT'] = str(self.fail_count)
        parameters['SDATE'] = self.sdate
        parameters['MEMBER'] = self.member
        parameters['SPLIT'] = self.split
        parameters['SHAPE'] = self.shape
        parameters['SPLITS'] = self.splits
        parameters['DELAY'] = self.delay
        parameters['FREQUENCY'] = self.frequency
        parameters['SYNCHRONIZE'] = self.synchronize
        parameters['PACKED'] = self.packed
        parameters['CHUNK'] = 1
        parameters['RETRIALS'] = self.retrials
        parameters['DELAY_RETRIALS'] = self.delay_retrials
        parameters['DELETE_WHEN_EDGELESS'] = self.delete_when_edgeless
        parameters = self.calendar_chunk(parameters)
        parameters = self.calendar_split(as_conf, parameters, set_attributes)
        parameters['NUMMEMBERS'] = len(as_conf.get_member_list())

        parameters['JOB_DEPENDENCIES'] = self.dependencies
        parameters['EXPORT'] = self.export
        parameters['PROJECT_TYPE'] = as_conf.get_project_type()
        parameters['X11'] = self.x11
        return parameters

    def update_job_variables_final_values(self, parameters):
        """ Jobs variables final values based on parameters dict instead of as_conf
            This function is called to handle %CURRENT_% placeholders as they are filled up dynamically for each job
        """
        self.splits = parameters["SPLITS"]
        self.delete_when_edgeless = parameters["DELETE_WHEN_EDGELESS"]
        self.dependencies = parameters["JOB_DEPENDENCIES"]
        self.ec_queue = parameters["EC_QUEUE"]
        self.executable = parameters["EXECUTABLE"]
        self.total_jobs = parameters["TOTALJOBS"]
        self.max_waiting_jobs = parameters["MAXWAITINGJOBS"]
        self.processors = parameters["PROCESSORS"]
        self.shape = parameters["SHAPE"]
        self.processors_per_node = parameters["PROCESSORS_PER_NODE"]
        self.nodes = parameters["NODES"]
        self.exclusive = parameters["EXCLUSIVE"]
        self.threads = parameters["THREADS"]
        self.tasks = parameters["TASKS"]
        self.reservation = parameters["RESERVATION"]
        self.hyperthreading = parameters["HYPERTHREADING"]
        self.queue = parameters["CURRENT_QUEUE"]
        self.partition = parameters["PARTITION"]
        self.scratch_free_space = parameters["SCRATCH_FREE_SPACE"]
        self.memory = parameters["MEMORY"]
        self.memory_per_task = parameters["MEMORY_PER_TASK"]
        self.wallclock = parameters["WALLCLOCK"]
        self.custom_directives = parameters["CUSTOM_DIRECTIVES"]
        self.retrials = parameters["RETRIALS"]
        self.reservation = parameters["RESERVATION"]

    def reset_logs(self, as_conf: AutosubmitConfig) -> None:
        self.log_recovered = False
        self.packed_during_building = False
        self.workflow_commit = as_conf.experiment_data.get("AUTOSUBMIT", {}).get("WORKFLOW_COMMIT", "")

    def update_parameters(self, as_conf: AutosubmitConfig, set_attributes: bool = False, reset_logs: bool = False) -> None:
        """
        Refresh the job's parameters value.

        This method reloads the Autosubmit configuration and updates the job's parameters
        based on the configuration and the current state of the job.

        :param as_conf: The Autosubmit configuration object.
        :type as_conf: AutosubmitConfig
        :param set_attributes: Flag indicating whether to set attributes, defaults to False.
        :type set_attributes: bool
        :param reset_logs: Flag indicating whether to reset logs, defaults to False.
        :type reset_logs: bool
        :return: None
        """
        if not set_attributes and as_conf.needs_reload():
            set_attributes = True

        if set_attributes:
            as_conf.reload()
            if reset_logs:
                self.reset_logs(as_conf)
            self._init_runtime_parameters()
            if not hasattr(self, "start_time"):
                self.start_time = datetime.datetime.now()
            # Parameters that affect to all the rest of parameters
            self.update_dict_parameters(as_conf)
        parameters = as_conf.load_parameters()
        parameters.update(as_conf.default_parameters)
        parameters = as_conf.substitute_dynamic_variables(parameters, 25)
        parameters = self.update_platform_parameters(as_conf, parameters)
        parameters = self.update_current_parameters(as_conf, parameters)
        parameters = as_conf.deep_read_loops(parameters)
        parameters = as_conf.substitute_dynamic_variables(parameters, 25)
        parameters = self.update_job_parameters(as_conf, parameters, set_attributes)
        parameters = self.update_platform_associated_parameters(as_conf, parameters, parameters['CHUNK'], set_attributes)
        parameters = self.update_wrapper_parameters(as_conf, parameters)
        if set_attributes:
            self.update_job_variables_final_values(parameters)
        for event in self.platform.worker_events:  # keep alive log retrieval workers.
            if not event.is_set():
                event.set()
        return parameters


    def update_content_extra(self,as_conf,files):
        additional_templates = []
        for file in files:
            if as_conf.get_project_type().lower() == "none":
                template = "%DEFAULT.EXPID%"
            else:
                template = open(os.path.join(as_conf.get_project_dir(), file), 'r').read()
            additional_templates += [template]
        return additional_templates

    def update_content(self, as_conf, parameters):
        """
        Create the script content to be run for the job

        :param as_conf: Autosubmit configuration object
        :type as_conf: AutosubmitConfig
        :return: script code
        :rtype: str
        """
        if self.script:
            if self.file:
                Log.warning(f"Custom script for job {self.name} is being used, file contents are ignored.")
            template = self.script
        else:
            try:
                if as_conf.get_project_type().lower() != "none" and len(as_conf.get_project_type()) > 0:
                    template_file = open(os.path.join(as_conf.get_project_dir(), self.file), 'r')
                    template = ''
                    if as_conf.get_remote_dependencies() == "true":
                        if self.type == Type.BASH:
                            template = 'sleep 5' + "\n"
                        elif self.type == Type.PYTHON2:
                            template = 'time.sleep(5)' + "\n"
                        elif self.type == Type.PYTHON3 or self.type == Type.PYTHON:
                            template = 'time.sleep(5)' + "\n"
                        elif self.type == Type.R:
                            template = 'Sys.sleep(5)' + "\n"
                    template += template_file.read()
                    template_file.close()
                else:
                    if self.type == Type.BASH:
                        template = 'sleep 5'
                    elif self.type == Type.PYTHON2:
                        template = 'time.sleep(5)' + "\n"
                    elif self.type == Type.PYTHON3 or self.type == Type.PYTHON:
                        template = 'time.sleep(5)' + "\n"
                    elif self.type == Type.R:
                        template = 'Sys.sleep(5)'
                    else:
                        template = ''
            except Exception as e:
                template = ''

        if self.type == Type.BASH:
            snippet = StatisticsSnippetBash
        elif self.type == Type.PYTHON or self.type == Type.PYTHON3:
            snippet = StatisticsSnippetPython("3")
        elif self.type == Type.PYTHON2:
            snippet = StatisticsSnippetPython("2")
        elif self.type == Type.R:
            snippet = StatisticsSnippetR
        else:
            raise Exception('Job type {0} not supported'.format(self.type))
        template_content = self._get_template_content(as_conf, snippet, template, parameters)
        additional_content = self.update_content_extra(as_conf, self.additional_files)
        return template_content,additional_content

    def get_wrapped_content(self, as_conf, parameters):
        snippet = StatisticsSnippetEmpty
        template = 'python $SCRATCH/{1}/LOG_{1}/{0}.cmd'.format(
            self.name, self.expid)
        template_content = self._get_template_content(
            as_conf, snippet, template, parameters)
        return template_content

    def _get_template_content(self, as_conf, snippet, template, parameters):
        #communications_library = as_conf.get_communications_library()
        # if communications_library == 'paramiko':
        return self._get_paramiko_template(snippet, template, parameters)
        # else:
        #    raise AutosubmitCritical(
        #        "Job {0} does not have a correct template// template not found".format(self.name), 7014)

    def _get_paramiko_template(self, snippet, template, parameters):
        current_platform = self._platform
        return ''.join([
            snippet.as_header(
                current_platform.get_header(self, parameters), self.executable),
            template,
            snippet.as_tailer()
        ])

    def queuing_reason_cancel(self, reason):
        try:
            if len(reason.split('(', 1)) > 1:
                reason = reason.split('(', 1)[1].split(')')[0]
                if 'Invalid' in reason or reason in ['AssociationJobLimit', 'AssociationResourceLimit', 'AssociationTimeLimit',
                                                     'BadConstraints', 'QOSMaxCpuMinutesPerJobLimit', 'QOSMaxWallDurationPerJobLimit',
                                                     'QOSMaxNodePerJobLimit', 'DependencyNeverSatisfied', 'QOSMaxMemoryPerJob',
                                                     'QOSMaxMemoryPerNode', 'QOSMaxMemoryMinutesPerJob', 'QOSMaxNodeMinutesPerJob',
                                                     'InactiveLimit', 'JobLaunchFailure', 'NonZeroExitCode', 'PartitionNodeLimit',
                                                     'PartitionTimeLimit', 'SystemFailure', 'TimeLimit', 'QOSUsageThreshold',
                                                     'QOSTimeLimit','QOSResourceLimit','QOSJobLimit','InvalidQOS','InvalidAccount']:
                    return True
            return False
        except Exception as e:
            return False

    @staticmethod
    def is_a_completed_retrial(fields):
        """
        Returns true only if there are 4 fields: submit start finish status, and status equals COMPLETED.
        """
        if len(fields) == 4:
            if fields[3] == 'COMPLETED':
                return True
        return False

    def create_script(self, as_conf):
        """
        Creates script file to be run for the job

        :param as_conf: configuration object
        :type as_conf: AutosubmitConfig
        :return: script's filename
        :rtype: str
        """

        lang = locale.getlocale()[1]
        if lang is None:
            lang = locale.getdefaultlocale()[1]
            if lang is None:
                lang = 'UTF-8'
        parameters = self.update_parameters(as_conf, set_attributes=False)
        template_content,additional_templates = self.update_content(as_conf, parameters)
        #enumerate and get value
        #TODO regresion test
        for additional_file, additional_template_content in zip(self.additional_files, additional_templates):
            # append to a list all names don't matter the location, inside additional_template_content that  starts with % and ends with %
            placeholders_inside_additional_template = re.findall('%(?<!%%)[a-zA-Z0-9_.-]+%(?!%%)', additional_template_content,flags=re.IGNORECASE)
            for placeholder in placeholders_inside_additional_template:
                if placeholder in as_conf.default_parameters.values():
                    continue
                placeholder = placeholder[1:-1]
                value = str(parameters.get(placeholder.upper(),""))
                if not value:
                    additional_template_content = re.sub('%(?<!%%)' + placeholder + '%(?!%%)', '',
                                                         additional_template_content, flags=re.I)
                else:
                    if "\\" in value:
                        value = re.escape(value)
                    additional_template_content = re.sub('%(?<!%%)' + placeholder + '%(?!%%)', value, additional_template_content,flags=re.I)
            additional_template_content = additional_template_content.replace("%%", "%")
            #Write to file
            try:
                filename = os.path.basename(os.path.splitext(additional_file)[0])
                full_path = os.path.join(self._tmp_path,filename ) + "_" + self.name[5:]
                open(full_path, 'wb').write(additional_template_content.encode(lang))
            except:
                pass
        for key, value in parameters.items():
            # parameters[key] can have '\\' characters that are interpreted as escape characters
            # by re.sub. To avoid this, we use re.escape
            if "*\\" in str(parameters[key]):
                final_sub = re.escape(str(parameters[key]))
            else:
                final_sub = str(parameters[key])
            template_content = re.sub(
                '%(?<!%%)' + key + '%(?!%%)', final_sub, template_content,flags=re.I)
        if self.undefined_variables:
            for variable in self.undefined_variables:
                template_content = re.sub(
                    '%(?<!%%)' + variable + '%(?!%%)', '', template_content,flags=re.I)
        template_content = template_content.replace("%%", "%")
        script_name = '{0}.cmd'.format(self.name)
        self.script_name = '{0}.cmd'.format(self.name)

        open(os.path.join(self._tmp_path, script_name),'wb').write(template_content.encode(lang))

        os.chmod(os.path.join(self._tmp_path, script_name), 0o755)
        return script_name

    def create_wrapped_script(self, as_conf, wrapper_tag='wrapped'):
        parameters = self.update_parameters(as_conf, set_attributes=False)
        template_content = self.get_wrapped_content(as_conf, parameters)
        for key, value in parameters.items():
            template_content = re.sub(
                '%(?<!%%)' + key + '%(?!%%)', str(parameters[key]), template_content,flags=re.I)
        for variable in self.undefined_variables:
            template_content = re.sub(
                '%(?<!%%)' + variable + '%(?!%%)', '', template_content,flags=re.I)
        template_content = template_content.replace("%%", "%")
        script_name = '{0}.{1}.cmd'.format(self.name, wrapper_tag)
        self.script_name_wrapper = '{0}.{1}.cmd'.format(self.name, wrapper_tag)
        open(os.path.join(self._tmp_path, script_name),
             'w').write(template_content)
        os.chmod(os.path.join(self._tmp_path, script_name), 0o755)
        return script_name

    def check_script(self, as_conf, show_logs="false"):
        """
        Checks if script is well-formed

        :param parameters: script parameters
        :type parameters: dict
        :param as_conf: configuration file
        :type as_conf: AutosubmitConfig
        :param show_logs: Display output
        :type show_logs: Bool
        :return: true if not problem has been detected, false otherwise
        :rtype: bool
        """

        out = False
        parameters = self.update_parameters(as_conf, set_attributes=False)
        template_content, additional_templates = self.update_content(as_conf, parameters)
        if template_content is not False:
            variables = re.findall('%(?<!%%)[a-zA-Z0-9_.-]+%(?!%%)', template_content,flags=re.IGNORECASE)
            variables = [variable[1:-1] for variable in variables]
            variables = [variable for variable in variables if variable not in as_conf.default_parameters]
            for template in additional_templates:
                variables_tmp = re.findall('%(?<!%%)[a-zA-Z0-9_.-]+%(?!%%)', template,flags=re.IGNORECASE)
                variables_tmp = [variable[1:-1] for variable in variables_tmp]
                variables_tmp = [variable for variable in variables_tmp if variable not in as_conf.default_parameters]
                variables.extend(variables_tmp)

            out = set(parameters).issuperset(set(variables))
            # Check if the variables in the templates are defined in the configurations
            if not out:
                self.undefined_variables = set(variables) - set(parameters)
                if str(show_logs).lower() != "false":
                    Log.printlog("The following set of variables to be substituted in template script is not part of parameters set, and will be replaced by a blank value: {0}".format(
                        self.undefined_variables), 5013)
                    if not set(variables).issuperset(set(parameters)):
                        Log.printlog(
                            "The following set of variables are not being used in the templates: {0}".format(
                                str(set(parameters) - set(variables))), 5013)

        return out

    def update_local_logs(self, count=-1, update_submit_time=True):
        if update_submit_time:
            self.submit_time_timestamp = date2str(datetime.datetime.now(), 'S')
        if count > 0:
            self.local_logs = (f"{self.name}.{self.submit_time_timestamp}.out_retrial_{count}",
                               f"{self.name}.{self.submit_time_timestamp}.err_retrial_{count}")
        else:
            self.local_logs = (f"{self.name}.{self.submit_time_timestamp}.out",
                               f"{self.name}.{self.submit_time_timestamp}.err")

    def write_submit_time(self):
        """
        Writes submit date and time to TOTAL_STATS file. It doesn't write if hold is True.
        """
        data_time = ["",int(datetime.datetime.strptime(self.submit_time_timestamp, "%Y%m%d%H%M%S").timestamp())]
        path = os.path.join(self._tmp_path, self.name + '_TOTAL_STATS')
        if os.path.exists(path):
            with open(path, 'a') as f:
                f.write('\n')

        with open(path, 'w') as f:
            f.write(self.submit_time_timestamp)

        # Writing database
        exp_history = ExperimentHistory(self.expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR, historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR)
        exp_history.write_submit_time(self.name, submit=data_time[1],
                                      status=Status.VALUE_TO_KEY.get(self.status, "UNKNOWN"), ncpus=self.processors,
                                      wallclock=self.wallclock, qos=self.queue, date=self.date, member=self.member,
                                      section=self.section, chunk=self.chunk,
                                      platform=self.platform_name, job_id=self.id, wrapper_queue=self._wrapper_queue,
                                      wrapper_code=get_job_package_code(self.expid, self.name),
                                      children=self.children_names_str, workflow_commit=self.workflow_commit)

    def update_start_time(self, count=-1):
        start_time_ = self.check_start_time(count) # last known start time from the .cmd file
        if start_time_:
            self.start_time_timestamp = start_time_
        else:
            Log.warning(f"Start time for job {self.name} not found in the .cmd file, using last known time.")
            self.start_time_timestamp = self.start_time_timestamp if self.start_time_timestamp else time.time()
        if count > 0 or self.wrapper_name in self.platform.processed_wrapper_logs:
            self.submit_time_timestamp = date2str(datetime.datetime.fromtimestamp(self.start_time_timestamp),'S')

    def write_start_time(self, count=-1, vertical_wrapper=False):
        """
        Writes start date and time to TOTAL_STATS file
        :return: True if successful, False otherwise
        :rtype: bool
        """
        if not vertical_wrapper:
            self.update_start_time(count)
        path = os.path.join(self._tmp_path, self.name + '_TOTAL_STATS')
        f = open(path, 'a')
        f.write(' ')
        # noinspection PyTypeChecker
        f.write(date2str(datetime.datetime.fromtimestamp(self.start_time_timestamp), 'S'))
        # Writing database
        exp_history = ExperimentHistory(self.expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR, historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR)
        exp_history.write_start_time(self.name, start=self.start_time_timestamp, status=Status.VALUE_TO_KEY.get(self.status, "UNKNOWN"),  qos=self.queue, job_id=self.id, wrapper_queue=self._wrapper_queue, wrapper_code=get_job_package_code(self.expid, self.name),
                                children=self.children_names_str)
        return True

    def write_vertical_time(self, count=-1):
        self.update_start_time(count=count)
        self.update_local_logs(update_submit_time=False)
        self.write_submit_time()
        self.write_start_time(count=count, vertical_wrapper=True)
        self.write_end_time(self.status == Status.COMPLETED, count=count)

    def write_end_time(self, completed, count=-1):
        """
        Writes end timestamp to TOTAL_STATS file and jobs_data.db
        :param completed: True if the job has been completed, False otherwise
        :type completed: bool
        :param count: number of retrials
        :type count: int
        """

        end_time = self.check_end_time(count)
        path = os.path.join(self._tmp_path, self.name + '_TOTAL_STATS')
        f = open(path, 'a')
        f.write(' ')
        if end_time > 0:
            # noinspection PyTypeChecker
            f.write(date2str(datetime.datetime.fromtimestamp(float(end_time)), 'S'))
            self.finish_time_timestamp = date2str(datetime.datetime.fromtimestamp(end_time),'S')
            # date2str(datetime.datetime.fromtimestamp(end_time), 'S')
            finish_time = end_time
        else:
            f.write(date2str(datetime.datetime.now(), 'S'))
            self.finish_time_timestamp = date2str(datetime.datetime.now(), 'S')
            finish_time = time.time()
        f.write(' ')
        if completed:
            final_status = "COMPLETED"
            f.write('COMPLETED')
        else:
            final_status = "FAILED"
            f.write('FAILED')
        out, err = self.local_logs
        # Launch first as simple non-threaded function
        exp_history = ExperimentHistory(self.expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR, historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR)
        job_data_dc = exp_history.write_finish_time(self.name, finish=finish_time, status=final_status, job_id=self.id, out_file=out, err_file=err)

        # Launch second as threaded function only for slurm
        if job_data_dc and type(self.platform) is not str and self.platform.type == "slurm":
            thread_write_finish = Thread(target=ExperimentHistory(self.expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR, historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR).write_platform_data_after_finish, args=(job_data_dc, self.platform))
            thread_write_finish.name = "JOB_data_{}".format(self.name)
            thread_write_finish.start()


    def write_total_stat_by_retries(self, total_stats, first_retrial = False):
        """
        Writes all data to TOTAL_STATS file
        :param total_stats: data gathered by the wrapper
        :type total_stats: dict
        :param first_retrial: True if this is the first retry, False otherwise
        :type first_retrial: bool

        """
        path = os.path.join(self._tmp_path, self.name + '_TOTAL_STATS')
        f = open(path, 'a')
        if first_retrial:
            f.write(" " + date2str(datetime.datetime.fromtimestamp(total_stats[0]), 'S') + ' ' + date2str(datetime.datetime.fromtimestamp(total_stats[1]), 'S') + ' ' + total_stats[2])
        else:
            f.write('\n' + date2str(datetime.datetime.fromtimestamp(total_stats[0]), 'S') + ' ' + date2str(datetime.datetime.fromtimestamp(total_stats[0]), 'S') + ' ' + date2str(datetime.datetime.fromtimestamp(total_stats[1]), 'S') + ' ' + total_stats[2])
        out, err = self.local_logs
        path_out = os.path.join(self._tmp_path, 'LOG_' + str(self.expid), out)
        # Launch first as simple non-threaded function

        exp_history = ExperimentHistory(self.expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR, historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR)
        exp_history.write_start_time(self.name, start=total_stats[0], status=Status.VALUE_TO_KEY.get(self.status, "UNKNOWN"), qos=self.queue, job_id=self.id, wrapper_queue=self._wrapper_queue, wrapper_code=get_job_package_code(self.expid, self.name),
                                    children=self.children_names_str)
        if not first_retrial:
            exp_history = ExperimentHistory(self.expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR, historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR)
            exp_history.write_submit_time(self.name, submit=total_stats[0], status=Status.VALUE_TO_KEY.get(self.status, "UNKNOWN"), ncpus=self.processors,
                                        wallclock=self.wallclock, qos=self.queue, date=self.date, member=self.member, section=self.section, chunk=self.chunk,
                                        platform=self.platform_name, job_id=self.id, wrapper_queue=self._wrapper_queue, wrapper_code=get_job_package_code(self.expid, self.name),
                                        children=self.children_names_str)
        exp_history = ExperimentHistory(self.expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR, historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR)
        job_data_dc = exp_history.write_finish_time(self.name, finish=total_stats[1], status=total_stats[2], job_id=self.id, out_file=out, err_file=err)
         # Launch second as threaded function only for slurm
        if job_data_dc and type(self.platform) is not str and self.platform.type == "slurm":
            thread_write_finish = Thread(target=ExperimentHistory(self.expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR, historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR).write_platform_data_after_finish, args=(job_data_dc, self.platform))
            thread_write_finish.name = "JOB_data_{}".format(self.name)
            thread_write_finish.start()

    def check_started_after(self, date_limit):
        """
        Checks if the job started after the given date
        :param date_limit: reference date
        :type date_limit: datetime.datetime
        :return: True if job started after the given date, false otherwise
        :rtype: bool
        """
        if any(parse_date(str(date_retrial)) > date_limit for date_retrial in self.check_retrials_start_time()):
            return True
        else:
            return False

    def check_running_after(self, date_limit):
        """
        Checks if the job was running after the given date
        :param date_limit: reference date
        :type date_limit: datetime.datetime
        :return: True if job was running after the given date, false otherwise
        :rtype: bool
        """
        if any(parse_date(str(date_end)) > date_limit for date_end in self.check_retrials_end_time()):
            return True
        else:
            return False

    def is_parent(self, job):
        """
        Check if the given job is a parent
        :param job: job to be checked if is a parent
        :return: True if job is a parent, false otherwise
        :rtype bool
        """
        return job in self.parents

    def is_ancestor(self, job):
        """
        Check if the given job is an ancestor
        :param job: job to be checked if is an ancestor
        :return: True if job is an ancestor, false otherwise
        :rtype bool
        """
        for parent in list(self.parents):
            if parent.is_parent(job):
                return True
            elif parent.is_ancestor(job):
                return True
        return False

    def remove_redundant_parents(self):
        """
        Checks if a parent is also an ancestor, if true, removes the link in both directions.
        Useful to remove redundant dependencies.
        """
        for parent in list(self.parents):
            if self.is_ancestor(parent):
                parent.children.remove(self)
                self.parents.remove(parent)

    def synchronize_logs(self, platform, remote_logs, local_logs, last = True):
        platform.move_file(remote_logs[0], local_logs[0], True)  # .out
        platform.move_file(remote_logs[1], local_logs[1], True)  # .err
        if last and local_logs[0] != "":
            self.local_logs = local_logs
            self.remote_logs = copy.deepcopy(local_logs)

    def _recover_last_log_name_from_filesystem(self) -> bool:
        """
        Recovers the log name for the job from the filesystem.
        :return: True if the log name was already recovered, False otherwise
        :rtype: bool
        """
        log_name = sorted(list(self._log_path.glob(f"{self.name}*")), key=lambda x: x.stat().st_mtime)
        log_name = log_name[-1] if log_name else None
        if log_name:
            file_timestamp = int(datetime.datetime.fromtimestamp(log_name.stat().st_mtime).strftime("%Y%m%d%H%M%S"))
            if self.ready_date and file_timestamp >= int(self.ready_date):
                self.local_logs = (log_name.with_suffix(".out").name, log_name.with_suffix(".err").name)
                self.remote_logs = copy.deepcopy(self.local_logs)
                return True
        self.local_logs = (f"{self.name}.out.{self._fail_count}", f"{self.name}.err.{self._fail_count}")
        self.remote_logs = copy.deepcopy(self.local_logs)
        return False

    def recover_last_log_name(self):
        """
        Recovers the last log name for the job
        """
        if not self.updated_log:
            self.updated_log = self._recover_last_log_name_from_filesystem()
            # TODO: After PostgreSQL migration, implement _recover_last_log_from_db() to retrieve the last log from the database.

    def recover_last_ready_date(self) -> None:
        """
        Recovers the last ready date for this job
        """
        if not self.ready_date:
            stat_file = Path(f"{self._tmp_path}/{self.name}_TOTAL_STATS")
            if stat_file.exists():
                output_by_lines = stat_file.read_text().splitlines()
                if output_by_lines:
                    line_info = output_by_lines[-1].split(" ")
                    if line_info and line_info[0].isdigit():
                        self.ready_date = line_info[0]
                    else:
                        self.ready_date = datetime.datetime.fromtimestamp(stat_file.stat().st_mtime).strftime('%Y%m%d%H%M%S')
                        Log.debug(f"Failed to recover ready date for the job {self.name}")
                else:  # Default to last mod time
                    self.ready_date = datetime.datetime.fromtimestamp(stat_file.stat().st_mtime).strftime('%Y%m%d%H%M%S')
                    Log.debug(f"Failed to recover ready date for the job {self.name}")


class WrapperJob(Job):
    """
    Defines a wrapper from a package.

    Calls Job constructor.

    :param name: Name of the Package \n
    :type name: String \n
    :param job_id: ID of the first Job of the package \n
    :type job_id: Integer \n
    :param status: 'READY' when coming from submit_ready_jobs() \n
    :type status: String \n
    :param priority: 0 when coming from submit_ready_jobs() \n
    :type priority: Integer \n
    :param job_list: List of jobs in the package \n
    :type job_list: List() of Job() objects \n
    :param total_wallclock: Wallclock of the package \n
    :type total_wallclock: String Formatted \n
    :param num_processors: Number of processors for the package \n
    :type num_processors: Integer \n
    :param platform: Platform object defined for the package \n
    :type platform: Platform Object. e.g. EcPlatform() \n
    :param as_config: Autosubmit basic configuration object \n
    :type as_config: AutosubmitConfig object \n
    """

    def __init__(self, name, job_id, status, priority, job_list, total_wallclock, num_processors, platform, as_config, hold):
        super(WrapperJob, self).__init__(name, job_id, status, priority)
        self.failed = False
        self.job_list = job_list
        # divide jobs in dictionary by state?
        self.wallclock = total_wallclock # Now it is reloaded after a run -> stop -> run
        self.num_processors = num_processors
        self.running_jobs_start = OrderedDict()
        self._platform = platform
        self.as_config = as_config
        # save start time, wallclock and processors?!
        self.checked_time = datetime.datetime.now()
        self.hold = hold
        self.inner_jobs_running = list()
        self.is_wrapper = True


    def _queuing_reason_cancel(self, reason):
        try:
            if len(reason.split('(', 1)) > 1:
                reason = reason.split('(', 1)[1].split(')')[0]
                if 'Invalid' in reason or reason in ['AssociationJobLimit', 'AssociationResourceLimit', 'AssociationTimeLimit',
                                                     'BadConstraints', 'QOSMaxCpuMinutesPerJobLimit', 'QOSMaxWallDurationPerJobLimit',
                                                     'QOSMaxNodePerJobLimit', 'DependencyNeverSatisfied', 'QOSMaxMemoryPerJob',
                                                     'QOSMaxMemoryPerNode', 'QOSMaxMemoryMinutesPerJob', 'QOSMaxNodeMinutesPerJob',
                                                     'InactiveLimit', 'JobLaunchFailure', 'NonZeroExitCode', 'PartitionNodeLimit',
                                                     'PartitionTimeLimit', 'SystemFailure', 'TimeLimit', 'QOSUsageThreshold',
                                                     'QOSTimeLimit','QOSResourceLimit','QOSJobLimit','InvalidQOS','InvalidAccount']:
                    return True
            return False
        except Exception as e:
            return False

    def check_status(self, status):
        prev_status = self.status
        self.prev_status = prev_status
        self.status = status

        Log.debug('Checking inner jobs status')
        if self.status in [Status.HELD, Status.QUEUING]:  # If WRAPPER is QUEUED OR HELD
            # This will update the inner jobs to QUEUE or HELD (normal behaviour) or WAITING ( if they fail to be held)
            self._check_inner_jobs_queue(prev_status)
        elif self.status == Status.RUNNING:  # If wrapper is running
            #Log.info("Wrapper {0} is {1}".format(self.name, Status().VALUE_TO_KEY[self.status]))
            # This will update the status from submitted or hold to running (if safety timer is high enough or queue is fast enough)
            if prev_status in [Status.SUBMITTED]:
                for job in self.job_list:
                    job.status = Status.QUEUING
            self._check_running_jobs()  # Check and update inner_jobs status that are eligible
        # Completed wrapper will always come from check function.
        elif self.status == Status.COMPLETED:
            self._check_running_jobs()  # Check and update inner_jobs status that are eligible
            self.check_inner_jobs_completed(self.job_list)

        # Fail can come from check function or running/completed checkers.
        if self.status in [Status.FAILED, Status.UNKNOWN]:
            self.status = Status.FAILED
            if self.prev_status in [Status.SUBMITTED,Status.QUEUING]:
                self.update_failed_jobs(True) # check false ready jobs
            elif self.prev_status in [Status.FAILED, Status.UNKNOWN]:
                self.failed = True
                self._check_running_jobs()
            if len(self.inner_jobs_running) > 0:
                still_running = True
                if not self.failed:
                    if self._platform.check_file_exists('WRAPPER_FAILED', wrapper_failed=True):
                        for job in self.inner_jobs_running:
                            if job.platform.check_file_exists('{0}_FAILED'.format(job.name), wrapper_failed=True):
                                Log.info(
                                    "Wrapper {0} Failed, checking inner_jobs...".format(self.name))
                                self.failed = True
                                self._platform.delete_file('WRAPPER_FAILED')
                                break
                if self.failed:
                    self.update_failed_jobs()
                    if len(self.inner_jobs_running) <= 0:
                        still_running = False
            else:
                still_running = False
            if not still_running:
                self.cancel_failed_wrapper_job()

    def check_inner_jobs_completed(self, jobs):
        not_completed_jobs = [
            job for job in jobs if job.status != Status.COMPLETED]
        not_completed_job_names = [job.name for job in not_completed_jobs]
        job_names = ' '.join(not_completed_job_names)
        if job_names:
            completed_files = self._platform.check_completed_files(job_names)
            completed_jobs = []
            for job in not_completed_jobs:
                if completed_files and len(completed_files) > 0:
                    if job.name in completed_files:
                        completed_jobs.append(job)
                        job.new_status = Status.COMPLETED
                        job.updated_log = False
                        job.update_status(self.as_config)

            for job in completed_jobs:
                self.running_jobs_start.pop(job, None)

            not_completed_jobs = list(
                set(not_completed_jobs) - set(completed_jobs))

        for job in not_completed_jobs:
            self._check_finished_job(job)

    def _check_inner_jobs_queue(self, prev_status):
        reason = str()
        if self._platform.type == 'slurm':
            self._platform.send_command(
                self._platform.get_queue_status_cmd(self.id))
            reason = self._platform.parse_queue_reason(
                self._platform._ssh_output, self.id)
            if self._queuing_reason_cancel(reason):
                Log.printlog("Job {0} will be cancelled and set to FAILED as it was queuing due to {1}".format(
                    self.name, reason), 6009)
                # while running jobs?
                self._check_running_jobs()
                self.update_failed_jobs(check_ready_jobs=True)
                self.cancel_failed_wrapper_job()

                return
            if reason == '(JobHeldUser)':
                if self.hold == "false":
                    # SHOULD BE MORE CLASS (GET_scontrol release but not sure if this can be implemented on others PLATFORMS
                    self._platform.send_command("scontrol release " + "{0}".format(self.id))
                    self.new_status = Status.QUEUING
                    for job in self.job_list:
                        job.hold = self.hold
                        job.new_status = Status.QUEUING
                        job.update_status(self.as_config)
                    Log.info("Job {0} is QUEUING {1}", self.name, reason)
                else:
                    self.status = Status.HELD
                    Log.info("Job {0} is HELD", self.name)
            elif reason == '(JobHeldAdmin)':
                Log.debug(
                    "Job {0} Failed to be HELD, canceling... ", self.name)
                self._platform.send_command(
                    self._platform.cancel_cmd + " {0}".format(self.id))
                self.status = Status.WAITING
            else:
                Log.info("Job {0} is QUEUING {1}", self.name, reason)
        if prev_status != self.status:
            for job in self.job_list:
                job.hold = self.hold
                job.status = self.status

    def _check_inner_job_wallclock(self, job):
        start_time = self.running_jobs_start[job]
        if self._is_over_wallclock(start_time, job.wallclock):
            if job.wrapper_type != "vertical":
                Log.printlog("Job {0} inside wrapper {1} is running for longer than it's wallclock!".format(
                    job.name, self.name), 6009)
            return True
        return False

    def _check_running_jobs(self):
        not_finished_jobs_dict = OrderedDict()
        self.inner_jobs_running = list()
        not_finished_jobs = [job for job in self.job_list if job.status not in [
            Status.COMPLETED, Status.FAILED]]
        for job in not_finished_jobs:
            tmp = [parent for parent in job.parents if parent.status ==
                   Status.COMPLETED or self.status == Status.COMPLETED]
            if job.parents is None or len(tmp) == len(job.parents):
                not_finished_jobs_dict[job.name] = job
                self.inner_jobs_running.append(job)
        if len(list(not_finished_jobs_dict.keys())) > 0:  # Only running jobs will enter there
            not_finished_jobs_names = ' '.join(list(not_finished_jobs_dict.keys()))
            remote_log_dir = self._platform.get_remote_log_dir()
            # PREPARE SCRIPT TO SEND
            # When a inner_job is running? When the job has an _STAT file
            command = textwrap.dedent("""
            cd {1}
            for job in {0}
            do
                if [ -f "${{job}}_STAT" ]
                then
                        echo ${{job}} $(head ${{job}}_STAT)
                else
                        echo ${{job}}
                fi
            done
            """).format(str(not_finished_jobs_names), str(remote_log_dir), '\n'.ljust(13))

            log_dir = os.path.join(
                self._tmp_path, 'LOG_{0}'.format(self.expid))
            multiple_checker_inner_jobs = os.path.join(
                log_dir, "inner_jobs_checker.sh")
            if not os.stat(log_dir):
                os.mkdir(log_dir)
                os.chmod(log_dir, 0o770)
            open(multiple_checker_inner_jobs, 'w+').write(command)
            os.chmod(multiple_checker_inner_jobs, 0o770)
            if self.platform.name != "local":  # already "sent"...
                self._platform.send_file(multiple_checker_inner_jobs, False)
                command = f"cd {self._platform.get_files_path()}; {os.path.join(self._platform.get_files_path(), 'inner_jobs_checker.sh')}"
            else:
                command = f"cd {self._platform.get_files_path()}; ./inner_jobs_checker.sh; cd {os.getcwd()}"
            #
            wait = 2
            retries = 5
            over_wallclock = False
            content = ''
            while content == '' and retries > 0:
                self._platform.send_command(command, False)
                content = self._platform._ssh_output.split('\n')
                # content.reverse()
                for line in content[:-1]:
                    out = line.split()
                    if out:
                        jobname = out[0]
                        job = not_finished_jobs_dict[jobname]
                        if len(out) > 1:
                            if job not in self.running_jobs_start:
                                start_time = self._check_time(out, 1)
                                Log.info("Job {0} started at {1}".format(
                                    jobname, str(parse_date(start_time))))
                                self.running_jobs_start[job] = start_time
                                job.new_status = Status.RUNNING
                                #job.status = Status.RUNNING
                                job.update_status(self.as_config)
                            if len(out) == 2:
                                Log.info("Job {0} is RUNNING".format(jobname))
                                over_wallclock = self._check_inner_job_wallclock(
                                    job)  # messaged included
                                if over_wallclock:
                                    if job.wrapper_type != "vertical":
                                        job.status = Status.FAILED
                                        Log.printlog(
                                            "Job {0} is FAILED".format(jobname), 6009)
                            elif len(out) == 3:
                                end_time = self._check_time(out, 2)
                                self._check_finished_job(job)
                                Log.info("Job {0} finished at {1}".format(
                                    jobname, str(parse_date(end_time))))
                if content == '':
                    sleep(wait)
                retries = retries - 1
            if retries == 0 or over_wallclock:
                self.status = Status.FAILED

    def _check_finished_job(self, job, failed_file=False):
        job.new_status = Status.FAILED
        if not failed_file:
            wait = 2
            retries = 2
            output = ''
            while output == '' and retries > 0:
                output = self._platform.check_completed_files(job.name)
                if output is None or len(output) == 0:
                    sleep(wait)
                retries = retries - 1
            if (output is not None and len(str(output)) > 0 ) or 'COMPLETED' in output:
                job.new_status = Status.COMPLETED
            else:
                failed_file = True
        job.update_status(self.as_config, failed_file)
        self.running_jobs_start.pop(job, None)

    def update_failed_jobs(self, check_ready_jobs=False):
        running_jobs = self.inner_jobs_running
        real_running = copy.deepcopy(self.inner_jobs_running)
        if check_ready_jobs:
            running_jobs += [job for job in self.job_list if job.status == Status.READY or job.status == Status.SUBMITTED or job.status == Status.QUEUING]
        self.inner_jobs_running = list()
        for job in running_jobs:
            if job.platform.check_file_exists('{0}_FAILED'.format(job.name), wrapper_failed=True, max_retries=2):
                if job.platform.get_file('{0}_FAILED'.format(job.name), False, wrapper_failed=True):
                    self._check_finished_job(job, True)
            else:
                if job in real_running:
                    self.inner_jobs_running.append(job)

    def cancel_failed_wrapper_job(self):
        try:
            if self.platform_name == "local":
                # Check if the job is still running to avoid a misleading message in the logs
                if self.platform.get_pscall(self.id):
                    self._platform.send_command(
                        self._platform.cancel_cmd + " " + str(self.id))
            else:
                Log.warning(f"Wrapper {self.name} failed, cancelling it")
                self._platform.send_command(
                    self._platform.cancel_cmd + " " + str(self.id))
        except:
            Log.info(f'Job with {self.id} was finished before canceling it')
        self._check_running_jobs()
        for job in self.inner_jobs_running:
            job.status = Status.FAILED
        for job in self.job_list:
            if job.status not in [Status.COMPLETED, Status.FAILED]:
                job.status = Status.WAITING
            else:
                if job.wrapper_type == "vertical":  # job is being retrieved internally by the wrapper
                    job.fail_count = job.retrials


    def _is_over_wallclock(self, start_time, wallclock):
        elapsed = datetime.datetime.now() - parse_date(start_time)
        wallclock = datetime.datetime.strptime(wallclock, '%H:%M')
        total = 0.0
        if wallclock.hour > 0:
            total = wallclock.hour
        if wallclock.minute > 0:
            total += wallclock.minute / 60.0
        if wallclock.second > 0:
            total += wallclock.second / 60.0 / 60.0
        total = total * 1.15
        hour = int(total)
        minute = int((total - int(total)) * 60.0)
        second = int(((total - int(total)) * 60 -
                      int((total - int(total)) * 60.0)) * 60.0)
        wallclock_delta = datetime.timedelta(hours=hour, minutes=minute,
                                             seconds=second)
        if elapsed > wallclock_delta:
            return True
        return False

    def _parse_timestamp(self, timestamp):
        value = datetime.datetime.fromtimestamp(timestamp)
        time = value.strftime('%Y-%m-%d %H:%M:%S')
        return time

    def _check_time(self, output, index):
        time = int(output[index])
        time = self._parse_timestamp(time)
        return time
