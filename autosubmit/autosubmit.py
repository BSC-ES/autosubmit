#!/usr/bin/env python

# Copyright 2015-2020 Earth Sciences Department, BSC-CNS

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
from __future__ import print_function
from helpers.utils import as_rsync
import requests
import threading
import traceback

try:
    # noinspection PyCompatibility
    from configparser import SafeConfigParser
except ImportError:
    # noinspection PyCompatibility
    from ConfigParser import SafeConfigParser
from job.job_packager import JobPackager
from job.job_exceptions import WrongTemplateException
from platforms.paramiko_submitter import ParamikoSubmitter
from platforms.platform import Platform
from notifications.notifier import Notifier
from notifications.mail_notifier import MailNotifier
from bscearth.utils.date import date2str
from monitor.monitor import Monitor
from database.db_common import get_autosubmit_version, check_experiment_exists
from database.db_common import delete_experiment, update_experiment_descrip_version
from database.db_structure import get_structure
from experiment.experiment_common import copy_experiment
from experiment.experiment_common import new_experiment
from database.db_common import create_db
from job.job_grouping import JobGrouping
from job.job_list_persistence import JobListPersistencePkl
from job.job_list_persistence import JobListPersistenceDb
from job.job_package_persistence import JobPackagePersistence
from job.job_packages import JobPackageThread, JobPackageBase
from job.job_list import JobList
from job.job_utils import SubJob, SubJobManager
from job.job import Job
from git.autosubmit_git import AutosubmitGit
from job.job_common import Status
from config.config_parser import ConfigParserFactory
from config.config_common import AutosubmitConfig
from config.basicConfig import BasicConfig
import locale
from distutils.util import strtobool
from log.log import Log, AutosubmitError, AutosubmitCritical
from typing import Set

# try:
#    import dialog
# except Exception:
#    dialog = None
dialog = None
from time import sleep
import argparse
import subprocess
import json
import tarfile
import time
import copy
import os
import pwd
import sys
import shutil
import re
import random
import signal
import datetime

import portalocker
from pkg_resources import require, resource_listdir, resource_exists, resource_string
from collections import defaultdict
from pyparsing import nestedExpr
from history.experiment_status import ExperimentStatus
from history.experiment_history import ExperimentHistory
from typing import List
import history.utils as HUtils
import helpers.autosubmit_helper as AutosubmitHelper
import helpers.utils as HelperUtils
import statistics.utils as StatisticsUtils

"""
Main module for autosubmit. Only contains an interface class to all functionality implemented on autosubmit
"""

sys.path.insert(0, os.path.abspath('.'))


# noinspection PyUnusedLocal


def signal_handler(signal_received, frame):
    """
    Used to handle interrupt signals, allowing autosubmit to clean before exit

    :param signal_received:
    :param frame:
    """
    Log.info('Autosubmit will interrupt at the next safe occasion')
    Autosubmit.exit = True


def signal_handler_create(signal_received, frame):
    """
    Used to handle KeyboardInterrumpt signals while the create method is being executed

    :param signal_received:
    :param frame:
    """
    raise AutosubmitCritical(
        'Autosubmit has been closed in an unexpected way. Killed or control + c.', 7010)


class MyParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)


class Autosubmit:
    """
    Interface class for autosubmit.
    """
    sys.setrecursionlimit(500000)
    # Get the version number from the relevant file. If not, from autosubmit package
    script_dir = os.path.abspath(os.path.dirname(__file__))

    if not os.path.exists(os.path.join(script_dir, 'VERSION')):
        script_dir = os.path.join(script_dir, os.path.pardir)

    version_path = os.path.join(script_dir, 'VERSION')
    readme_path = os.path.join(script_dir, 'README')
    changes_path = os.path.join(script_dir, 'CHANGELOG')
    if os.path.isfile(version_path):
        with open(version_path) as f:
            autosubmit_version = f.read().strip()
    else:
        autosubmit_version = require("autosubmit")[0].version

    exit = False

    @staticmethod
    def parse_args():
        """
        Parse arguments given to an executable and start execution of command given
        """

        try:
            BasicConfig.read()
            parser = MyParser(
                description='Main executable for autosubmit. ')
            parser.add_argument('-v', '--version', action='version',
                                version=Autosubmit.autosubmit_version)
            parser.add_argument('-lf', '--logfile', choices=('NO_LOG', 'INFO', 'WARNING', 'DEBUG'),
                                default='DEBUG', type=str,
                                help="sets file's log level.")
            parser.add_argument('-lc', '--logconsole', choices=('NO_LOG', 'INFO', 'WARNING', 'DEBUG'),
                                default='INFO', type=str,
                                help="sets console's log level")

            subparsers = parser.add_subparsers(dest='command')
            # Run
            subparser = subparsers.add_parser(
                'run', description="runs specified experiment")
            subparser.add_argument('expid', help='experiment identifier')
            subparser.add_argument('-nt', '--notransitive', action='store_true',
                                   default=False, help='Disable transitive reduction')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')
            subparser.add_argument('-st', '--start_time', required=False,
                                   help='Sets the starting time for this experiment')
            subparser.add_argument('-sa', '--start_after', required=False,
                                   help='Sets a experiment expid which completion will trigger the start of this experiment.')
            subparser.add_argument('-rm', '--run_members', required=False,
                                   help='Sets members allowed on this run.')

            # Expid
            subparser = subparsers.add_parser(
                'expid', description="Creates a new experiment")
            group = subparser.add_mutually_exclusive_group()
            group.add_argument(
                '-y', '--copy', help='makes a copy of the specified experiment')
            group.add_argument('-dm', '--dummy', action='store_true',
                               help='creates a new experiment with default values, usually for testing')
            group.add_argument('-op', '--operational', action='store_true',
                               help='creates a new experiment with operational experiment id')
            subparser.add_argument('-H', '--HPC', required=True,
                                   help='specifies the HPC to use for the experiment')
            subparser.add_argument('-d', '--description', type=str, required=True,
                                   help='sets a description for the experiment to store in the database.')
            subparser.add_argument('-c', '--config', type=str, required=False,
                                   help='defines where are located the configuration files.')
            # Delete
            subparser = subparsers.add_parser(
                'delete', description="delete specified experiment")
            subparser.add_argument('expid', help='experiment identifier')
            subparser.add_argument(
                '-f', '--force', action='store_true', help='deletes experiment without confirmation')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')
            # Monitor
            subparser = subparsers.add_parser(
                'monitor', description="plots specified experiment")
            subparser.add_argument('expid', help='experiment identifier')
            subparser.add_argument('-o', '--output', choices=('pdf', 'png', 'ps', 'svg', 'txt'),
                                   help='chooses type of output for generated plot')  # Default -o value comes from .conf
            subparser.add_argument('-group_by', choices=('date', 'member', 'chunk', 'split', 'automatic'), default=None,
                                   help='Groups the jobs automatically or by date, member, chunk or split')
            subparser.add_argument('-expand', type=str,
                                   help='Supply the list of dates/members/chunks to filter the list of jobs. Default = "Any". '
                                        'LIST = "[ 19601101 [ fc0 [1 2 3 4] fc1 [1] ] 19651101 [ fc0 [16-30] ] ]"')
            subparser.add_argument(
                '-expand_status', type=str, help='Select the stat uses to be expanded')
            subparser.add_argument('--hide_groups', action='store_true',
                                   default=False, help='Hides the groups from the plot')
            subparser.add_argument('-cw', '--check_wrapper', action='store_true',
                                   default=False, help='Generate possible wrapper in the current workflow')
            group2 = subparser.add_mutually_exclusive_group(required=False)

            group.add_argument('-fs', '--filter_status', type=str,
                               choices=('Any', 'READY', 'COMPLETED',
                                        'WAITING', 'SUSPENDED', 'FAILED', 'UNKNOWN'),
                               help='Select the original status to filter the list of jobs')
            group = subparser.add_mutually_exclusive_group(required=False)
            group.add_argument('-fl', '--list', type=str,
                               help='Supply the list of job names to be filtered. Default = "Any". '
                                    'LIST = "b037_20101101_fc3_21_sim b037_20111101_fc4_26_sim"')
            group.add_argument('-fc', '--filter_chunks', type=str,
                               help='Supply the list of chunks to filter the list of jobs. Default = "Any". '
                                    'LIST = "[ 19601101 [ fc0 [1 2 3 4] fc1 [1] ] 19651101 [ fc0 [16-30] ] ]"')
            group.add_argument('-fs', '--filter_status', type=str,
                               choices=('Any', 'READY', 'COMPLETED',
                                        'WAITING', 'SUSPENDED', 'FAILED', 'UNKNOWN'),
                               help='Select the original status to filter the list of jobs')
            group.add_argument('-ft', '--filter_type', type=str,
                               help='Select the job type to filter the list of jobs')
            subparser.add_argument('--hide', action='store_true', default=False,
                                   help='hides plot window')
            group2.add_argument('-txt', '--text', action='store_true', default=False,
                                help='Generates only txt status file')

            group2.add_argument('-txtlog', '--txt_logfiles', action='store_true', default=False,
                                help='Generates only txt status file(AS < 3.12b behaviour)')

            subparser.add_argument('-nt', '--notransitive', action='store_true',
                                   default=False, help='Disable transitive reduction')
            # subparser.add_argument('-d', '--detail', action='store_true',
            #                        default=False, help='Shows Job List view in terminal')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')
            # Stats
            subparser = subparsers.add_parser(
                'stats', description="plots statistics for specified experiment")
            subparser.add_argument('expid', help='experiment identifier')
            subparser.add_argument('-ft', '--filter_type', type=str, help='Select the job type to filter '
                                                                          'the list of jobs')
            subparser.add_argument('-fp', '--filter_period', type=int, help='Select the period to filter jobs '
                                                                            'from current time to the past '
                                                                            'in number of hours back')
            subparser.add_argument('-o', '--output', choices=('pdf', 'png', 'ps', 'svg'), default='pdf',
                                   help='type of output for generated plot')
            subparser.add_argument('--hide', action='store_true', default=False,
                                   help='hides plot window')
            subparser.add_argument('-nt', '--notransitive', action='store_true',
                                   default=False, help='Disable transitive reduction')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')
            # Clean
            subparser = subparsers.add_parser(
                'clean', description="clean specified experiment")
            subparser.add_argument('expid', help='experiment identifier')
            subparser.add_argument(
                '-pr', '--project', action="store_true", help='clean project')
            subparser.add_argument('-p', '--plot', action="store_true",
                                   help='clean plot, only 2 last will remain')
            subparser.add_argument('-s', '--stats', action="store_true",
                                   help='clean stats, only last will remain')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')
            # Recovery
            subparser = subparsers.add_parser(
                'recovery', description="recover specified experiment")
            subparser.add_argument(
                'expid', type=str, help='experiment identifier')
            subparser.add_argument(
                '-np', '--noplot', action='store_true', default=False, help='omit plot')
            subparser.add_argument('--all', action="store_true", default=False,
                                   help='Get completed files to synchronize pkl')
            subparser.add_argument(
                '-s', '--save', action="store_true", default=False, help='Save changes to disk')
            subparser.add_argument('--hide', action='store_true', default=False,
                                   help='hides plot window')
            subparser.add_argument('-group_by', choices=('date', 'member', 'chunk', 'split', 'automatic'), default=None,
                                   help='Groups the jobs automatically or by date, member, chunk or split')
            subparser.add_argument('-expand', type=str,
                                   help='Supply the list of dates/members/chunks to filter the list of jobs. Default = "Any". '
                                        'LIST = "[ 19601101 [ fc0 [1 2 3 4] fc1 [1] ] 19651101 [ fc0 [16-30] ] ]"')
            subparser.add_argument(
                '-expand_status', type=str, help='Select the statuses to be expanded')
            subparser.add_argument('-nt', '--notransitive', action='store_true',
                                   default=False, help='Disable transitive reduction')
            subparser.add_argument('-nl', '--no_recover_logs', action='store_true', default=False,
                                   help='Disable logs recovery')
            subparser.add_argument('-d', '--detail', action='store_true',
                                   default=False, help='Show Job List view in terminal')
            subparser.add_argument('-f', '--force', action='store_true',
                                   default=False, help='Cancel active jobs ')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')
            # Migrate
            subparser = subparsers.add_parser(
                'migrate', description="Migrate experiments from current user to another")
            subparser.add_argument('expid', help='experiment identifier')
            group = subparser.add_mutually_exclusive_group(required=True)
            group.add_argument('-o', '--offer', action="store_true",
                               default=False, help='Offer experiment')
            group.add_argument('-p', '--pickup', action="store_true",
                               default=False, help='Pick-up released experiment')
            subparser.add_argument('-r', '--onlyremote', action="store_true",
                                   default=False, help='Only moves remote files')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')
            # Inspect
            subparser = subparsers.add_parser(
                'inspect', description="Generate all .cmd files")
            subparser.add_argument('expid', help='experiment identifier')
            subparser.add_argument('-nt', '--notransitive', action='store_true',
                                   default=False, help='Disable transitive reduction')
            subparser.add_argument(
                '-f', '--force', action="store_true", help='Overwrite all cmd')
            subparser.add_argument('-cw', '--check_wrapper', action='store_true',
                                   default=False, help='Generate possible wrapper in the current workflow')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')
            group.add_argument('-fs', '--filter_status', type=str,
                               choices=('Any', 'READY', 'COMPLETED',
                                        'WAITING', 'SUSPENDED', 'FAILED', 'UNKNOWN'),
                               help='Select the original status to filter the list of jobs')
            group = subparser.add_mutually_exclusive_group(required=False)
            group.add_argument('-fl', '--list', type=str,
                               help='Supply the list of job names to be filtered. Default = "Any". '
                                    'LIST = "b037_20101101_fc3_21_sim b037_20111101_fc4_26_sim"')
            group.add_argument('-fc', '--filter_chunks', type=str,
                               help='Supply the list of chunks to filter the list of jobs. Default = "Any". '
                                    'LIST = "[ 19601101 [ fc0 [1 2 3 4] fc1 [1] ] 19651101 [ fc0 [16-30] ] ]"')
            group.add_argument('-fs', '--filter_status', type=str,
                               choices=('Any', 'READY', 'COMPLETED',
                                        'WAITING', 'SUSPENDED', 'FAILED', 'UNKNOWN'),
                               help='Select the original status to filter the list of jobs')
            group.add_argument('-ft', '--filter_type', type=str,
                               help='Select the job type to filter the list of jobs')

            # Check
            subparser = subparsers.add_parser(
                'check', description="check configuration for specified experiment")
            subparser.add_argument('expid', help='experiment identifier')
            subparser.add_argument('-nt', '--notransitive', action='store_true',
                                   default=False, help='Disable transitive reduction')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')
            # Describe
            subparser = subparsers.add_parser(
                'describe', description="Show details for specified experiment")
            subparser.add_argument('expid', help='experiment identifier')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')

            # Report
            subparser = subparsers.add_parser(
                'report', description="Show metrics.. ")  # TODO
            subparser.add_argument('expid', help='experiment identifier')
            subparser.add_argument(
                '-t', '--template', type=str, help='Supply the metric template.')
            subparser.add_argument('-all', '--show_all_parameters', action='store_true',
                                   default=False, help='Writes a file containing all parameters')
            subparser.add_argument(
                '-fp', '--folder_path', type=str, help='Allows to select a non-default folder.')
            subparser.add_argument(
                '-p', '--placeholders', default=False, action='store_true',
                help='disables the sustitution of placeholders by -')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')
            # Create
            subparser = subparsers.add_parser(
                'create', description="create specified experiment joblist")
            subparser.add_argument('expid', help='experiment identifier')
            subparser.add_argument(
                '-np', '--noplot', action='store_true', default=False, help='omit plot')
            subparser.add_argument('--hide', action='store_true', default=False,
                                   help='hides plot window')
            subparser.add_argument('-d', '--detail', action='store_true',
                                   default=False, help='Show Job List view in terminal')
            subparser.add_argument('-o', '--output', choices=('pdf', 'png', 'ps', 'svg'),
                                   help='chooses type of output for generated plot')  # Default -o value comes from .conf
            subparser.add_argument('-group_by', choices=('date', 'member', 'chunk', 'split', 'automatic'), default=None,
                                   help='Groups the jobs automatically or by date, member, chunk or split')
            subparser.add_argument('-expand', type=str,
                                   help='Supply the list of dates/members/chunks to filter the list of jobs. Default = "Any". '
                                        'LIST = "[ 19601101 [ fc0 [1 2 3 4] fc1 [1] ] 19651101 [ fc0 [16-30] ] ]"')
            subparser.add_argument(
                '-expand_status', type=str, help='Select the statuses to be expanded')
            subparser.add_argument('-nt', '--notransitive', action='store_true',
                                   default=False, help='Disable transitive reduction')
            subparser.add_argument('-cw', '--check_wrapper', action='store_true',
                                   default=False, help='Generate possible wrapper in the current workflow')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')
            # Configure
            subparser = subparsers.add_parser('configure', description="configure database and path for autosubmit. It "
                                                                       "can be done at machine, user or local level."
                                                                       "If no arguments specified configure will "
                                                                       "display dialog boxes (if installed)")
            subparser.add_argument(
                '--advanced', action="store_true", help="Open advanced configuration of autosubmit")
            subparser.add_argument('-db', '--databasepath', default=None, help='path to database. If not supplied, '
                                                                               'it will prompt for it')
            subparser.add_argument(
                '-dbf', '--databasefilename', default=None, help='database filename')
            subparser.add_argument('-lr', '--localrootpath', default=None, help='path to store experiments. If not '
                                                                                'supplied, it will prompt for it')
            subparser.add_argument('-pc', '--platformsconfpath', default=None, help='path to platforms.conf file to '
                                                                                    'use by default. Optional')
            subparser.add_argument('-jc', '--jobsconfpath', default=None, help='path to jobs.conf file to use by '
                                                                               'default. Optional')
            subparser.add_argument(
                '-sm', '--smtphostname', default=None, help='STMP server hostname. Optional')
            subparser.add_argument(
                '-mf', '--mailfrom', default=None, help='Notifications sender address. Optional')
            group = subparser.add_mutually_exclusive_group()
            group.add_argument('--all', action="store_true",
                               help='configure for all users')
            group.add_argument('--local', action="store_true", help='configure only for using Autosubmit from this '
                                                                    'path')

            # Install
            subparsers.add_parser(
                'install', description='install database for autosubmit on the configured folder')

            # Set status
            subparser = subparsers.add_parser(
                'setstatus', description="sets job status for an experiment")
            subparser.add_argument('expid', help='experiment identifier')
            subparser.add_argument(
                '-np', '--noplot', action='store_true', default=False, help='omit plot')
            subparser.add_argument(
                '-s', '--save', action="store_true", default=False, help='Save changes to disk')
            subparser.add_argument('-t', '--status_final',
                                   choices=('READY', 'COMPLETED', 'WAITING', 'SUSPENDED', 'FAILED', 'UNKNOWN',
                                            'QUEUING', 'RUNNING', 'HELD'),
                                   required=True,
                                   help='Supply the target status')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')
            group = subparser.add_mutually_exclusive_group(required=True)
            group.add_argument('-fl', '--list', type=str,
                               help='Supply the list of job names to be changed. Default = "Any". '
                                    'LIST = "b037_20101101_fc3_21_sim b037_20111101_fc4_26_sim"')
            group.add_argument('-fc', '--filter_chunks', type=str,
                               help='Supply the list of chunks to change the status. Default = "Any". '
                                    'LIST = "[ 19601101 [ fc0 [1 2 3 4] fc1 [1] ] 19651101 [ fc0 [16-30] ] ]"')
            group.add_argument('-fs', '--filter_status', type=str,
                               help='Select the status (one or more) to filter the list of jobs.'
                                    "Valid values = ['Any', 'READY', 'COMPLETED', 'WAITING', 'SUSPENDED', 'FAILED', 'UNKNOWN']")
            group.add_argument('-ft', '--filter_type', type=str,
                               help='Select the job type to filter the list of jobs')
            group.add_argument('-ftc', '--filter_type_chunk', type=str,
                               help='Supply the list of chunks to change the status. Default = "Any". When the member name "all" is set, all the chunks \
                               selected from for that member will be updated for all the members. Example: all [1], will have as a result that the \
                                   chunks 1 for all the members will be updated. Follow the format: '
                                    '"[ 19601101 [ fc0 [1 2 3 4] Any [1] ] 19651101 [ fc0 [16-30] ] ],SIM,SIM2,SIM3"')

            subparser.add_argument('--hide', action='store_true', default=False,
                                   help='hides plot window')
            subparser.add_argument('-group_by', choices=('date', 'member', 'chunk', 'split', 'automatic'), default=None,
                                   help='Groups the jobs automatically or by date, member, chunk or split')
            subparser.add_argument('-expand', type=str,
                                   help='Supply the list of dates/members/chunks to filter the list of jobs. Default = "Any". '
                                        'LIST = "[ 19601101 [ fc0 [1 2 3 4] fc1 [1] ] 19651101 [ fc0 [16-30] ] ]"')
            subparser.add_argument(
                '-expand_status', type=str, help='Select the statuses to be expanded')
            subparser.add_argument('-nt', '--notransitive', action='store_true',
                                   default=False, help='Disable transitive reduction')
            subparser.add_argument('-cw', '--check_wrapper', action='store_true',
                                   default=False, help='Generate possible wrapper in the current workflow')
            subparser.add_argument('-d', '--detail', action='store_true',
                                   default=False, help='Generate detailed view of changes')

            # Test Case
            subparser = subparsers.add_parser(
                'testcase', description='create test case experiment')
            subparser.add_argument(
                '-y', '--copy', help='makes a copy of the specified experiment')
            subparser.add_argument(
                '-d', '--description', required=True, help='description of the test case')
            subparser.add_argument('-c', '--chunks', help='chunks to run')
            subparser.add_argument('-m', '--member', help='member to run')
            subparser.add_argument('-s', '--stardate', help='stardate to run')
            subparser.add_argument(
                '-H', '--HPC', required=True, help='HPC to run experiment on it')
            subparser.add_argument(
                '-b', '--branch', help='branch of git to run (or revision from subversion)')

            # Database Fix
            subparser = subparsers.add_parser(
                'dbfix', description='historical database functions')
            subparser.add_argument('expid', help='experiment identifier')

            # Pkl Fix
            subparser = subparsers.add_parser(
                'pklfix', description='restore the backup of your pkl')
            subparser.add_argument('expid', help='experiment identifier')

            # Update Description
            subparser = subparsers.add_parser(
                'updatedescrip', description="Updates the experiment's description.")
            subparser.add_argument('expid', help='experiment identifier')
            subparser.add_argument('description', help='New description.')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')
            # Test
            subparser = subparsers.add_parser(
                'test', description='test experiment')
            subparser.add_argument('expid', help='experiment identifier')
            subparser.add_argument(
                '-c', '--chunks', required=True, help='chunks to run')
            subparser.add_argument('-m', '--member', help='member to run')
            subparser.add_argument('-s', '--stardate', help='stardate to run')
            subparser.add_argument(
                '-H', '--HPC', help='HPC to run experiment on it')
            subparser.add_argument(
                '-b', '--branch', help='branch of git to run (or revision from subversion)')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')
            # Refresh
            subparser = subparsers.add_parser(
                'refresh', description='refresh project directory for an experiment')
            subparser.add_argument('expid', help='experiment identifier')
            subparser.add_argument('-mc', '--model_conf', default=False, action='store_true',
                                   help='overwrite model conf file')
            subparser.add_argument('-jc', '--jobs_conf', default=False, action='store_true',
                                   help='overwrite jobs conf file')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')
            # Update Version
            subparser = subparsers.add_parser(
                'updateversion', description='refresh experiment version')
            subparser.add_argument('expid', help='experiment identifier')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')
            # Archive
            subparser = subparsers.add_parser(
                'archive', description='archives an experiment')
            subparser.add_argument('expid', help='experiment identifier')
            subparser.add_argument('-nclean', '--noclean', default=False, action='store_true',
                                   help='Avoid Cleaning of experiment folder')
            subparser.add_argument('-uc', '--uncompress', default=False, action='store_true',
                                   help='Only does a container without compress')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')
            # Unarchive
            subparser = subparsers.add_parser(
                'unarchive', description='unarchives an experiment')
            subparser.add_argument('expid', help='experiment identifier')
            subparser.add_argument('-nclean', '--noclean', default=False, action='store_true',
                                   help='Avoid Cleaning of experiment folder')
            subparser.add_argument('-uc', '--uncompressed', default=False, action='store_true',
                                   help='Untar an uncompressed tar')
            subparser.add_argument('-v', '--update_version', action='store_true',
                                   default=False, help='Update experiment version')
            # Readme
            subparsers.add_parser('readme', description='show readme')

            # Changelog
            subparsers.add_parser('changelog', description='show changelog')
            args = parser.parse_args()


        except Exception as e:
            if type(e) is SystemExit:
                # Version keyword force an exception in parse arg due and os_exit(0) but the program is succesfully finished
                if str(e) == 0:
                    print(Autosubmit.autosubmit_version)
                    os._exit(0)
            raise AutosubmitCritical(
                "Incorrect arguments for this command", 7011)

        expid = "None"
        if hasattr(args, 'expid'):
            expid = args.expid
        if args.command != "configure" and args.command != "install":
            Autosubmit._init_logs(args, args.logconsole, args.logfile, expid)

        if args.command == 'run':
            return Autosubmit.run_experiment(args.expid, args.notransitive, args.update_version, args.start_time,
                                             args.start_after, args.run_members)
        elif args.command == 'expid':
            return Autosubmit.expid(args.HPC, args.description, args.copy, args.dummy, False,
                                    args.operational, args.config) != ''
        elif args.command == 'delete':
            return Autosubmit.delete(args.expid, args.force)
        elif args.command == 'monitor':
            return Autosubmit.monitor(args.expid, args.output, args.list, args.filter_chunks, args.filter_status,
                                      args.filter_type, args.hide, args.text, args.group_by, args.expand,
                                      args.expand_status, args.hide_groups, args.notransitive, args.check_wrapper,
                                      args.txt_logfiles, detail=False)
        elif args.command == 'stats':
            return Autosubmit.statistics(args.expid, args.filter_type, args.filter_period, args.output, args.hide,
                                         args.notransitive)
        elif args.command == 'clean':
            return Autosubmit.clean(args.expid, args.project, args.plot, args.stats)
        elif args.command == 'recovery':
            return Autosubmit.recovery(args.expid, args.noplot, args.save, args.all, args.hide, args.group_by,
                                       args.expand, args.expand_status, args.notransitive, args.no_recover_logs,
                                       args.detail, args.force)
        elif args.command == 'check':
            return Autosubmit.check(args.expid, args.notransitive)
        elif args.command == 'inspect':
            return Autosubmit.inspect(args.expid, args.list, args.filter_chunks, args.filter_status,
                                      args.filter_type, args.notransitive, args.force, args.check_wrapper)
        elif args.command == 'report':
            return Autosubmit.report(args.expid, args.template, args.show_all_parameters, args.folder_path,
                                     args.placeholders)
        elif args.command == 'describe':
            return Autosubmit.describe(args.expid)
        elif args.command == 'migrate':
            return Autosubmit.migrate(args.expid, args.offer, args.pickup, args.onlyremote)
        elif args.command == 'create':
            return Autosubmit.create(args.expid, args.noplot, args.hide, args.output, args.group_by, args.expand,
                                     args.expand_status, args.notransitive, args.check_wrapper, args.detail)
        elif args.command == 'configure':
            if not args.advanced or (args.advanced and dialog is None):
                return Autosubmit.configure(args.advanced, args.databasepath, args.databasefilename,
                                            args.localrootpath, args.platformsconfpath, args.jobsconfpath,
                                            args.smtphostname, args.mailfrom, args.all, args.local)
            else:
                return Autosubmit.configure_dialog()
        elif args.command == 'install':
            return Autosubmit.install()
        elif args.command == 'setstatus':
            return Autosubmit.set_status(args.expid, args.noplot, args.save, args.status_final, args.list,
                                         args.filter_chunks, args.filter_status, args.filter_type,
                                         args.filter_type_chunk, args.hide,
                                         args.group_by, args.expand, args.expand_status, args.notransitive,
                                         args.check_wrapper, args.detail)
        elif args.command == 'testcase':
            return Autosubmit.testcase(args.copy, args.description, args.chunks, args.member, args.stardate,
                                       args.HPC, args.branch)
        elif args.command == 'test':
            return Autosubmit.test(args.expid, args.chunks, args.member, args.stardate, args.HPC, args.branch)
        elif args.command == 'refresh':
            return Autosubmit.refresh(args.expid, args.model_conf, args.jobs_conf)
        elif args.command == 'updateversion':
            return Autosubmit.update_version(args.expid)
        elif args.command == 'archive':
            return Autosubmit.archive(args.expid, noclean=args.noclean, uncompress=args.uncompress)
        elif args.command == 'unarchive':
            return Autosubmit.unarchive(args.expid, uncompressed=args.uncompressed)

        elif args.command == 'readme':
            if os.path.isfile(Autosubmit.readme_path):
                with open(Autosubmit.readme_path) as f:
                    print(f.read())
                    return True
            return False
        elif args.command == 'changelog':
            if os.path.isfile(Autosubmit.changes_path):
                with open(Autosubmit.changes_path) as f:
                    print(f.read())
                    return True
            return False
        elif args.command == 'dbfix':
            return Autosubmit.database_fix(args.expid)
        elif args.command == 'pklfix':
            return Autosubmit.pkl_fix(args.expid)
        elif args.command == 'updatedescrip':
            return Autosubmit.update_description(args.expid, args.description)

    @staticmethod
    def _init_logs(args, console_level='INFO', log_level='DEBUG', expid='None'):
        Log.set_console_level(console_level)
        expid_less = ["expid", "testcase", "install", "-v",
                      "readme", "changelog", "configure", "unarchive"]
        global_log_command = ["delete", "archive"]
        if "offer" in args:
            if args.offer:
                global_log_command.append("migrate")  # offer
            else:
                expid_less.append("migrate")  # pickup
        import platform
        fullhost = platform.node()
        if "." in fullhost:
            host = fullhost.split(".")[0]
        elif "," in fullhost:
            host = fullhost.split(",")[0]
        else:
            host = fullhost
        forbidden = BasicConfig.DENIED_HOSTS
        authorized = BasicConfig.ALLOWED_HOSTS
        message = "Command: {0} is not allowed to run in host: {1}.\n".format(args.command.upper(), fullhost)
        message += "List of permissions as follows:Command | hosts \nAllowed hosts\n"
        for command in BasicConfig.ALLOWED_HOSTS:
            message += "   {0}:{1} \n".format(command, BasicConfig.ALLOWED_HOSTS[command])
        message += "Denied hosts\n"
        for command in BasicConfig.DENIED_HOSTS:
            message += "   {0}:{1} \n".format(command, BasicConfig.DENIED_HOSTS[command])
        message += "[Command: autosubmit {0}] is not allowed to run in [host: {1}].".format(args.command.upper(),
                                                                                            fullhost)
        if args.command in BasicConfig.DENIED_HOSTS:
            if 'all' in BasicConfig.DENIED_HOSTS[args.command] or host in BasicConfig.DENIED_HOSTS[
                args.command] or fullhost in BasicConfig.DENIED_HOSTS[args.command]:
                raise AutosubmitCritical(message, 7071)
        if args.command in BasicConfig.ALLOWED_HOSTS:
            if 'all' not in BasicConfig.ALLOWED_HOSTS[args.command] and not (
                    host in BasicConfig.ALLOWED_HOSTS[args.command] or fullhost in BasicConfig.ALLOWED_HOSTS[
                args.command]):
                raise AutosubmitCritical(message, 7071)
        if expid != 'None' and args.command not in expid_less and args.command not in global_log_command:
            as_conf = AutosubmitConfig(expid, BasicConfig, ConfigParserFactory())
            as_conf.reload()
            exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid)
            tmp_path = os.path.join(exp_path, BasicConfig.LOCAL_TMP_DIR)
            aslogs_path = os.path.join(tmp_path, BasicConfig.LOCAL_ASLOG_DIR)
            if not os.path.exists(exp_path):
                raise AutosubmitCritical("Experiment does not exist", 7012)
            # delete is treated differently
            if args.command not in ["monitor", "describe", "delete", "report", "stats", "dbfix"]:
                owner, eadmin, currentOwner = Autosubmit._check_ownership(expid, raise_error=True)  # fastlook
            else:
                owner, eadmin, currentOwner = Autosubmit._check_ownership(expid, raise_error=False)  # fastlook

            if not os.path.exists(tmp_path):
                os.mkdir(tmp_path)
            if not os.path.exists(aslogs_path):
                os.mkdir(aslogs_path)
            if owner:
                os.chmod(tmp_path, 0o775)
                Log.set_file(os.path.join(aslogs_path, args.command + '.log'), "out", log_level)
                Log.set_file(os.path.join(aslogs_path, args.command + '_err.log'), "err")
                if args.command in ["run"]:
                    if os.path.exists(os.path.join(aslogs_path, 'jobs_active_status.log')):
                        os.remove(os.path.join(aslogs_path, 'jobs_active_status.log'))
                    if os.path.exists(os.path.join(aslogs_path, 'jobs_failed_status.log')):
                        os.remove(os.path.join(aslogs_path, 'jobs_failed_status.log'))
                        Log.set_file(os.path.join(aslogs_path, 'jobs_active_status.log'), "status")
                        Log.set_file(os.path.join(aslogs_path, 'jobs_failed_status.log'), "status_failed")
            else:
                st = os.stat(tmp_path)
                oct_perm = str(oct(st.st_mode))[-3:]
                if int(oct_perm[1]) in [6, 7] or int(oct_perm[2]) in [6, 7]:
                    Log.set_file(os.path.join(tmp_path, args.command + '.log'), "out", log_level)
                    Log.set_file(os.path.join(tmp_path, args.command + '_err.log'), "err")
                else:
                    Log.set_file(os.path.join(BasicConfig.GLOBAL_LOG_DIR,
                                              args.command + expid + '.log'), "out", log_level)
                    Log.set_file(os.path.join(BasicConfig.GLOBAL_LOG_DIR,
                                              args.command + expid + '_err.log'), "err")
                    Log.printlog(
                        "Permissions of {0} are {1}. The log is being written in the {2} path instead of {1}. Please tell to the owner to fix the permissions".format(
                            tmp_path, oct_perm, BasicConfig.GLOBAL_LOG_DIR))
            Log.file_path = tmp_path
            if owner:
                if "update_version" in args:
                    force_update_version = args.update_version
                else:
                    force_update_version = False
                if args.command not in ["upgrade", "updateversion"]:
                    if force_update_version:
                        if as_conf.get_version() != Autosubmit.autosubmit_version:
                            Log.info(
                                "The {2} experiment {0} version is being updated to {1} for match autosubmit version",
                                as_conf.get_version(), Autosubmit.autosubmit_version, expid)
                            as_conf.set_version(Autosubmit.autosubmit_version)
                    else:
                        if as_conf.get_version() is not None and as_conf.get_version() != Autosubmit.autosubmit_version:
                            raise AutosubmitCritical(
                                "Current experiment uses ({0}) which is not the running Autosubmit version  \nPlease, update the experiment version if you wish to continue using AutoSubmit {1}\nYou can achieve this using the command autosubmit updateversion {2} \n"
                                "Or with the -v parameter: autosubmit {3} {2} -v ".format(as_conf.get_version(),
                                                                                          Autosubmit.autosubmit_version,
                                                                                          expid, args.command),
                                7014)
        else:
            if expid == 'None':
                exp_id = ""
            else:
                exp_id = "_" + expid
            if args.command not in expid_less:
                exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid)
                if not os.path.exists(exp_path):
                    raise AutosubmitCritical("Experiment does not exist", 7012)
            Log.set_file(os.path.join(BasicConfig.GLOBAL_LOG_DIR,
                                      args.command + exp_id + '.log'), "out", log_level)
            Log.set_file(os.path.join(BasicConfig.GLOBAL_LOG_DIR,
                                      args.command + exp_id + '_err.log'), "err")
        # Enforce LANG=UTF-8
        try:
            try:
                locale.setlocale(locale.LC_ALL, 'C.UTF-8')
            except Exception as e:
                try:
                    locale.setlocale(locale.LC_ALL, 'C.utf8')
                except Exception as e:
                    try:
                        locale.setlocale(locale.LC_ALL, 'en_GB')
                    except Exception as e:
                        locale.setlocale(locale.LC_ALL, 'es_ES')
        except Exception as e:
            Log.info("Locale C.utf8 is not found, using '{0}' as fallback".format("C"))
            locale.setlocale(locale.LC_ALL, 'C')
        Log.info(
            "Autosubmit is running with {0}", Autosubmit.autosubmit_version)

    @staticmethod
    def _check_ownership(expid, raise_error=False):
        """
        Check if user owns or if it is edamin
        :return: owner,eadmin
        :rtype: boolean,boolean
        """
        return HelperUtils.check_experiment_ownership(expid, BasicConfig, raise_error, Log)

    @staticmethod
    def _delete_expid(expid_delete, force=False):
        """
        Removes an experiment from path and database
        If current user is eadmin and -f has been sent, it deletes regardless
        of experiment owner

        :type expid_delete: str
        :param expid_delete: identifier of the experiment to delete
        :type force: boolean
        :param force: True if the force flag has been sent
        :return: True if succesfully deleted, False otherwise
        :rtype: boolean
        """
        message = "The {0} experiment was removed from the local disk and from the database.".format(expid_delete)
        message += " Note that this action does not delete any data written by the experiment.\n"
        message += "Complete list of files/directories deleted:\n"
        for root, dirs, files in os.walk(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid_delete)):
            for dir_ in dirs:
                message += os.path.join(root, dir_) + "\n"
        message += os.path.join(BasicConfig.LOCAL_ROOT_DIR, BasicConfig.STRUCTURES_DIR,
                                "structure_{0}.db".format(expid_delete)) + "\n"
        message += os.path.join(BasicConfig.LOCAL_ROOT_DIR, BasicConfig.JOBDATA_DIR,
                                "job_data_{0}.db".format(expid_delete)) + "\n"
        owner, eadmin, currentOwner = Autosubmit._check_ownership(expid_delete)
        if expid_delete == '' or expid_delete is None and not os.path.exists(
                os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid_delete)):
            Log.printlog("Experiment directory does not exist.", Log.WARNING)
        else:
            # Deletion workflow continues as usual, a disjunction is included for the case when
            # force is sent, and user is eadmin
            error_message = ""
            try:
                if owner or (force and eadmin):
                    if force and eadmin:
                        Log.info("Preparing deletion of experiment {0} from owner: {1}, as eadmin.", expid_delete,
                                 currentOwner)
                    try:
                        Log.info("Deleting experiment from database...")
                        try:
                            ret = delete_experiment(expid_delete)
                            if ret:
                                Log.result("Experiment {0} deleted".format(expid_delete))
                        except BaseException as e:
                            error_message += 'Can not delete experiment entry: {0}\n'.format(str(e))
                        Log.info("Removing experiment directory...")
                        try:
                            shutil.rmtree(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid_delete))
                        except BaseException as e:
                            error_message += 'Can not delete directory: {0}\n'.format(str(e))
                        try:
                            Log.info("Removing Structure db...")
                            structures_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, BasicConfig.STRUCTURES_DIR,
                                                           "structure_{0}.db".format(expid_delete))
                            if os.path.exists(structures_path):
                                os.remove(structures_path)
                        except BaseException as e:
                            error_message += 'Can not delete structure: {0}\n'.format(str(e))
                        try:
                            Log.info("Removing job_data db...")
                            job_data_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, BasicConfig.JOBDATA_DIR,
                                                         "job_data_{0}.db".format(expid_delete))
                            if os.path.exists(job_data_path):
                                os.remove(job_data_path)
                        except BaseException as e:
                            error_message += 'Can not delete job_data: {0}\n'.format(str(e))
                    except OSError as e:
                        error_message += 'Can not delete directory: {0}\n'.format(str(e))
                else:
                    if not eadmin:
                        raise AutosubmitCritical(
                            'Detected Eadmin user however, -f flag is not found.  {0} can not be deleted!'.format(
                                expid_delete), 7012)
                    else:
                        raise AutosubmitCritical(
                            'Current user is not the owner of the experiment. {0} can not be deleted!'.format(
                                expid_delete), 7012)
                Log.printlog(message, Log.RESULT)
            except Exception as e:
                # Avoid calling Log at this point since it is possible that tmp folder is already deleted.
                error_message += "Couldn't delete the experiment".format(e.message)
            if error_message != "":
                raise AutosubmitError(
                    "Some experiment files weren't correctly deleted\nPlease if the trace shows DATABASE IS LOCKED, report it to git\nIf there are I/O issues, wait until they're solved and then use this command again.\n",
                    error_message, 6004)

    @staticmethod
    def expid(hpc, description, copy_id='', dummy=False, test=False, operational=False, root_folder=''):
        """
        Creates a new experiment for given HPC

        :param operational: if true, creates an operational experiment
        :type operational: bool
        :type hpc: str
        :type description: str
        :type copy_id: str
        :type dummy: bool
        :param hpc: name of the main HPC for the experiment
        :param description: short experiment's description.
        :param copy_id: experiment identifier of experiment to copy
        :param dummy: if true, writes a default dummy configuration for testing
        :param test: if true, creates an experiment for testing
        :return: experiment identifier. If method fails, returns ''.
        :rtype: str
        """
        exp_id = None
        if description is None or hpc is None:
            raise AutosubmitCritical(
                "Check that the parameters are defined (-d and -H) ", 7011)
        if not copy_id:
            exp_id = new_experiment(
                description, Autosubmit.autosubmit_version, test, operational)
            if exp_id == '':
                raise AutosubmitCritical(
                    "Couldn't create a new experiment", 7011)
            try:
                os.mkdir(os.path.join(BasicConfig.LOCAL_ROOT_DIR, exp_id))
                os.mkdir(os.path.join(
                    BasicConfig.LOCAL_ROOT_DIR, exp_id, 'conf'))
                Log.info("Copying config files...")
                # autosubmit config and experiment copied from AS.
                files = resource_listdir('autosubmit.config', 'files')
                for filename in files:
                    if resource_exists('autosubmit.config', 'files/' + filename):
                        index = filename.index('.')
                        new_filename = filename[:index] + \
                                       "_" + exp_id + filename[index:]

                        if filename == 'platforms.conf' and BasicConfig.DEFAULT_PLATFORMS_CONF != '':
                            content = open(os.path.join(
                                BasicConfig.DEFAULT_PLATFORMS_CONF, filename)).read()
                        elif filename == 'jobs.conf' and BasicConfig.DEFAULT_JOBS_CONF != '':
                            content = open(os.path.join(
                                BasicConfig.DEFAULT_JOBS_CONF, filename)).read()
                        else:
                            content = resource_string(
                                'autosubmit.config', 'files/' + filename)

                        # If autosubmitrc [conf] custom_platforms has been set and file exists, replace content
                        if filename.startswith("platforms") and os.path.isfile(BasicConfig.CUSTOM_PLATFORMS_PATH):
                            content = open(
                                BasicConfig.CUSTOM_PLATFORMS_PATH, 'r').read()

                        conf_new_filename = os.path.join(
                            BasicConfig.LOCAL_ROOT_DIR, exp_id, "conf", new_filename)
                        Log.debug(conf_new_filename)
                        open(conf_new_filename, 'w').write(content)
                Autosubmit._prepare_conf_files(
                    exp_id, hpc, Autosubmit.autosubmit_version, dummy, copy_id)
            except (OSError, IOError) as e:
                Autosubmit._delete_expid(exp_id, True)
                raise AutosubmitCritical(
                    "Couldn't create a new experiment, permissions?", 7012, e.message)
            except BaseException as e:
                raise AutosubmitCritical("Couldn't create a new experiment", 7012, e.message)
        else:
            try:
                if root_folder == '' or root_folder is None:
                    root_folder = os.path.join(
                        BasicConfig.LOCAL_ROOT_DIR, copy_id)
                if os.path.exists(root_folder):
                    # List of allowed files from conf
                    conf_copy_filter_folder = []
                    conf_copy_filter = ["autosubmit_" + str(copy_id) + ".conf",
                                        "expdef_" + str(copy_id) + ".conf",
                                        "jobs_" + str(copy_id) + ".conf",
                                        "platforms_" + str(copy_id) + ".conf",
                                        "proj_" + str(copy_id) + ".conf"]
                    if root_folder != os.path.join(BasicConfig.LOCAL_ROOT_DIR, copy_id):
                        conf_copy_filter_folder = ["autosubmit.conf",
                                                   "expdef.conf",
                                                   "jobs.conf",
                                                   "platforms.conf",
                                                   "proj.conf"]
                        exp_id = new_experiment(
                            description, Autosubmit.autosubmit_version, test, operational)
                    else:
                        exp_id = copy_experiment(
                            copy_id, description, Autosubmit.autosubmit_version, test, operational)

                    if exp_id == '':
                        return ''
                    dir_exp_id = os.path.join(
                        BasicConfig.LOCAL_ROOT_DIR, exp_id)
                    os.mkdir(dir_exp_id)
                    os.mkdir(dir_exp_id + '/conf')
                    if root_folder == os.path.join(BasicConfig.LOCAL_ROOT_DIR, copy_id):
                        Log.info(
                            "Copying previous experiment config directories")
                        conf_copy_id = os.path.join(
                            BasicConfig.LOCAL_ROOT_DIR, copy_id, "conf")
                    else:
                        Log.info("Copying from folder: {0}", root_folder)
                        conf_copy_id = root_folder
                    files = os.listdir(conf_copy_id)
                    for filename in files:
                        # Allow only those files in the list
                        if filename in conf_copy_filter:
                            if os.path.isfile(os.path.join(conf_copy_id, filename)):
                                new_filename = filename.replace(
                                    copy_id, exp_id)
                                # Using readlines for replacement handling
                                content = open(os.path.join(
                                    conf_copy_id, filename), 'r').readlines()

                                # If autosubmitrc [conf] custom_platforms has been set and file exists, replace content
                                if filename.startswith("platforms") and os.path.isfile(
                                        BasicConfig.CUSTOM_PLATFORMS_PATH):
                                    content = open(
                                        BasicConfig.CUSTOM_PLATFORMS_PATH, 'r').readlines()
                                # Setting email notifications to false
                                if filename == str("autosubmit_" + str(copy_id) + ".conf"):
                                    content = ["NOTIFICATIONS = False\n" if line.startswith(
                                        ("NOTIFICATIONS =", "notifications =")) else line for line in content]
                                # Putting content together before writing
                                sep = ""
                                open(os.path.join(dir_exp_id, "conf",
                                                  new_filename), 'w').write(sep.join(content))
                        if filename in conf_copy_filter_folder:
                            if os.path.isfile(os.path.join(conf_copy_id, filename)):
                                new_filename = filename.split(
                                    ".")[0] + "_" + exp_id + ".conf"
                                content = open(os.path.join(
                                    conf_copy_id, filename), 'r').read()
                                # If autosubmitrc [conf] custom_platforms has been set and file exists, replace content
                                if filename.startswith("platforms") and os.path.isfile(
                                        BasicConfig.CUSTOM_PLATFORMS_PATH):
                                    content = open(
                                        BasicConfig.CUSTOM_PLATFORMS_PATH, 'r').read()

                                open(os.path.join(dir_exp_id, "conf",
                                                  new_filename), 'w').write(content)

                    Autosubmit._prepare_conf_files(
                        exp_id, hpc, Autosubmit.autosubmit_version, dummy, copy_id)
                    #####
                    autosubmit_config = AutosubmitConfig(
                        exp_id, BasicConfig, ConfigParserFactory())
                    autosubmit_config.check_conf_files(False)
                    project_type = autosubmit_config.get_project_type()
                    if project_type == "git":
                        autosubmit_git = AutosubmitGit(copy_id[0])
                        Log.info("checking model version...")
                        if not autosubmit_git.check_commit(autosubmit_config):
                            raise AutosubmitCritical(
                                "Uncommitted changes", 7013)

                else:
                    raise AutosubmitCritical(
                        "The experiment directory doesn't exist", 7012)
            except (OSError, IOError) as e:
                Autosubmit._delete_expid(exp_id, True)
                raise AutosubmitCritical(
                    "Can not create experiment", 7012, e.message)
            except BaseException as e:
                raise AutosubmitCritical(
                    "Can not create experiment", 7012, e.message)

        Log.debug("Creating temporal directory...")
        exp_id_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, exp_id)
        tmp_path = os.path.join(exp_id_path, "tmp")
        os.mkdir(tmp_path)
        os.chmod(tmp_path, 0o775)
        os.mkdir(os.path.join(tmp_path, BasicConfig.LOCAL_ASLOG_DIR))
        os.chmod(os.path.join(tmp_path, BasicConfig.LOCAL_ASLOG_DIR), 0o775)
        Log.debug("Creating temporal remote directory...")
        remote_tmp_path = os.path.join(tmp_path, "LOG_" + exp_id)
        os.mkdir(remote_tmp_path)
        os.chmod(remote_tmp_path, 0o755)

        Log.debug("Creating pkl directory...")
        os.mkdir(os.path.join(exp_id_path, "pkl"))

        Log.debug("Creating plot directory...")
        os.mkdir(os.path.join(exp_id_path, "plot"))
        os.chmod(os.path.join(exp_id_path, "plot"), 0o775)
        Log.result("Experiment registered successfully")
        Log.warning("Remember to MODIFY the config files!")
        try:
            Log.debug("Setting the right permissions...")
            os.chmod(os.path.join(exp_id_path, "conf"), 0o755)
            os.chmod(os.path.join(exp_id_path, "pkl"), 0o755)
            os.chmod(os.path.join(exp_id_path, "tmp"), 0o775)
            os.chmod(os.path.join(exp_id_path, "plot"), 0o775)
            os.chmod(os.path.join(exp_id_path, "conf/autosubmit_" +
                                  str(exp_id) + ".conf"), 0o755)
            os.chmod(os.path.join(exp_id_path, "conf/expdef_" +
                                  str(exp_id) + ".conf"), 0o755)
            os.chmod(os.path.join(exp_id_path, "conf/jobs_" +
                                  str(exp_id) + ".conf"), 0o755)
            os.chmod(os.path.join(exp_id_path, "conf/platforms_" +
                                  str(exp_id) + ".conf"), 0o755)
            try:
                os.chmod(os.path.join(exp_id_path, "tmp/ASLOGS"), 0o755)
            except:
                pass
            os.chmod(os.path.join(exp_id_path, "conf/proj_" +
                                  str(exp_id) + ".conf"), 0o755)
        except:
            pass  # some folder may no exists, like proj
        return exp_id

    @staticmethod
    def delete(expid, force):
        """
        Deletes and experiment from database and experiment's folder

        :type force: bool
        :type expid: str
        :param expid: identifier of the experiment to delete
        :param force: if True, does not ask for confirmation

        :returns: True if succesful, False if not
        :rtype: bool
        """

        if os.path.exists(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid)):
            if force or Autosubmit._user_yes_no_query("Do you want to delete " + expid + " ?"):

                Log.debug('Enter Autosubmit._delete_expid {0}', expid)
                try:
                    return Autosubmit._delete_expid(expid, force)
                except AutosubmitCritical as e:
                    raise
                except BaseException as e:
                    raise AutosubmitCritical("Seems that something went wrong, please check the trace", 7012, e.message)
            else:
                raise AutosubmitCritical("Insufficient permissions", 7012)
        else:
            raise AutosubmitCritical("Experiment does not exist", 7012)

    @staticmethod
    def _load_parameters(as_conf, job_list, platforms):
        """
        Add parameters from configuration files into platform objects, and into the job_list object.

        :param as_conf: Basic configuration handler.\n
        :type as_conf: AutosubmitConfig object\n
        :param job_list: Handles the list as a unique entity.\n
        :type job_list: JobList() object\n
        :param platforms: List of platforms related to the experiment.\n
        :type platforms: List() of Platform Objects. e.g EcPlatform(), SgePlatform().
        :return: Nothing, modifies input.
        """

        Log.debug("Loading parameters...")
        parameters = as_conf.load_parameters()
        Log.debug("Parameters load.")
        for platform_name in platforms:
            platform = platforms[platform_name]
            # Call method from platform.py parent object
            platform.add_parameters(parameters)
        # Platform = from DEFAULT.HPCARCH, e.g. marenostrum4
        if as_conf.get_platform().lower() not in platforms.keys():
            Log.warning("Main platform is not defined in platforms.conf")
        else:
            platform = platforms[as_conf.get_platform().lower()]
            platform.add_parameters(parameters, True)
        # Attach paramenters to JobList
        parameters['STARTDATES'] = []
        for date in job_list._date_list:
            parameters['STARTDATES'].append(date2str(date, job_list.get_date_format()))

        job_list.parameters = parameters

    @staticmethod
    def inspect(expid, lst, filter_chunks, filter_status, filter_section, notransitive=False, force=False,
                check_wrapper=False):
        """
         Generates cmd files experiment.

         :type expid: str
         :param expid: identifier of experiment to be run
         :return: True if run to the end, False otherwise
         :rtype: bool
         """
        try:
            Autosubmit._check_ownership(expid, raise_error=True)
            exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid)
            tmp_path = os.path.join(exp_path, BasicConfig.LOCAL_TMP_DIR)
            if os.path.exists(os.path.join(tmp_path, 'autosubmit.lock')):
                locked = True
            else:
                locked = False
            Log.info("Starting inspect command")
            os.system('clear')
            signal.signal(signal.SIGINT, signal_handler)
            as_conf = AutosubmitConfig(expid, BasicConfig, ConfigParserFactory())
            as_conf.check_conf_files(True)
            project_type = as_conf.get_project_type()
            safetysleeptime = as_conf.get_safetysleeptime()
            Log.debug("The Experiment name is: {0}", expid)
            Log.debug("Sleep: {0}", safetysleeptime)
            packages_persistence = JobPackagePersistence(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl"),
                                                         "job_packages_" + expid)
            os.chmod(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid,
                                  "pkl", "job_packages_" + expid + ".db"), 0644)

            packages_persistence.reset_table(True)
            job_list_original = Autosubmit.load_job_list(
                expid, as_conf, notransitive=notransitive)
            job_list = Autosubmit.load_job_list(
                expid, as_conf, notransitive=notransitive)
            job_list.packages_dict = {}

            Log.debug("Length of the jobs list: {0}", len(job_list))

            # variables to be updated on the fly
            safetysleeptime = as_conf.get_safetysleeptime()
            Log.debug("Sleep: {0}", safetysleeptime)
            # Generate
            Log.info("Starting to generate cmd scripts")

            if not isinstance(job_list, type([])):
                jobs = []
                jobs_cw = []
                if check_wrapper and (not locked or (force and locked)):
                    Log.info("Generating all cmd script adapted for wrappers")
                    jobs = job_list.get_uncompleted()

                    jobs_cw = job_list.get_completed()
                else:
                    if (force and not locked) or (force and locked):
                        Log.info("Overwritting all cmd scripts")
                        jobs = job_list.get_job_list()
                    elif locked:
                        Log.warning(
                            "There is a .lock file and not -f, generating only all unsubmitted cmd scripts")
                        jobs = job_list.get_unsubmitted()
                    else:
                        Log.info("Generating cmd scripts only for selected jobs")
                        if filter_chunks:
                            fc = filter_chunks
                            Log.debug(fc)
                            if fc == 'Any':
                                jobs = job_list.get_job_list()
                            else:
                                # noinspection PyTypeChecker
                                data = json.loads(Autosubmit._create_json(fc))
                                for date_json in data['sds']:
                                    date = date_json['sd']
                                    jobs_date = filter(lambda j: date2str(
                                        j.date) == date, job_list.get_job_list())

                                    for member_json in date_json['ms']:
                                        member = member_json['m']
                                        jobs_member = filter(
                                            lambda j: j.member == member, jobs_date)

                                        for chunk_json in member_json['cs']:
                                            chunk = int(chunk_json)
                                            jobs = jobs + \
                                                   [job for job in filter(
                                                       lambda j: j.chunk == chunk, jobs_member)]

                        elif filter_status:
                            Log.debug(
                                "Filtering jobs with status {0}", filter_status)
                            if filter_status == 'Any':
                                jobs = job_list.get_job_list()
                            else:
                                fs = Autosubmit._get_status(filter_status)
                                jobs = [job for job in filter(
                                    lambda j: j.status == fs, job_list.get_job_list())]

                        elif filter_section:
                            ft = filter_section
                            Log.debug(ft)

                            if ft == 'Any':
                                jobs = job_list.get_job_list()
                            else:
                                for job in job_list.get_job_list():
                                    if job.section == ft:
                                        jobs.append(job)
                        elif lst:
                            jobs_lst = lst.split()

                            if jobs == 'Any':
                                jobs = job_list.get_job_list()
                            else:
                                for job in job_list.get_job_list():
                                    if job.name in jobs_lst:
                                        jobs.append(job)
                        else:
                            jobs = job_list.get_job_list()
            if isinstance(jobs, type([])):
                referenced_jobs_to_remove = set()
                for job in jobs:
                    for child in job.children:
                        if child not in jobs:
                            referenced_jobs_to_remove.add(child)
                    for parent in job.parents:
                        if parent not in jobs:
                            referenced_jobs_to_remove.add(parent)

                for job in jobs:
                    job.status = Status.WAITING

                Autosubmit.generate_scripts_andor_wrappers(
                    as_conf, job_list, jobs, packages_persistence, False)
            if len(jobs_cw) > 0:
                referenced_jobs_to_remove = set()
                for job in jobs_cw:
                    for child in job.children:
                        if child not in jobs_cw:
                            referenced_jobs_to_remove.add(child)
                    for parent in job.parents:
                        if parent not in jobs_cw:
                            referenced_jobs_to_remove.add(parent)

                for job in jobs_cw:
                    job.status = Status.WAITING
                Autosubmit.generate_scripts_andor_wrappers(
                    as_conf, job_list, jobs_cw, packages_persistence, False)

            Log.info("no more scripts to generate, now proceed to check them manually")
            time.sleep(safetysleeptime)
        except AutosubmitCritical as e:
            raise
        except AutosubmitError as e:
            raise
        except BaseException as e:
            raise AutosubmitCritical(
                "There are issues that occurred during the templates generation, please check that job parameters are well set and the template path exists.",
                7014, e.message)
        return True

    @staticmethod
    def generate_scripts_andor_wrappers(as_conf, job_list, jobs_filtered, packages_persistence, only_wrappers=False):
        """
        :param as_conf: Class that handles basic configuration parameters of Autosubmit. \n
        :type as_conf: AutosubmitConfig() Object \n
        :param job_list: Representation of the jobs of the experiment, keeps the list of jobs inside. \n
        :type job_list: JobList() Object \n
        :param jobs_filtered: list of jobs that are relevant to the process. \n
        :type jobs_filtered: List() of Job Objects \n
        :param packages_persistence: Object that handles local db persistence.  \n
        :type packages_persistence: JobPackagePersistence() Object \n
        :param only_wrappers: True when coming from Autosubmit.create(). False when coming from Autosubmit.inspect(), \n
        :type only_wrappers: Boolean \n
        :return: Nothing\n
        :rtype: \n
        """
        Log.warning("Generating the auxiliar job_list used for the -CW flag.")
        job_list._job_list = jobs_filtered
        parameters = as_conf.load_parameters()
        date_list = as_conf.get_date_list()
        if len(date_list) != len(set(date_list)):
            raise AutosubmitCritical(
                'There are repeated start dates!', 7014)
        num_chunks = as_conf.get_num_chunks()
        chunk_ini = as_conf.get_chunk_ini()
        member_list = as_conf.get_member_list()
        run_only_members = as_conf.get_member_list(run_only=True)
        date_format = ''
        if as_conf.get_chunk_size_unit() is 'hour':
            date_format = 'H'
        for date in date_list:
            if date.hour > 1:
                date_format = 'H'
            if date.minute > 1:
                date_format = 'M'
        wrapper_jobs = dict()
        if as_conf.get_wrapper_type() == "multi":
            for wrapper_section in as_conf.get_wrapper_multi():
                wrapper_jobs[wrapper_section] = as_conf.get_wrapper_jobs(wrapper_section)
        wrapper_jobs["wrapper"] = as_conf.get_wrapper_jobs("wrapper")
        #
        Log.warning("Aux Job_list was generated successfully")
        submitter = Autosubmit._get_submitter(as_conf)
        submitter.load_platforms(as_conf)
        hpcarch = as_conf.get_platform()
        Autosubmit._load_parameters(as_conf, job_list, submitter.platforms)
        platforms_to_test = set()
        for job in job_list.get_job_list():
            if job.platform_name is None:
                job.platform_name = hpcarch
            job.platform = submitter.platforms[job.platform_name.lower()]
            platforms_to_test.add(job.platform)

        job_list.check_scripts(as_conf)

        job_list.update_list(as_conf, False)
        # Loading parameters again
        Autosubmit._load_parameters(as_conf, job_list, submitter.platforms)
        # Related to TWO_STEP_START new variable defined in expdef
        unparsed_two_step_start = as_conf.get_parse_two_step_start()
        if unparsed_two_step_start != "":
            job_list.parse_jobs_by_filter(unparsed_two_step_start)
        job_list.create_dictionary(date_list, member_list, num_chunks, chunk_ini, date_format, as_conf.get_retrials(),
                                   wrapper_jobs)

        while job_list.get_active():
            Autosubmit.submit_ready_jobs(as_conf, job_list, platforms_to_test, packages_persistence, True,
                                         only_wrappers, hold=False)
            # for job in job_list.get_uncompleted_and_not_waiting():
            #    job.status = Status.COMPLETED
            job_list.update_list(as_conf, False)

    @staticmethod
    def run_experiment(expid, notransitive=False, update_version=False, start_time=None, start_after=None,
                       run_members=None):
        """
        Runs and experiment (submitting all the jobs properly and repeating its execution in case of failure).

        :type expid: str
        :param expid: identifier of experiment to be run
        :return: True if run to the end, False otherwise
        :rtype: bool
        """
        try:
            exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid)
            tmp_path = os.path.join(exp_path, BasicConfig.LOCAL_TMP_DIR)
            import platform
            host = platform.node()
            as_conf = AutosubmitConfig(expid, BasicConfig, ConfigParserFactory())

            as_conf.check_conf_files(True)
            try:
                Log.info("Removing job_data db...")
                job_data_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, BasicConfig.JOBDATA_DIR,
                                             "job_data_{0}.db".format(expid))
                # Delete job_data if 0 bytes
                if os.path.exists(job_data_path) and os.stat(job_data_path).st_size <= 6:
                    Log.info("job_data db is corrupted. It will be removed.")
                    os.remove(job_data_path)
            except BaseException as e:
                Log.warning("Error removing job_data db: {0}, please remove it manually".format(e.message))


        except BaseException as e:
            raise AutosubmitCritical("Failure during the loading of the experiment configuration, check file paths",
                                     7014, e.message)

        try:
            # Handling starting time
            AutosubmitHelper.handle_start_time(start_time)

            # Start start after completion trigger block
            AutosubmitHelper.handle_start_after(start_after, expid, BasicConfig)

            # Handling run_members
            allowed_members = AutosubmitHelper.get_allowed_members(run_members, as_conf)
        except AutosubmitCritical as e:
            raise
        except BaseException as e:
            raise AutosubmitCritical("Failure during setting the start time check trace for details", 7014, e.message)

        # checking if there is a lock file to avoid multiple running on the same expid
        try:
            with portalocker.Lock(os.path.join(tmp_path, 'autosubmit.lock'), timeout=1):
                try:
                    Log.info(
                        "Preparing .lock file to avoid multiple instances with same experiment id")
                    os.system('clear')
                    signal.signal(signal.SIGINT, signal_handler)

                    hpcarch = as_conf.get_platform()
                    safetysleeptime = as_conf.get_safetysleeptime()
                    retrials = as_conf.get_retrials()
                    submitter = Autosubmit._get_submitter(as_conf)
                    submitter.load_platforms(as_conf)
                    Log.debug("The Experiment name is: {0}", expid)
                    Log.debug("Sleep: {0}", safetysleeptime)
                    Log.debug("Default retrials: {0}", retrials)
                    Log.info("Starting job submission...")
                    pkl_dir = os.path.join(
                        BasicConfig.LOCAL_ROOT_DIR, expid, 'pkl')
                    try:
                        job_list = Autosubmit.load_job_list(
                            expid, as_conf, notransitive=notransitive)
                    except IOError as e:
                        raise AutosubmitError(
                            "Job_list not found", 6016, e.message)
                    except BaseException as e:
                        raise AutosubmitCritical(
                            "Corrupted job_list, backup couldn't be restored", 7040, e.message)

                    Log.debug(
                        "Starting from job list restored from {0} files", pkl_dir)
                    Log.debug("Length of the jobs list: {0}", len(job_list))
                    Autosubmit._load_parameters(
                        as_conf, job_list, submitter.platforms)
                    # check the job list script creation
                    Log.debug("Checking experiment templates...")
                    platforms_to_test = set()
                    for job in job_list.get_job_list():
                        if job.platform_name is None:
                            job.platform_name = hpcarch
                        # noinspection PyTypeChecker
                        try:
                            job.platform = submitter.platforms[job.platform_name.lower()]
                        except:
                            raise AutosubmitCritical(
                                "hpcarch={0} not found in the platforms configuration file".format(job.platform_name),
                                7014)
                        # noinspection PyTypeChecker
                        if job.status not in (Status.COMPLETED, Status.SUSPENDED):
                            platforms_to_test.add(job.platform)
                    try:
                        job_list.check_scripts(as_conf)
                    except Exception as e:
                        raise AutosubmitCritical(
                            "Error while checking job templates", 7015, str(e))
                    Log.debug("Loading job packages")
                    try:
                        packages_persistence = JobPackagePersistence(os.path.join(
                            BasicConfig.LOCAL_ROOT_DIR, expid, "pkl"), "job_packages_" + expid)
                    except IOError as e:
                        raise AutosubmitError(
                            "job_packages not found", 6016, e.message)
                    except BaseException as e:
                        raise AutosubmitCritical(
                            "Corrupted job_packages, python 2.7 and sqlite doesn't allow to restore these packages",
                            7040, e.message)
                    if as_conf.get_wrapper_type() != 'none':
                        os.chmod(os.path.join(BasicConfig.LOCAL_ROOT_DIR,
                                              expid, "pkl", "job_packages_" + expid + ".db"), 0644)
                        try:
                            packages = packages_persistence.load()
                        except IOError as e:
                            raise AutosubmitError(
                                "job_packages not found", 6016, e.message)
                        except BaseException as e:
                            raise AutosubmitCritical(
                                "Corrupted job_packages, python 2.7 and sqlite doesn't allow to restore these packages(will work on autosubmit4)",
                                7040, e.message)
                        Log.debug("Processing job packages")

                        try:
                            for (exp_id, package_name, job_name) in packages:
                                if package_name not in job_list.packages_dict:
                                    job_list.packages_dict[package_name] = []
                                job_list.packages_dict[package_name].append(
                                    job_list.get_job_by_name(job_name))
                            for package_name, jobs in job_list.packages_dict.items():
                                from job.job import WrapperJob
                                wrapper_status = Status.SUBMITTED
                                all_completed = True
                                running = False
                                queuing = False
                                failed = False
                                hold = False
                                submitted = False
                                if jobs[0].status == Status.RUNNING or jobs[0].status == Status.COMPLETED:
                                    running = True
                                for job in jobs:
                                    if job.status == Status.QUEUING:
                                        queuing = True
                                        all_completed = False
                                    elif job.status == Status.FAILED:
                                        failed = True
                                        all_completed = False
                                    elif job.status == Status.HELD:
                                        hold = True
                                        all_completed = False
                                    elif job.status == Status.SUBMITTED:
                                        submitted = True
                                        all_completed = False
                                if all_completed:
                                    wrapper_status = Status.COMPLETED
                                elif hold:
                                    wrapper_status = Status.HELD
                                else:
                                    if running:
                                        wrapper_status = Status.RUNNING
                                    elif queuing:
                                        wrapper_status = Status.QUEUING
                                    elif submitted:
                                        wrapper_status = Status.SUBMITTED
                                    elif failed:
                                        wrapper_status = Status.FAILED
                                    else:
                                        wrapper_status = Status.SUBMITTED
                                wrapper_job = WrapperJob(package_name, jobs[0].id, wrapper_status, 0, jobs,
                                                         None,
                                                         None, jobs[0].platform, as_conf, jobs[0].hold)
                                job_list.job_package_map[jobs[0].id] = wrapper_job
                        except Exception as e:
                            raise AutosubmitCritical(
                                "Autosubmit failed while processing job packages. This might be due to a change in your experiment configuration files after 'autosubmit create' was performed.",
                                7014, str(e))

                    Log.debug("Checking job_list current status")
                    job_list.update_list(as_conf, first_time=True)
                    job_list.save()

                    Log.info("Autosubmit is running with v{0}", Autosubmit.autosubmit_version)
                    # Before starting main loop, setup historical database tables and main information
                    Log.debug("Running job data structure")
                    try:
                        # Historical Database: Can create a new run if there is a difference in the number of jobs or if the current run does not exist.
                        exp_history = ExperimentHistory(expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR,
                                                        historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR)
                        exp_history.initialize_database()
                        exp_history.process_status_changes(job_list.get_job_list(), as_conf.get_chunk_size_unit(),
                                                           as_conf.get_chunk_size(),
                                                           current_config=as_conf.get_full_config_as_json())
                        Autosubmit.database_backup(expid)
                    except Exception as e:
                        try:
                            Autosubmit.database_fix(expid)
                            # This error is important
                        except:
                            pass
                    try:
                        ExperimentStatus(expid).set_as_running()
                    except Exception as e:
                        # Connection to status database ec_earth.db can fail.
                        # API worker will fix the status.
                        Log.debug(
                            "Autosubmit couldn't set your experiment as running on the autosubmit times database: {1}. Exception: {0}".format(
                                str(e), os.path.join(BasicConfig.DB_DIR, BasicConfig.AS_TIMES_DB)), 7003)
                    if allowed_members:
                        # Set allowed members after checks have been performed. This triggers the setter and main logic of the -rm feature.
                        job_list.run_members = allowed_members
                        Log.result(
                            "Only jobs with member value in {0} or no member will be allowed in this run. Also, those jobs already SUBMITTED, QUEUING, or RUNNING will be allowed to complete and will be tracked.".format(
                                str(allowed_members)))
                except AutosubmitCritical as e:
                    e.message += " HINT: check the CUSTOM_DIRECTIVE syntax in your jobs configuration files."
                    raise AutosubmitCritical(e.message, 7014, e.trace)
                except Exception as e:
                    raise AutosubmitCritical(
                        "Error in run initialization", 7014, str(e))  # Changing default to 7014
                # Related to TWO_STEP_START new variable defined in expdef
                unparsed_two_step_start = as_conf.get_parse_two_step_start()
                if unparsed_two_step_start != "":
                    job_list.parse_jobs_by_filter(unparsed_two_step_start)

                main_loop_retrials = 11250 * 2  # Hard limit of tries ( 48h min 72h max), 2 retrials per stop
                # establish the connection to all platforms

                Autosubmit.restore_platforms(platforms_to_test)
                save = True
                # @main
                Log.debug("Running main loop")
                #########################
                # AUTOSUBMIT - MAIN LOOP
                #########################
                # Main loop. Finishing when all jobs have been submitted

                while job_list.get_active():
                    # Log.info("FD: {0}".format(log.fd_show.fd_table_status_str()))
                    try:
                        if Autosubmit.exit:
                            # Closing threads on Ctrl+C
                            Log.info(
                                "Looking for active threads before closing Autosubmit. Ending the program before these threads finish may result in unexpected behavior. This procedure will last until all threads have finished or the program has waited for more than 30 seconds.")
                            timeout = 0
                            active_threads = True
                            all_threads = threading.enumerate()
                            while active_threads and timeout <= 60:
                                active_threads = False
                                for thread in all_threads:
                                    if "JOB_" in thread.name:
                                        if thread.isAlive():
                                            active_threads = True
                                            Log.info(
                                                "{0} is still retrieving outputs, time remaining is {1} seconds.".format(
                                                    thread.name, 60 - timeout))
                                            break
                                if active_threads:
                                    sleep(10)
                                    timeout += 10
                            return 0
                        # reload parameters changes
                        Log.debug("Reloading parameters...")
                        try:
                            Autosubmit._load_parameters(as_conf, job_list, submitter.platforms)
                            # Log.info("FD 2: {0}".format(log.fd_show.fd_table_status_str()))
                        except BaseException as e:
                            raise AutosubmitError("Config files seems to not be accesible", 6040, e.message)
                        total_jobs = len(job_list.get_job_list())
                        Log.info("\n\n{0} of {1} jobs remaining ({2})".format(
                            total_jobs - len(job_list.get_completed()), total_jobs, time.strftime("%H:%M")))
                        if len(job_list.get_failed()) > 0:
                            Log.info("{0} jobs has been  failed ({1})".format(
                                len(job_list.get_failed()), time.strftime("%H:%M")))
                        safetysleeptime = as_conf.get_safetysleeptime()
                        default_retrials = as_conf.get_retrials()
                        check_wrapper_jobs_sleeptime = as_conf.get_wrapper_check_time()
                        Log.debug("Sleep: {0}", safetysleeptime)
                        Log.debug("Number of retrials: {0}", default_retrials)
                        Log.debug('WRAPPER CHECK TIME = {0}'.format(
                            check_wrapper_jobs_sleeptime))
                        if save:  # previous iteration
                            job_list.backup_save()
                        save = False
                        slurm = []
                        job_changes_tracker = {}  # to easily keep track of changes per iteration
                        for platform in platforms_to_test:
                            list_jobid = ""
                            completed_joblist = []
                            list_prevStatus = []
                            queuing_jobs = job_list.get_in_queue_grouped_id(platform)
                            Log.debug('Checking jobs for platform={0}'.format(platform.name))
                            for job_id, job in queuing_jobs.items():
                                # Check Wrappers one-by-one
                                if job_list.job_package_map and job_id in job_list.job_package_map:
                                    wrapper_job = job_list.job_package_map[job_id]
                                    # Setting prev_status as an easy way to check status change for inner jobs
                                    if as_conf.get_notifications() == 'true':
                                        for inner_job in wrapper_job.job_list:
                                            inner_job.prev_status = inner_job.status
                                    check_wrapper = True
                                    if wrapper_job.status == Status.RUNNING:
                                        check_wrapper = True if datetime.timedelta.total_seconds(datetime.datetime.now(
                                        ) - wrapper_job.checked_time) >= check_wrapper_jobs_sleeptime else False
                                    if check_wrapper:
                                        Log.debug('Checking Wrapper {0}'.format(str(job_id)))
                                        wrapper_job.checked_time = datetime.datetime.now()
                                        # This is where wrapper will be checked on the slurm platform, update takes place.
                                        try:
                                            platform.check_job(wrapper_job, is_wrapper=True)
                                        except BaseException as e:
                                            job_list.save()
                                            raise AutosubmitError(
                                                "The communication with {0} went wrong while checking wrapper {1}\n{2}".format(
                                                    platform.name, wrapper_job.id, str(e)))
                                        # Log.info("FD 3Wrapper checked: {0}".format(log.fd_show.fd_table_status_str()))
                                        try:
                                            if wrapper_job.status != wrapper_job.new_status:
                                                Log.info('Wrapper job ' + wrapper_job.name + ' changed from ' + str(
                                                    Status.VALUE_TO_KEY[wrapper_job.status]) + ' to status ' + str(
                                                    Status.VALUE_TO_KEY[wrapper_job.new_status]))
                                                save = True
                                        except:
                                            raise AutosubmitCritical(
                                                "Wrapper is in Unknown Status couldn't get wrapper parameters", 7050)

                                        # New status will be saved and inner_jobs will be checked.
                                        try:
                                            wrapper_job.check_status(wrapper_job.new_status)
                                        except:
                                            job_list.save()
                                            raise AutosubmitError(
                                                "The communication with {0} went wrong while checking the inner_jobs of {1}\n{2}".format(
                                                    platform.name, wrapper_job.id, str(e)))

                                        # Erase from packages if the wrapper failed to be queued ( Hold Admin bug )
                                        if wrapper_job.status == Status.WAITING:
                                            for inner_job in wrapper_job.job_list:
                                                inner_job.packed = False
                                            job_list.job_package_map.pop(
                                                job_id, None)
                                            job_list.packages_dict.pop(
                                                job_id, None)
                                        save = True

                                    # Notifications e-mail
                                    if as_conf.get_notifications() == 'true':
                                        for inner_job in wrapper_job.job_list:
                                            if inner_job.prev_status != inner_job.status:
                                                if Status.VALUE_TO_KEY[inner_job.status] in inner_job.notify_on:
                                                    Notifier.notify_status_change(MailNotifier(BasicConfig), expid,
                                                                                  inner_job.name,
                                                                                  Status.VALUE_TO_KEY[
                                                                                      inner_job.prev_status],
                                                                                  Status.VALUE_TO_KEY[inner_job.status],
                                                                                  as_conf.get_mails_to())
                                    # Detect and store changes
                                    job_changes_tracker = {job.name: (
                                        job.prev_status, job.status) for job in wrapper_job.job_list if
                                        job.prev_status != job.status}
                                else:  # Prepare jobs, if slurm check all active jobs at once.
                                    # TODO: All of this should be a function, put in slurm_platform file, paramiko and ecmwf check_jobs to clean the code
                                    job = job[0]
                                    prev_status = job.status
                                    if job.status == Status.FAILED:
                                        continue
                                    # If exist key has been pressed and previous status was running, do not check
                                    if not (Autosubmit.exit is True and prev_status == Status.RUNNING):
                                        if platform.type == "slurm":  # List for add all jobs that will be checked
                                            # Do not check if Autosubmit exit is True and the previous status was running.
                                            # if not (Autosubmit.exit == True and prev_status == Status.RUNNING):
                                            list_jobid += str(job_id) + ','
                                            list_prevStatus.append(prev_status)
                                            completed_joblist.append(job)
                                        else:  # If they're not from slurm platform check one-by-one TODO: Implement ecwmf future platform and mnX, abstract this part
                                            platform.check_job(job)
                                            # Log.info("FD 4 check job: {0}".format(log.fd_show.fd_table_status_str()))
                                            if prev_status != job.update_status(
                                                    as_conf.get_copy_remote_logs() == 'true'):
                                                # Keeping track of changes
                                                job_changes_tracker[job.name] = (
                                                    prev_status, job.status)
                                                if as_conf.get_notifications() == 'true':
                                                    if Status.VALUE_TO_KEY[job.status] in job.notify_on:
                                                        Notifier.notify_status_change(MailNotifier(BasicConfig), expid,
                                                                                      job.name,
                                                                                      Status.VALUE_TO_KEY[prev_status],
                                                                                      Status.VALUE_TO_KEY[job.status],
                                                                                      as_conf.get_mails_to())
                                        save = True

                            # IF there are jobs in an slurm platform, prepare the check them at once
                            if platform.type == "slurm" and list_jobid != "":
                                slurm.append(
                                    [platform, list_jobid, list_prevStatus, completed_joblist])
                        # Check slurm single jobs, the other platforms has already been checked.
                        for platform_jobs in slurm:
                            platform = platform_jobs[0]
                            Log.debug("Checking all jobs at once")
                            platform.check_Alljobs(
                                platform_jobs[3], as_conf)
                            # Log.info("FD slurm jobs: {0}".format(log.fd_show.fd_table_status_str()))

                            for j_Indx in xrange(0, len(platform_jobs[3])):
                                prev_status = platform_jobs[2][j_Indx]
                                job = platform_jobs[3][j_Indx]
                                if prev_status != job.update_status(as_conf.get_copy_remote_logs() == 'true'):
                                    # Keeping track of changes
                                    job_changes_tracker[job.name] = (prev_status, job.status)
                                    if as_conf.get_notifications() == 'true':
                                        if Status.VALUE_TO_KEY[job.status] in job.notify_on:
                                            Notifier.notify_status_change(MailNotifier(BasicConfig), expid, job.name,
                                                                          Status.VALUE_TO_KEY[prev_status],
                                                                          Status.VALUE_TO_KEY[job.status],
                                                                          as_conf.get_mails_to())
                                save = True
                        Log.debug("End of checking")
                        # End Check Current jobs
                        save2 = job_list.update_list(
                            as_conf, submitter=submitter)
                        job_list.save()
                        if len(job_list.get_ready()) > 0:
                            Log.debug("Reloading configuration each Autosubmit iteration")
                            save = Autosubmit.submit_ready_jobs(
                                as_conf, job_list, platforms_to_test, packages_persistence, hold=False)
                            job_list.update_list(as_conf, submitter=submitter)
                            job_list.save()

                        if as_conf.get_remote_dependencies() and len(job_list.get_prepared()) > 0:
                            Autosubmit.submit_ready_jobs(
                                as_conf, job_list, platforms_to_test, packages_persistence, hold=True)
                            job_list.update_list(as_conf, submitter=submitter)
                            job_list.save()
                        # Safe spot to store changes
                        try:
                            exp_history = ExperimentHistory(expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR,
                                                            historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR)
                            if len(job_changes_tracker) > 0:
                                exp_history.process_job_list_changes_to_experiment_totals(job_list.get_job_list())
                                Autosubmit.database_backup(expid)
                        except BaseException as e:
                            Log.printlog("Historic database seems corrupted, AS will repair it and resume the run",
                                         Log.INFO)
                            try:
                                Autosubmit.database_fix(expid)
                                exp_history = ExperimentHistory(expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR,
                                                                historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR)
                                if len(job_changes_tracker) > 0:
                                    exp_history.process_job_list_changes_to_experiment_totals(job_list.get_job_list())
                                    Autosubmit.database_backup(expid)
                            except:
                                Log.warning(
                                    "Couldn't recover the Historical database, AS will continue without it, GUI may be affected")
                        job_changes_tracker = {}
                        if Autosubmit.exit:
                            job_list.save()
                        time.sleep(safetysleeptime)

                    except AutosubmitError as e:  # If an error is detected, restore all connections and job_list
                        Log.error("Trace: {0}", e.trace)
                        Log.error("{1} [eCode={0}]", e.code, e.message)
                        # Log.debug("FD recovery: {0}".format(log.fd_show.fd_table_status_str()))
                        # No need to wait until the remote platform reconnection
                        recovery = False
                        as_conf = AutosubmitConfig(expid, BasicConfig, ConfigParserFactory())
                        consecutive_retrials = 1
                        failed_names = {}
                        Log.info("Storing failed job count...")
                        try:
                            for job in job_list.get_job_list():
                                if job.fail_count > 0:
                                    failed_names[job.name] = job.fail_count
                        except BaseException as e:
                            Log.printlog("Error trying to store failed job count", Log.WARNING)
                        Log.result("Storing failed job count...done")
                        while not recovery and main_loop_retrials > 0:
                            delay = min(15 * consecutive_retrials, 30)
                            main_loop_retrials = main_loop_retrials - 1
                            sleep(delay)
                            consecutive_retrials = consecutive_retrials + 1
                            Log.info("Waiting {0} seconds before continue".format(delay))
                            try:
                                as_conf.reload()
                                Log.info("Recovering job_list...")
                                job_list = Autosubmit.load_job_list(
                                    expid, as_conf, notransitive=notransitive)
                                Log.info("Recovering job_list... Done")
                                if allowed_members:
                                    # Set allowed members after checks have been performed. This triggers the setter and main logic of the -rm feature.
                                    job_list.run_members = allowed_members
                                    Log.result(
                                        "Only jobs with member value in {0} or no member will be allowed in this run. Also, those jobs already SUBMITTED, QUEUING, or RUNNING will be allowed to complete and will be tracked.".format(
                                            str(allowed_members)))
                                platforms_to_test = set()
                                Log.info("Recovering platform information...")
                                for job in job_list.get_job_list():
                                    if job.platform_name is None:
                                        job.platform_name = hpcarch
                                    job.platform = submitter.platforms[job.platform_name.lower()]
                                    platforms_to_test.add(job.platform)

                                Log.info("Recovering platform information... Done")
                                Log.info("Recovering Failure count...")
                                for job in job_list.get_job_list():
                                    if job.name in failed_names.keys():
                                        job.fail_count = failed_names[job.name]
                                Log.info("Recovering Failure count... Done")

                                Log.info("Recovering parameters...")
                                Autosubmit._load_parameters(as_conf, job_list, submitter.platforms)
                                # Recovery wrapper [Packages]
                                Log.info("Recovering Wrappers...")
                                packages_persistence = JobPackagePersistence(
                                    os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl"), "job_packages_" + expid)
                                packages = packages_persistence.load()
                                for (exp_id, package_name, job_name) in packages:
                                    if package_name not in job_list.packages_dict:
                                        job_list.packages_dict[package_name] = []
                                    job_list.packages_dict[package_name].append(
                                        job_list.get_job_by_name(job_name))
                                # Recovery wrappers [Wrapper status]
                                for package_name, jobs in job_list.packages_dict.items():
                                    from job.job import WrapperJob
                                    wrapper_status = Status.SUBMITTED
                                    all_completed = True
                                    running = False
                                    queuing = False
                                    failed = False
                                    hold = False
                                    submitted = False
                                    if jobs[0].status == Status.RUNNING or jobs[0].status == Status.COMPLETED:
                                        running = True
                                    for job in jobs:
                                        if job.status == Status.QUEUING:
                                            queuing = True
                                            all_completed = False
                                        elif job.status == Status.FAILED:
                                            failed = True
                                            all_completed = False
                                        elif job.status == Status.HELD:
                                            hold = True
                                            all_completed = False
                                        elif job.status == Status.SUBMITTED:
                                            submitted = True
                                            all_completed = False
                                    if all_completed:
                                        wrapper_status = Status.COMPLETED
                                    elif hold:
                                        wrapper_status = Status.HELD
                                    else:
                                        if running:
                                            wrapper_status = Status.RUNNING
                                        elif queuing:
                                            wrapper_status = Status.QUEUING
                                        elif submitted:
                                            wrapper_status = Status.SUBMITTED
                                        elif failed:
                                            wrapper_status = Status.FAILED
                                        else:
                                            wrapper_status = Status.SUBMITTED
                                    wrapper_job = WrapperJob(package_name, jobs[0].id, wrapper_status, 0, jobs,
                                                             None,
                                                             None, jobs[0].platform, as_conf, jobs[0].hold)
                                    job_list.job_package_map[jobs[0].id] = wrapper_job
                                Log.info("Recovering wrappers... Done")
                                job_list.update_list(as_conf)
                                Log.info("Saving recovered job list...")
                                job_list.save()
                                Log.info("Saving recovered job list... Done")
                                recovery = True
                                Log.result("Recover of job_list is completed")
                            except AutosubmitError as e:
                                recovery = False
                                Log.result("Recover of job_list has fail {0}".format(e.message))
                            except IOError as e:
                                recovery = False
                                Log.result("Recover of job_list has fail {0}".format(e.message))
                            except BaseException as e:
                                recovery = False
                                Log.result("Recover of job_list has fail {0}".format(e.message))
                        # Restore platforms and try again, to avoid endless loop with failed configuration, a hard limit is set.
                        reconnected = False
                        mail_notify = True
                        times = 0
                        max = 10
                        Log.info("Restoring the connection to all experiment platforms")
                        consecutive_retrials = 1
                        delay = min(15 * consecutive_retrials, 120)
                        while not reconnected and main_loop_retrials > 0:
                            main_loop_retrials = main_loop_retrials - 1
                            Log.info("Recovering the remote platform connection")
                            Log.info("Waiting {0} seconds before continue".format(delay))
                            sleep(delay)
                            consecutive_retrials = consecutive_retrials + 1
                            try:
                                if times % max == 0:
                                    mail_notify = True
                                    max = max + max
                                    times = 0
                                else:
                                    mail_notify = False
                                times = times + 1
                                Autosubmit.restore_platforms(platforms_to_test, mail_notify=mail_notify,
                                                             as_conf=as_conf, expid=expid)
                                reconnected = True
                            except AutosubmitCritical as e:
                                # Message prompt by restore_platforms.
                                Log.info(
                                    "{0}\nCouldn't recover the platforms, retrying in 15seconds...".format(e.message))
                                reconnected = False
                            except IOError:
                                reconnected = False
                            except BaseException:
                                reconnected = False
                        if main_loop_retrials <= 0:
                            raise AutosubmitCritical(
                                "Autosubmit Encounter too much errors during running time, limit of {0} retrials reached".format(
                                    main_loop_retrials), 7051, e.message)
                    except AutosubmitCritical as e:  # Critical errors can't be recovered. Failed configuration or autosubmit error
                        raise AutosubmitCritical(e.message, e.code, e.trace)
                    except portalocker.AlreadyLocked:
                        message = "We have detected that there is another Autosubmit instance using the experiment\n. Stop other Autosubmit instances that are using the experiment or delete autosubmit.lock file located on tmp folder"
                        raise AutosubmitCritical(message, 7000)
                    except BaseException as e:  # If this happens, there is a bug in the code or an exception not-well caught
                        raise AutosubmitCritical("There is a bug in the code, please contact via gitlab", 7070, str(e))
                Log.result("No more jobs to run.")
                # Updating job data header with current information when experiment ends
                try:
                    exp_history = ExperimentHistory(expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR,
                                                    historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR)
                    exp_history.process_job_list_changes_to_experiment_totals(job_list.get_job_list())
                    Autosubmit.database_backup(expid)
                except:
                    try:
                        Autosubmit.database_fix(expid)
                    except:
                        pass
                # Wait for all remaining threads of I/O, close remaining connections
                timeout = 0
                active_threads = True
                all_threads = threading.enumerate()
                while active_threads and timeout <= 180:
                    active_threads = False
                    for thread in all_threads:
                        if "JOB_" in thread.name:
                            if thread.isAlive():
                                active_threads = True
                                Log.info("{0} is still retrieving outputs, time remaining is {1} seconds.".format(
                                    thread.name, 180 - timeout))
                                break
                    if active_threads:
                        sleep(10)
                        timeout += 10
                for platform in platforms_to_test:
                    platform.closeConnection()
                if len(job_list.get_failed()) > 0:
                    Log.info("Some jobs have failed and reached maximum retrials")
                else:
                    Log.result("Run successful")
                    # Updating finish time for job data header
                    exp_history.finish_current_experiment_run()
        except portalocker.AlreadyLocked:
            message = "We have detected that there is another Autosubmit instance using the experiment\n. Stop other Autosubmit instances that are using the experiment or delete autosubmit.lock file located on tmp folder"
            raise AutosubmitCritical(message, 7000)
        except AutosubmitCritical as e:
            raise
        except BaseException as e:
            raise AutosubmitCritical("This seems like a bug in the code, please contact AS developers", 7070, str(e))

    @staticmethod
    def restore_platforms(platform_to_test, mail_notify=False, as_conf=None, expid=expid):
        Log.info("Checking the connection to all platforms in use")
        issues = ""
        platform_issues = ""
        ssh_config_issues = ""
        for platform in platform_to_test:
            platform_issues = ""
            try:
                message = platform.test_connection()
                if message is None:
                    message = "OK"
                if message != "OK":
                    if message.find("doesn't accept remote connections") != -1:
                        ssh_config_issues += message
                    elif message.find("Authentication failed") != -1:
                        ssh_config_issues += message + ". Please, check the user and project of this platform\nIf it is correct, try another host"
                    else:
                        ssh_config_issues += message + " this is an PARAMIKO SSHEXCEPTION: indicates that there is something incompatible in the ssh_config for host:{0}\n maybe you need to contact your sysadmin".format(
                            platform.host)
            except BaseException as e:
                try:
                    if mail_notify:
                        email = as_conf.get_mails_to()
                        if "@" in email[0]:
                            Notifier.notify_experiment_status(MailNotifier(BasicConfig), expid, email, platform)
                except:
                    pass
                platform_issues += "\n[{1}] Connection Unsuccessful to host {0} ".format(
                    platform.host, platform.name)
                issues += platform_issues
                continue
            if platform.check_remote_permissions():
                Log.result("[{1}] Correct user privileges for host {0}",
                           platform.host, platform.name)
            else:
                platform_issues += "\n[{0}] has configuration issues.\n Check that the connection is passwd-less.(ssh {1}@{4})\n Check the parameters that build the root_path are correct:{{scratch_dir/project/user}} = {{{3}/{2}/{1}}}".format(
                    platform.name, platform.user, platform.project_dir, platform.scratch, platform.host)
                issues += platform_issues
            # Checks if bashrc is provinding output that could mess with Autosubmit remote pooling, if so, warns the user but continues as Autosubmit should be able to strip the output
            platform.get_bashrc_output()
            if platform.bashrc_output != "" or platform.bashrc_err != "":
                Log.warning(
                    "Bashrc is providing output that could mess with Autosubmit remote pooling\nHINT: add [ -z \"$PS1\" ] && return. at the header of {1}:~/.bashrc".format(
                        platform.name, platform.host))
            if platform_issues == "":
                Log.result("[{1}] Connection successful to host {0}", platform.host, platform.name)
            else:
                if platform.connected:
                    platform.connected = False
                    Log.printlog(
                        "[{1}] Connection sucessful to host {0}, however there are issues with %HPCROOT%".format(
                            platform.host, platform.name),
                        Log.WARNING)
                else:
                    Log.printlog("[{1}] Connection failed to host {0}".format(platform.host, platform.name),
                                 Log.WARNING)
        if issues != "":
            raise AutosubmitCritical(
                "Issues while checking the connectivity of platforms.", 7010, issues + "\n" + ssh_config_issues)

    @staticmethod
    def submit_ready_jobs(as_conf, job_list, platforms_to_test, packages_persistence, inspect=False,
                          only_wrappers=False, hold=False):
        # type: (AutosubmitConfig, JobList, Set[Platform], JobPackagePersistence, bool, bool, bool) -> bool
        """
        Gets READY jobs and send them to the platforms if there is available space on the queues

        :param as_conf: autosubmit config object \n
        :type as_conf: AutosubmitConfig object  \n
        :param job_list: job list to check  \n
        :type job_list: JobList object  \n
        :param platforms_to_test: platforms used  \n
        :type platforms_to_test: set of Platform Objects, e.g. SgePlatform(), LsfPlatform().  \n
        :param packages_persistence: Handles database per experiment. \n
        :type packages_persistence: JobPackagePersistence object \n
        :param inspect: True if coming from generate_scripts_andor_wrappers(). \n
        :type inspect: Boolean \n
        :param only_wrappers: True if it comes from create -cw, False if it comes from inspect -cw. \n
        :type only_wrappers: Boolean \n
        :return: True if at least one job was submitted, False otherwise \n
        :rtype: Boolean
        """
        save = False
        failed_packages = list()
        error_message = ""
        for platform in platforms_to_test:
            if not inspect:
                job_list.save()
            if not hold:
                Log.debug("\nJobs ready for {1}: {0}", len(
                    job_list.get_ready(platform, hold=hold)), platform.name)
                ready_jobs = job_list.get_ready(platform, hold=hold)
            else:
                Log.debug("\nJobs prepared for {1}: {0}", len(
                    job_list.get_prepared(platform)), platform.name)
            job_list.update_list(as_conf, False)
            packages_to_submit = JobPackager(
                as_conf, platform, job_list, hold=hold).build_packages()
            if not inspect:
                platform.open_submit_script()
            valid_packages_to_submit = []  # type: List[JobPackageBase]
            for package in packages_to_submit:
                try:

                    # If called from inspect command or -cw
                    if only_wrappers or inspect:
                        if hasattr(package, "name"):
                            job_list.packages_dict[package.name] = package.jobs
                            from job.job import WrapperJob
                            wrapper_job = WrapperJob(package.name, package.jobs[0].id, Status.READY, 0,
                                                     package.jobs,
                                                     package._wallclock, package._num_processors,
                                                     package.platform, as_conf, hold)
                            job_list.job_package_map[package.jobs[0].id] = wrapper_job
                            packages_persistence.save(
                                package.name, package.jobs, package._expid, inspect)
                        for innerJob in package._jobs:
                            # Setting status to COMPLETED so it does not get stuck in the loop that calls this function
                            innerJob.status = Status.COMPLETED

                    # If called from RUN or inspect command
                    if not only_wrappers:
                        try:
                            package.submit(as_conf, job_list.parameters, inspect, hold=hold)
                            save = True
                            if not inspect:
                                if str(platform.type).lower() != "slurm":
                                    job_list.update_list(as_conf)
                                    job_list.save()
                            valid_packages_to_submit.append(package)
                        except (IOError, OSError):
                            if package.jobs[0].id != 0:
                                failed_packages.append(package.jobs[0].id)
                            continue
                        except AutosubmitError as e:
                            if package.jobs[0].id != 0:
                                failed_packages.append(package.jobs[0].id)
                            platform.connected = False
                            if str(e.trace).lower().find("bad parameters") != -1 or str(e.message).lower().find(
                                    "scheduler is not installed") != -1:
                                error_msg = ""
                                for package_tmp in valid_packages_to_submit:
                                    for job_tmp in package_tmp.jobs:
                                        if job_tmp.section not in error_msg:
                                            error_msg += job_tmp.section + "&"
                                for job_tmp in package.jobs:
                                    if job_tmp.section not in error_msg:
                                        error_msg += job_tmp.section + "&"
                                if str(e.trace).lower().find("bad parameters") != -1:
                                    error_message += "\ncheck job and queue specified in jobs.conf. Sections that could be affected: {0}".format(
                                        error_msg[:-1])
                                else:
                                    error_message += "\ncheck that {1} platform has set the correct scheduler. Sections that could be affected: {0}".format(
                                        error_msg[:-1], platform.name)
                        except WrongTemplateException as e:
                            raise AutosubmitCritical("Invalid parameter substitution in {0} template".format(
                                e.job_name), 7014, e.message)
                        except AutosubmitCritical:
                            raise
                        except Exception as e:
                            platform.connected = False
                            raise AutosubmitError(
                                "{0} submission failed. May be related to running a job with check=on_submission and another that affect this job template".format(
                                    platform.name), 6015, str(e))
                except WrongTemplateException as e:
                    raise AutosubmitCritical(
                        "Invalid parameter substitution in {0} template".format(e.job_name), 7014)
                except AutosubmitCritical as e:
                    raise AutosubmitCritical(e.message, e.code, e.trace)
                except AutosubmitError as e:
                    raise
                except Exception as e:
                    raise
            if str(platform.type).lower() in ["slurm", "pjm"] and not inspect and not only_wrappers:
                try:
                    valid_packages_to_submit = [package for package in valid_packages_to_submit if package.x11 != True]
                    if len(valid_packages_to_submit) > 0:
                        submit_time = int(time.time() / 60)
                        try:
                            jobs_id = platform.submit_Script(hold=hold)
                        except AutosubmitError as e:
                            if not e.message:
                                e.message = ""
                            try:
                                for package in valid_packages_to_submit:
                                    try:
                                        elapsed_time_minutes = str(int(round(int(time.time() / 60) - submit_time) + 1))
                                        job_historic = platform.get_jobid_by_jobname(package.jobs[0].name,
                                                                                     minutes=elapsed_time_minutes)
                                    except:
                                        job_historic = []
                                    # Recover jobid from jobname
                                    if len(job_historic) > 0 and isinstance(job_historic, list):
                                        job_id = job_historic[-1]
                                        for job_id_historic in job_historic:
                                            if job_id_historic != job_id:
                                                try:
                                                    platform.send_command(
                                                        platform.cancel_cmd + " {0}".format(job_id_historic))
                                                except:
                                                    pass
                                        for job in package.jobs:
                                            job.hold = hold
                                            job.id = str(job_id)
                                            job.status = Status.SUBMITTED
                                            job.write_submit_time(hold=hold)
                            except:
                                pass
                            job_list.save()
                            job_list.update_list(as_conf, store_change=True)
                            jobs_id = None
                            platform.connected = False
                            if e.trace is not None:
                                has_trace_bad_parameters = str(e.trace).lower().find("bad parameters") != -1
                            else:
                                e.trace = ""
                                has_trace_bad_parameters = False
                            if has_trace_bad_parameters or e.message.lower().find(
                                    "invalid partition") != -1 or e.message.lower().find(
                                "invalid qos") != -1 or e.message.lower().find(
                                "scheduler is not installed") != -1 or e.message.lower().find(
                                "failed") != -1 or e.message.lower().find("not available") != -1:
                                error_msg = ""
                                for package_tmp in valid_packages_to_submit:
                                    for job_tmp in package_tmp.jobs:
                                        if job_tmp.section not in error_msg:
                                            error_msg += job_tmp.section + "&"
                                if has_trace_bad_parameters:
                                    error_message += "Check job and queue specified in jobs.conf. Sections that could be affected: {0}".format(
                                        error_msg[:-1])
                                else:
                                    error_message += "Check that {1} platform has set the correct scheduler. Sections that could be affected: {0}".format(
                                        error_msg[:-1], platform.name)
                                if e.trace is None:
                                    e.trace = ""
                                raise AutosubmitCritical(error_message, 7014, e.message + "\n" + str(e.trace))
                        except IOError as e:
                            raise AutosubmitError(
                                "IO issues ", 6016, e.message)
                        except BaseException as e:
                            if e.message.find("scheduler") != -1:
                                raise AutosubmitCritical(
                                    "Are you sure that [{0}] scheduler is the correct type for platform [{1}]?.\n Please, double check that {0} is loaded for {1} before autosubmit launch any job.".format(
                                        platform.type.upper(), platform.name.upper()), 7070)
                            raise AutosubmitError(
                                "Submission failed, this can be due a failure on the platform", 6015, e.message)
                        if jobs_id is None or len(jobs_id) <= 0:
                            try:
                                jobnames = []
                                for package in valid_packages_to_submit:
                                    jobnames += [job.name for job in package.jobs]
                                for jobname in jobnames:
                                    jobid = platform.get_jobid_by_jobname(jobname)
                                    # cancel bad submitted job if jobid is encountered
                                    for id in jobid:
                                        platform.cancel_job(id)
                            except:
                                pass
                            platform.connected = False

                            raise AutosubmitError(
                                "Submission failed, this can be due a failure on the platform\n{0}\n{1}".format(
                                    str(platform.name), ""), 6015)
                        i = 0
                        if hold:
                            sleep(10)
                        for package in valid_packages_to_submit:
                            if hold:
                                retries = 5
                                package.jobs[0].id = str(jobs_id[i])
                                try:
                                    can_continue = True
                                    while can_continue and retries > 0:
                                        cmd = package.jobs[0].platform.get_queue_status_cmd(jobs_id[i])
                                        package.jobs[0].platform.send_command(cmd)
                                        queue_status = package.jobs[0].platform._ssh_output
                                        reason = package.jobs[0].platform.parse_queue_reason(queue_status, jobs_id[i])
                                        if reason == '(JobHeldAdmin)':
                                            can_continue = False
                                        elif reason == '(JobHeldUser)':
                                            can_continue = True
                                        else:
                                            can_continue = False
                                            sleep(5)
                                        retries = retries - 1
                                    if not can_continue:
                                        package.jobs[0].platform.send_command(
                                            package.jobs[0].platform.cancel_cmd + " {0}".format(jobs_id[i]))
                                        i = i + 1
                                        continue
                                    if not platform.hold_job(package.jobs[0]):
                                        i = i + 1
                                        continue
                                except Exception as e:
                                    failed_packages.append(jobs_id)
                                    continue
                            for job in package.jobs:
                                job.hold = hold
                                job.id = str(jobs_id[i])
                                job.status = Status.SUBMITTED
                                job.write_submit_time(hold=hold)
                            i += 1
                    if not inspect:
                        job_list.save()
                    if len(failed_packages) > 0:
                        try:
                            for job_id in failed_packages:
                                platform.send_command(platform.cancel_cmd + " {0}".format(job_id))
                        except:
                            pass
                        raise AutosubmitError(
                            "{0} submission failed, some hold jobs failed to be held".format(platform.name), 6015)
                except WrongTemplateException as e:
                    raise AutosubmitCritical("Invalid parameter substitution in {0} template".format(
                        e.job_name), 7014, str(e))
                except AutosubmitError as e:
                    raise
                except AutosubmitCritical as e:
                    raise
                except Exception as e:
                    raise AutosubmitError("{0} submission failed".format(platform.name), 6015, str(e))

            for package in valid_packages_to_submit:
                if package.jobs[0].id not in failed_packages:
                    if hasattr(package, "name"):
                        job_list.packages_dict[package.name] = package.jobs
                        from job.job import WrapperJob
                        wrapper_job = WrapperJob(package.name, package.jobs[0].id, Status.SUBMITTED, 0,
                                                 package.jobs,
                                                 package._wallclock, package._num_processors,
                                                 package.platform, as_conf, hold)
                        job_list.job_package_map[package.jobs[0].id] = wrapper_job
                        if isinstance(package, JobPackageThread):
                            # Saving only when it is a real multi job package
                            packages_persistence.save(
                                package.name, package.jobs, package._expid, inspect)
            if not inspect:
                job_list.save()
            if error_message != "":
                raise AutosubmitCritical("Submission Failed due wrong configuration:{0}".format(error_message), 7015)
        return save

    @staticmethod
    def monitor(expid, file_format, lst, filter_chunks, filter_status, filter_section, hide, txt_only=False,
                group_by=None, expand="", expand_status=list(), hide_groups=False, notransitive=False,
                check_wrapper=False, txt_logfiles=False, detail=False):
        """
        Plots workflow graph for a given experiment with status of each job coded by node color.
        Plot is created in experiment's plot folder with name <expid>_<date>_<time>.<file_format>

        :type file_format: str
        :type expid: str
        :param expid: identifier of the experiment to plot
        :param file_format: plot's file format. It can be pdf, png, ps or svg
        :param lst: list of jobs to change status
        :type lst: str
        :param filter_chunks: chunks to change status
        :type filter_chunks: str
        :param filter_status: current status of the jobs to change status
        :type filter_status: str
        :param filter_section: sections to change status
        :type filter_section: str
        :param hide: hides plot window
        :type hide: bool
        :param txt_only: workflow will only be written as text
        :type txt_only: bool
        :param group_by: workflow will only be written as text
        :type group_by: bool
        :param expand: Filtering of jobs for it's visualization
        :type expand: str
        :param expand_status: Filtering of jobs for it's visualization
        :type expand_status: str
        :param hide_groups: Simplified workflow illustration by encapsulating the jobs.
        :type hide_groups: bool
        :param notransitive: workflow will only be written as text
        :type notransitive: bool
        :param check_wrapper: Shows a preview of how the wrappers will look
        :type check_wrapper: bool
        :param notransitive: Some dependencies will be omitted
        :type notransitive: bool
        :param detail: better text format representation but more expensive
        :type detail: bool



        """
        try:
            exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid)
            Log.info("Getting job list...")
            as_conf = AutosubmitConfig(expid, BasicConfig, ConfigParserFactory())
            as_conf.check_conf_files(False)
            # Getting output type from configuration
            output_type = as_conf.get_output_type()
            pkl_dir = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, 'pkl')
            job_list = Autosubmit.load_job_list(
                expid, as_conf, notransitive=notransitive, monitor=True)
            Log.debug("Job list restored from {0} files", pkl_dir)
        except AutosubmitError as e:
            raise AutosubmitCritical(e.message, e.code, e.trace)
        except AutosubmitCritical as e:
            raise
        except BaseException as e:
            raise AutosubmitCritical("Error while checking the configuration files or loading the job_list", 7040,
                                     e.message)
        try:
            jobs = []
            if not isinstance(job_list, type([])):
                if filter_chunks:
                    fc = filter_chunks
                    Log.debug(fc)

                    if fc == 'Any':
                        jobs = job_list.get_job_list()
                    else:
                        # noinspection PyTypeChecker
                        data = json.loads(Autosubmit._create_json(fc))
                        for date_json in data['sds']:
                            date = date_json['sd']
                            jobs_date = filter(lambda j: date2str(
                                j.date) == date, job_list.get_job_list())

                            for member_json in date_json['ms']:
                                member = member_json['m']
                                jobs_member = filter(
                                    lambda j: j.member == member, jobs_date)

                                for chunk_json in member_json['cs']:
                                    chunk = int(chunk_json)
                                    jobs = jobs + \
                                           [job for job in filter(
                                               lambda j: j.chunk == chunk, jobs_member)]

                elif filter_status:
                    Log.debug("Filtering jobs with status {0}", filter_status)
                    if filter_status == 'Any':
                        jobs = job_list.get_job_list()
                    else:
                        fs = Autosubmit._get_status(filter_status)
                        jobs = [job for job in filter(
                            lambda j: j.status == fs, job_list.get_job_list())]

                elif filter_section:
                    ft = filter_section
                    Log.debug(ft)

                    if ft == 'Any':
                        jobs = job_list.get_job_list()
                    else:
                        for job in job_list.get_job_list():
                            if job.section == ft:
                                jobs.append(job)

                elif lst:
                    jobs_lst = lst.split()

                    if jobs == 'Any':
                        jobs = job_list.get_job_list()
                    else:
                        for job in job_list.get_job_list():
                            if job.name in jobs_lst:
                                jobs.append(job)
                else:
                    jobs = job_list.get_job_list()
        except BaseException as e:
            raise AutosubmitCritical("Issues during the job_list generation. Maybe due I/O error", 7040, e.message)

        referenced_jobs_to_remove = set()
        for job in jobs:
            for child in job.children:
                if child not in jobs:
                    referenced_jobs_to_remove.add(child)
            for parent in job.parents:
                if parent not in jobs:
                    referenced_jobs_to_remove.add(parent)
        if len(referenced_jobs_to_remove) > 0:
            for job in jobs:
                job.children = job.children - referenced_jobs_to_remove
                job.parents = job.parents - referenced_jobs_to_remove
        # WRAPPERS
        try:
            if as_conf.get_wrapper_type() != 'none' and check_wrapper:
                # Class constructor creates table if it does not exist
                packages_persistence = JobPackagePersistence(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl"),
                                                             "job_packages_" + expid)
                # Permissons
                os.chmod(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl", "job_packages_" + expid + ".db"), 0644)
                # Database modification
                packages_persistence.reset_table(True)
                referenced_jobs_to_remove = set()
                job_list_wrappers = copy.deepcopy(job_list)
                jobs_wr_aux = copy.deepcopy(jobs)
                jobs_wr = []
                [jobs_wr.append(job) for job in jobs_wr_aux]
                for job in jobs_wr:
                    for child in job.children:
                        if child not in jobs_wr:
                            referenced_jobs_to_remove.add(child)
                    for parent in job.parents:
                        if parent not in jobs_wr:
                            referenced_jobs_to_remove.add(parent)

                for job in jobs_wr:
                    job.children = job.children - referenced_jobs_to_remove
                    job.parents = job.parents - referenced_jobs_to_remove

                try:
                    Autosubmit.generate_scripts_andor_wrappers(as_conf, job_list_wrappers, jobs_wr,
                                                               packages_persistence, True)
                except AutosubmitCritical as e:
                    Log.printlog("Incomplete preview of wrappers", 5000)
                    Log.printlog(e.message, 7000)

                packages = packages_persistence.load(True)
                packages += JobPackagePersistence(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl"),
                                                  "job_packages_" + expid).load()
            else:
                packages = JobPackagePersistence(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl"),
                                                 "job_packages_" + expid).load()
        except BaseException as e:
            raise AutosubmitCritical("Issues during the wrapper loading, may be related to IOissues", 7040, e.message)
        groups_dict = dict()
        try:
            if group_by:
                status = list()
                if expand_status:
                    for s in expand_status.split():
                        status.append(Autosubmit._get_status(s.upper()))

                job_grouping = JobGrouping(group_by, copy.deepcopy(
                    jobs), job_list, expand_list=expand, expanded_status=status)
                groups_dict = job_grouping.group_jobs()
        except BaseException as e:
            raise AutosubmitCritical(
                "Jobs can't be grouped, perhaps you're using an invalid format. Take a look into readthedocs", 7011,
                e.message)

        monitor_exp = Monitor()
        try:
            if txt_only or txt_logfiles or file_format == "txt":
                monitor_exp.generate_output_txt(expid, jobs, os.path.join(
                    exp_path, "/tmp/LOG_" + expid), txt_logfiles, job_list_object=job_list)
                if txt_only:
                    current_length = len(job_list.get_job_list())
                    if current_length > 1000:
                        Log.info(
                            "Experiment has too many jobs to be printed in the terminal. Maximum job quantity is 1000, your experiment has " + str(
                                current_length) + " jobs.")
                    else:
                        Log.info(job_list.print_with_status())
            else:
                # if file_format is set, use file_format, otherwise use conf value
                monitor_exp.generate_output(expid,
                                            jobs,
                                            os.path.join(
                                                exp_path, "/tmp/LOG_", expid),
                                            output_format=file_format if file_format is not None else output_type,
                                            packages=packages,
                                            show=not hide,
                                            groups=groups_dict,
                                            hide_groups=hide_groups,
                                            job_list_object=job_list)
        except BaseException as e:
            raise AutosubmitCritical(
                "An error has occurred while printing the workflow status. Check if you have X11 redirection and an img viewer correctly set",
                7014, e.message)

        return True

    @staticmethod
    def statistics(expid, filter_type, filter_period, file_format, hide, notransitive=False):
        """
        Plots statistics graph for a given experiment.
        Plot is created in experiment's plot folder with name <expid>_<date>_<time>.<file_format>

        :type file_format: str
        :type expid: str
        :param expid: identifier of the experiment to plot
        :param filter_type: type of the jobs to plot
        :param filter_period: period to plot
        :param file_format: plot's file format. It can be pdf, png, ps or svg
        :param hide: hides plot window
        :type hide: bool
        :param notransitive: Reduces workflow linkage complexity
        :type hide: bool

        """
        try:
            Log.info("Loading jobs...")
            as_conf = AutosubmitConfig(expid, BasicConfig, ConfigParserFactory())
            as_conf.check_conf_files(False)

            pkl_dir = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, 'pkl')
            job_list = Autosubmit.load_job_list(expid, as_conf, notransitive=notransitive)
            Log.debug("Job list restored from {0} files", pkl_dir)
            jobs = StatisticsUtils.filter_by_section(job_list.get_job_list(), filter_type)
            jobs, period_ini, period_fi = StatisticsUtils.filter_by_time_period(jobs, filter_period)
            # Package information
            job_to_package, package_to_jobs, _, _ = JobList.retrieve_packages(BasicConfig, expid, [job.name for job in
                                                                                                   job_list.get_job_list()])
            queue_time_fixes = {}
            if (job_to_package):
                current_table_structure = get_structure(expid, BasicConfig.STRUCTURES_DIR)
                subjobs = []
                for job in job_list.get_job_list():
                    job_info = JobList.retrieve_times(job.status, job.name, job._tmp_path, make_exception=False,
                                                      job_times=None, seconds=True, job_data_collection=None)
                    time_total = (job_info.queue_time + job_info.run_time) if job_info else 0
                    subjobs.append(
                        SubJob(job.name,
                               job_to_package.get(job.name, None),
                               job_info.queue_time if job_info else 0,
                               job_info.run_time if job_info else 0,
                               time_total,
                               job_info.status if job_info else Status.UNKNOWN)
                    )
                queue_time_fixes = SubJobManager(subjobs, job_to_package, package_to_jobs,
                                                 current_table_structure).get_collection_of_fixes_applied()

            if len(jobs) > 0:
                try:
                    Log.info("Plotting stats...")
                    monitor_exp = Monitor()
                    # noinspection PyTypeChecker
                    monitor_exp.generate_output_stats(expid, jobs, file_format, period_ini, period_fi, not hide,
                                                      queue_time_fixes)
                    Log.result("Stats plot ready")
                except Exception as e:
                    raise AutosubmitCritical(
                        "Stats couldn't be shown", 7061, str(e))
            else:
                Log.info("There are no {0} jobs in the period from {1} to {2}...".format(
                    filter_type, period_ini, period_fi))
        except BaseException as e:
            raise AutosubmitCritical("Stats couldn't be generated. Check trace for more details", 7061, e.message)
        return True

    @staticmethod
    def clean(expid, project, plot, stats):
        """
        Clean experiment's directory to save storage space.
        It removes project directory and outdated plots or stats.

        :param create_log_file: if true, creates log file
        :type create_log_file: bool
        :type plot: bool
        :type project: bool
        :type expid: str
        :type stats: bool
        :param expid: identifier of experiment to clean
        :param project: set True to delete project directory
        :param plot: set True to delete outdated plots
        :param stats: set True to delete outdated stats
        """
        try:
            exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid)

            if project:
                autosubmit_config = AutosubmitConfig(
                    expid, BasicConfig, ConfigParserFactory())
                autosubmit_config.check_conf_files(False)

                project_type = autosubmit_config.get_project_type()
                if project_type == "git":
                    Log.info("Registering commit SHA...")
                    autosubmit_config.set_git_project_commit(autosubmit_config)
                    autosubmit_git = AutosubmitGit(expid[0])
                    Log.info("Cleaning GIT directory...")
                    if not autosubmit_git.clean_git(autosubmit_config):
                        return False
                else:
                    Log.info("No project to clean...\n")
            if plot:
                Log.info("Cleaning plots...")
                monitor_autosubmit = Monitor()
                monitor_autosubmit.clean_plot(expid)
            if stats:
                Log.info("Cleaning stats directory...")
                monitor_autosubmit = Monitor()
                monitor_autosubmit.clean_stats(expid)
        except BaseException as e:
            raise AutosubmitCritical("Couldn't clean this experiment, check if you have the correct permisions", 7012,
                                     e.message)
        return True

    @staticmethod
    def recovery(expid, noplot, save, all_jobs, hide, group_by=None, expand=list(), expand_status=list(),
                 notransitive=False, no_recover_logs=False, detail=False, force=False):
        """
        Method to check all active jobs. If COMPLETED file is found, job status will be changed to COMPLETED,
        otherwise it will be set to WAITING. It will also update the jobs list.

        :param expid: identifier of the experiment to recover
        :type expid: str
        :param save: If true, recovery saves changes to the jobs list
        :type save: bool
        :param all_jobs: if True, it tries to get completed files for all jobs, not only active.
        :type all_jobs: bool
        :param hide: hides plot window
        :type hide: bool
        :param force: Allows to restore the workflow even if there are running jobs
        :type force: bool
        """
        try:
            Autosubmit._check_ownership(expid, raise_error=True)

            exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid)
            pkl_folder_path = os.path.join(exp_path, "pkl")
            current_pkl_path = os.path.join(
                pkl_folder_path, "job_list_{}.pkl".format(expid))

            try:
                Log.info("Removing job_data db...")
                job_data_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, BasicConfig.JOBDATA_DIR,
                                             "job_data_{0}.db".format(expid))
                # Delete job_data if 0 bytes
                if os.path.exists(job_data_path) and os.stat(job_data_path).st_size <= 6:
                    Log.info("job_data db is corrupted. It will be removed.")
                    os.remove(job_data_path)
            except BaseException as e:
                Log.warning("Error removing job_data db: {0}, please remove it manually".format(e.message))
            as_conf = AutosubmitConfig(expid, BasicConfig, ConfigParserFactory())
            as_conf.check_conf_files(True)

            Log.info('Recovering experiment {0}'.format(expid))
            pkl_dir = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, 'pkl')
            job_list = Autosubmit.load_job_list(
                expid, as_conf, notransitive=notransitive, monitor=True)

            current_active_jobs = job_list.get_in_queue()

            as_conf.check_conf_files(False)

            # Getting output type provided by the user in config, 'pdf' as default
            output_type = as_conf.get_output_type()
            hpcarch = as_conf.get_platform()

            submitter = Autosubmit._get_submitter(as_conf)
            submitter.load_platforms(as_conf)
            if submitter.platforms is None:
                return False
            platforms = submitter.platforms

            platforms_to_test = set()
            if len(current_active_jobs) > 0:
                if force and save:
                    for job in current_active_jobs:
                        if job.platform_name is None:
                            job.platform_name = hpcarch
                        job.platform = submitter.platforms[job.platform_name.lower()]
                        platforms_to_test.add(job.platform)

                    for platform in platforms_to_test:
                        try:
                            platform.test_connection()
                        except:
                            Log.warning("Platform {0} could not be established, jobs of this platform won't be cancelled in the remote").format(platform.name)
                    for job in current_active_jobs:
                        try:
                            job.platform.send_command(job.platform.cancel_cmd + " " + str(job.id), ignore_log=True)
                        except:
                            Log.warning("Job {0} could not be cancelled, please cancel it manually".format(job.name))
                if not force:
                    raise AutosubmitCritical(
                        "Experiment can't be recovered due being {0} active jobs in your experiment, If you want to recover the experiment, please use the flag -f and all active jobs will be cancelled".format(
                            len(current_active_jobs)), 7000)
            Log.debug("Job list restored from {0} files", pkl_dir)
        except AutosubmitCritical as e:
            raise
        except BaseException as e:
            raise AutosubmitCritical(
                "Couldn't restore the job_list or packages, check if the filesystem is having issues", 7040, e.message)
        Log.info('Recovering experiment {0}'.format(expid))
        try:
            for job in job_list.get_job_list():
                job.submitter = submitter
                if job.platform_name is None:
                    job.platform_name = hpcarch
                # noinspection PyTypeChecker
                job.platform = platforms[job.platform_name.lower()]
                # noinspection PyTypeChecker
                platforms_to_test.add(platforms[job.platform_name.lower()])
            # establish the connection to all platforms
            Autosubmit.restore_platforms(platforms_to_test)

            if all_jobs:
                jobs_to_recover = job_list.get_job_list()
            else:
                jobs_to_recover = job_list.get_active()
        except BaseException as e:
            raise AutosubmitCritical(
                "Couldn't restore the experiment platform, check if the filesystem is having issues", 7040, e.message)

        Log.info("Looking for COMPLETED files")
        try:
            start = datetime.datetime.now()
            for job in jobs_to_recover:
                if job.platform_name is None:
                    job.platform_name = hpcarch
                # noinspection PyTypeChecker
                job.platform = platforms[job.platform_name.lower()]

                if job.platform.get_completed_files(job.name, 0, recovery=True):
                    job.status = Status.COMPLETED
                    Log.info(
                        "CHANGED job '{0}' status to COMPLETED".format(job.name))
                    # Log.status("CHANGED job '{0}' status to COMPLETED".format(job.name))

                    if not no_recover_logs:
                        try:
                            job.platform.get_logs_files(expid, job.remote_logs)
                        except:
                            pass
                elif job.status != Status.SUSPENDED:
                    job.status = Status.WAITING
                    job.fail_count = 0
                    # Log.info("CHANGED job '{0}' status to WAITING".format(job.name))
                    # Log.status("CHANGED job '{0}' status to WAITING".format(job.name))

            end = datetime.datetime.now()
            Log.info("Time spent: '{0}'".format(end - start))
            Log.info("Updating the jobs list")
            job_list.update_list(as_conf)

            if save:
                job_list.save()
            else:
                Log.warning(
                    'Changes NOT saved to the jobList. Use -s option to save')

            Log.result("Recovery finalized")
        except BaseException as e:
            raise AutosubmitCritical("Couldn't restore the experiment workflow", 7040, e.message)

        try:
            packages = JobPackagePersistence(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl"),
                                             "job_packages_" + expid).load()

            groups_dict = dict()
            if group_by:
                status = list()
                if expand_status:
                    for s in expand_status.split():
                        status.append(Autosubmit._get_status(s.upper()))

                job_grouping = JobGrouping(group_by, copy.deepcopy(job_list.get_job_list()), job_list,
                                           expand_list=expand,
                                           expanded_status=status)
                groups_dict = job_grouping.group_jobs()

            if not noplot:
                Log.info("\nPlotting the jobs list...")
                monitor_exp = Monitor()
                monitor_exp.generate_output(expid,
                                            job_list.get_job_list(),
                                            os.path.join(
                                                exp_path, "/tmp/LOG_", expid),
                                            output_format=output_type,
                                            packages=packages,
                                            show=not hide,
                                            groups=groups_dict,
                                            job_list_object=job_list)

            if detail == True:
                current_length = len(job_list.get_job_list())
                if current_length > 1000:
                    Log.warning(
                        "-d option: Experiment has too many jobs to be printed in the terminal. Maximum job quantity is 1000, your experiment has " + str(
                            current_length) + " jobs.")
                else:
                    Log.info(job_list.print_with_status())
                    Log.status(job_list.print_with_status())
            # Warnings about precedence completion
            # time_0 = time.time()
            notcompleted_parents_completed_jobs = [job for job in job_list.get_job_list(
            ) if job.status == Status.COMPLETED and len(
                [jobp for jobp in job.parents if jobp.status != Status.COMPLETED]) > 0]

            if notcompleted_parents_completed_jobs and len(notcompleted_parents_completed_jobs) > 0:
                Log.error(
                    "The following COMPLETED jobs depend on jobs that have not been COMPLETED (this can result in unexpected behavior): {0}".format(
                        str([job.name for job in notcompleted_parents_completed_jobs])))
            # print("Warning calc took {0} seconds".format(time.time() - time_0))
        except BaseException as e:
            raise AutosubmitCritical(
                "An error has occurred while printing the workflow status. Check if you have X11 redirection and an img viewer correctly set",
                7000, e.message)

        return True



    @staticmethod
    def migrate(experiment_id, offer, pickup, only_remote):
        """
        Migrates experiment files from current to other user.
        It takes mapping information for new user from config files.

        :param experiment_id: experiment identifier:
        :param pickup:
        :param offer:
        :param only_remote:
        """

        if offer:
            as_conf = AutosubmitConfig(
                experiment_id, BasicConfig, ConfigParserFactory())
            as_conf.check_conf_files(True)
            pkl_dir = os.path.join(
                BasicConfig.LOCAL_ROOT_DIR, experiment_id, 'pkl')
            job_list = Autosubmit.load_job_list(
                experiment_id, as_conf, notransitive=True, monitor=True)
            Log.debug("Job list restored from {0} files", pkl_dir)
            error = False
            platforms_to_test = set()
            submitter = Autosubmit._get_submitter(as_conf)
            submitter.load_platforms(as_conf)
            if submitter.platforms is None:
                raise AutosubmitCritical("No platforms configured!!!", 7014)
            platforms = submitter.platforms
            for job in job_list.get_job_list():
                job.submitter = submitter
                if job.platform_name is None:
                    job.platform_name = as_conf.get_platform()
                platforms_to_test.add(platforms[job.platform_name.lower()])
            # establish the connection to all platforms on use
            Autosubmit.restore_platforms(platforms_to_test)
            Log.info('Migrating experiment {0}'.format(experiment_id))
            Autosubmit._check_ownership(experiment_id, raise_error=True)
            if submitter.platforms is None:
                return False
            Log.info("Checking remote platforms")
            platforms = filter(lambda x: x not in [
                'local', 'LOCAL'], submitter.platforms)
            already_moved = set()
            backup_files = []
            backup_conf = []
            error = False
            err_message = 'Invalid Configuration:'
            for platform in platforms:
                # Checks
                Log.info(
                    "Checking [{0}] from platforms configuration...", platform)
                if as_conf.get_migrate_user_to(platform) == '':
                    err_message += "\nInvalid USER_TO target [ USER == USER_TO in [{0}] ]".format(
                        platform)
                    error = True
                elif not as_conf.get_migrate_duplicate(platform) and as_conf.get_migrate_user_to(
                        platform) == as_conf.get_current_user(platform):
                    err_message += "\nInvalid USER_TO target [ USER == USER_TO in ({0}) ] while parameter SAME_USER is false (or unset)".format(
                        platform)
                    error = True
                p = submitter.platforms[platform]
                if p.temp_dir is None:
                    err_message += "\nInvalid TEMP_DIR, Parameter must be present even if empty in [{0}]".format(
                        platform)
                    error = True
                elif p.temp_dir != "":
                    if not p.check_tmp_exists():
                        err_message += "\nTEMP_DIR {0}, does not exists in [{1}]".format(
                            p.temp_dir, platform)
                        error = True
                if error:
                    raise AutosubmitCritical(err_message, 7014)
            for platform in platforms:
                if as_conf.get_migrate_project_to(platform) != '':
                    Log.info("Project in platform configuration file successfully updated to {0}",
                             as_conf.get_current_project(platform))
                    as_conf.get_current_project(platform)
                    backup_conf.append([platform, as_conf.get_current_user(
                        platform), as_conf.get_current_project(platform)])
                    as_conf.set_new_user(
                        platform, as_conf.get_migrate_user_to(platform))

                    as_conf.set_new_project(
                        platform, as_conf.get_migrate_project_to(platform))
                    as_conf.get_current_project(platform)
                    as_conf.get_current_user(platform)
                else:
                    Log.result(
                        "[OPTIONAL] PROJECT_TO directive not found. The directive PROJECT will remain unchanged")
                    backup_conf.append(
                        [platform, as_conf.get_current_user(platform), None])
                    as_conf.set_new_user(
                        platform, as_conf.get_migrate_user_to(platform))
                    as_conf.get_current_project(platform)
                    as_conf.get_current_user(platform)

                if as_conf.get_migrate_host_to(platform) != "none":
                    Log.result(
                        "Host in platform configuration file successfully updated to {0}",
                        as_conf.get_migrate_host_to(platform))
                    as_conf.set_new_host(
                        platform, as_conf.get_migrate_host_to(platform))
                else:
                    Log.result(
                        "[OPTIONAL] HOST_TO directive not found. The directive HOST will remain unchanged")
                p = submitter.platforms[platform]
                if p.root_dir not in already_moved:
                    if p.root_dir != p.temp_dir and len(p.temp_dir) > 0:
                        already_moved.add(p.root_dir)
                        # find /home/bsc32/bsc32070/dummy3 -type l -lname '/*' -printf ' ln -sf "$(realpath -s --relative-to="%p" $(readlink "%p")")" \n' > script.sh
                        # command = "find " + p.root_dir + " -type l -lname \'/*\' -printf 'var=\"$(realpath -s --relative-to=\"%p\" \"$(readlink \"%p\")\")\" && var=${var:3} && ln -sf $var \"%p\"  \\n'"
                        Log.info(
                            "Converting the absolute symlinks into relatives on platform {0} ", platform)
                        link_finder = "find " + p.root_dir + \
                                  " -type l -lname \'/*\' -printf 'var=\"$(realpath -s --relative-to=\"%p\" \"$(readlink \"%p\")\")\" && var=${var:3} && ln -sf $var \"%p\"  \\n' "
                        try:
                            p.send_command(link_finder, True)
                            retrials = 2
                            while p.get_ssh_output().startswith("var=") and retrials > 0:
                                convertLinkPath = os.path.join(
                                    BasicConfig.LOCAL_ROOT_DIR, experiment_id, BasicConfig.LOCAL_TMP_DIR,
                                    'convertLink.sh')
                                with open(convertLinkPath, 'w') as convertLinkFile:
                                    convertLinkFile.write(p.get_ssh_output())
                                try:
                                    Log.debug("Link\n{0}", p.get_ssh_output())
                                except:
                                    pass
                                p.send_file("convertLink.sh")
                                convertLinkPathRemote = os.path.join(
                                    p.remote_log_dir, "convertLink.sh")
                                command = "chmod +x " + convertLinkPathRemote + " && " + \
                                          convertLinkPathRemote + " && rm " + convertLinkPathRemote
                                p.send_command(command, True)
                                p.send_command(link_finder, True)
                                retrials = retrials - 1
                            else:
                                Log.result("No links found in {0} for [{1}] ".format(
                                    p.root_dir, platform))

                        except IOError:
                            Log.debug(
                                "The platform {0} does not contain absolute symlinks", platform)
                        except BaseException:
                            Log.printlog(
                                "Absolute symlinks failed to convert, check user in platform.conf", 3000)
                            error = True
                            break
                        try:
                            Log.info(
                                "Moving remote files/dirs on {0}", platform)
                            p.send_command("chmod 777 -R " + p.root_dir)
                            if not p.move_file(p.root_dir, os.path.join(p.temp_dir,experiment_id), False, path_root=""):
                                if not as_rsync(p,p.root_dir, p.temp_dir):
                                    Log.printlog("The files/dirs on {0} cannot be moved to {1}.".format(p.root_dir,
                                                                                                        os.path.join(
                                                                                                            p.temp_dir,
                                                                                                            experiment_id),
                                                                                                        6012))
                            Log.result("Data on {0} has been successfully moved".format(p.root_dir))

                        except IOError as e:
                            Log.printlog("The files/dirs on {0} cannot be moved to {1}.".format(p.root_dir,
                                                                                                os.path.join(p.temp_dir,
                                                                                                             experiment_id),
                                                                                                6012))
                            error = True
                            break
                        except Exception as e:
                            Log.printlog("Trace: {2}\nThe files/dirs on {0} cannot be moved to {1}.".format(
                                p.root_dir, os.path.join(p.temp_dir, experiment_id), str(e)), 6012)
                            error = True
                            break
                        backup_files.append(platform)
                Log.result(
                    "Files/dirs on {0} have been successfully offered", platform)
            if error:
                as_conf = AutosubmitConfig(
                    experiment_id, BasicConfig, ConfigParserFactory())
                as_conf.check_conf_files(False)
                for platform in backup_files:
                    p = submitter.platforms[platform]
                    p.move_file(os.path.join(
                        p.temp_dir, experiment_id), p.root_dir, True)
                for platform in backup_conf:
                    as_conf.set_new_user(platform[0], platform[1])
                    if platform[2] is not None:
                        as_conf.set_new_project(platform[0], platform[2])
                    if as_conf.get_migrate_host_to(platform[0]) != "none":
                        as_conf.set_new_host(
                            platform[0], as_conf.get_migrate_host_to(platform[0]))
                raise AutosubmitCritical(
                    "The experiment cannot be offered, changes are reverted", 7014)
            else:
                try:
                    if not only_remote:
                        if not Autosubmit.archive(experiment_id, True, True):
                            for platform in backup_files:
                                p = submitter.platforms[platform]
                                p.move_file(os.path.join(
                                    p.temp_dir, experiment_id), p.root_dir, True)
                            for platform in backup_conf:
                                as_conf.set_new_user(platform[0], platform[1])
                                if platform[2] is not None:
                                    as_conf.set_new_project(
                                        platform[0], platform[2])
                            raise AutosubmitCritical(
                                "The experiment cannot be offered, changes are reverted", 7014)
                    Log.result("The experiment has been successfully offered.")
                except Exception as e:
                    for platform in backup_files:
                        p = submitter.platforms[platform]
                        p.move_file(os.path.join(
                            p.temp_dir, experiment_id), p.root_dir, True)
                    for platform in backup_conf:
                        as_conf.set_new_user(platform[0], platform[1])
                        if platform[2] is not None:
                            as_conf.set_new_project(platform[0], platform[2])
                    raise AutosubmitCritical(
                        "The experiment cannot be offered, changes are reverted", 7014, str(e))
        elif pickup:
            Log.info('Migrating experiment {0}'.format(experiment_id))
            Log.info("Moving local files/dirs")
            if not only_remote:
                if not Autosubmit.unarchive(experiment_id, True):
                    raise AutosubmitCritical(
                        "The experiment cannot be picked up", 7012)
                Log.info("Local files/dirs have been successfully picked up")
            else:
                exp_path = os.path.join(
                    BasicConfig.LOCAL_ROOT_DIR, experiment_id)
                if not os.path.exists(exp_path):
                    raise AutosubmitCritical(
                        "Experiment seems to be archived, no action is performed", 7012)

            as_conf = AutosubmitConfig(
                experiment_id, BasicConfig, ConfigParserFactory())
            as_conf.check_conf_files(False)
            pkl_dir = os.path.join(
                BasicConfig.LOCAL_ROOT_DIR, experiment_id, 'pkl')
            job_list = Autosubmit.load_job_list(
                experiment_id, as_conf, notransitive=True, monitor=True)
            Log.debug("Job list restored from {0} files", pkl_dir)
            error = False
            platforms_to_test = set()
            submitter = Autosubmit._get_submitter(as_conf)
            submitter.load_platforms(as_conf)
            if submitter.platforms is None:
                raise AutosubmitCritical("No platforms configured!!!", 7014)
            platforms = submitter.platforms
            for job in job_list.get_job_list():
                job.submitter = submitter
                if job.platform_name is None:
                    job.platform_name = as_conf.get_platform()
                platforms_to_test.add(platforms[job.platform_name.lower()])

            Log.info("Checking remote platforms")
            platforms = filter(lambda x: x not in [
                'local', 'LOCAL'], submitter.platforms)
            already_moved = set()
            backup_files = []
            # establish the connection to all platforms on use
            try:
                Autosubmit.restore_platforms(platforms_to_test)
            except AutosubmitCritical as e:
                raise AutosubmitCritical(
                    e.message + "\nInvalid Remote Platform configuration, recover them manually or:\n 1) Configure platform.conf with the correct info\n 2) autosubmit expid -p --onlyremote",
                    7014, e.trace)
            except Exception as e:
                raise AutosubmitCritical(
                    "Invalid Remote Platform configuration, recover them manually or:\n 1) Configure platform.conf with the correct info\n 2) autosubmit expid -p --onlyremote",
                    7014, str(e))
            for platform in platforms:
                p = submitter.platforms[platform]
                if p.temp_dir is not None and p.temp_dir not in already_moved:
                    if p.root_dir != p.temp_dir and len(p.temp_dir) > 0:
                        already_moved.add(p.temp_dir)
                        Log.info(
                            "Copying remote files/dirs on {0}", platform)
                        Log.info("Copying from {0} to {1}", os.path.join(
                            p.temp_dir, experiment_id), p.root_dir)
                        try:
                            finished = as_rsync(p, os.path.join(
                                p.temp_dir, experiment_id), p.root_dir[:-5])
                            if finished:
                                p.send_command("chmod 755 -R " + p.root_dir)
                                Log.result(
                                    "Files/dirs on {0} have been successfully picked up", platform)
                                # p.send_command(
                                #    "find {0} -depth -type d -empty -delete".format(os.path.join(p.temp_dir, experiment_id)))
                                Log.result(
                                    "Empty dirs on {0} have been successfully deleted".format(p.temp_dir))
                            else:
                                Log.printlog("The files/dirs on {0} cannot be copied to {1}.".format(
                                    os.path.join(p.temp_dir, experiment_id), p.root_dir), 6012)
                                error = True
                        except IOError as e:
                            raise AutosubmitError(
                                "I/O Issues", 6016, e.message)
                        except BaseException as e:
                            error = True
                            Log.printlog("The files/dirs on {0} cannot be copied to {1}.\nTRACE:{2}".format(
                                os.path.join(p.temp_dir, experiment_id), p.root_dir, e.message), 6012)
                            break
                    else:
                        Log.result(
                            "Files/dirs on {0} have been successfully picked up", platform)
            if error:
                raise AutosubmitCritical(
                    "Unable to pickup all platforms, the non-moved files are on the TEMP_DIR\n You can try again with autosubmit {0} -p --onlyremote".format(
                        experiment_id), 7012)
            else:
                Log.result("The experiment has been successfully picked up.")
                return True

    @staticmethod
    def check(experiment_id, notransitive=False):
        """
        Checks experiment configuration and warns about any detected error or inconsistency.

        :param experiment_id: experiment identifier:
        :type experiment_id: str
        """
        try:
            exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, experiment_id)

            as_conf = AutosubmitConfig(
                experiment_id, BasicConfig, ConfigParserFactory())
            as_conf.check_conf_files(False)

            project_type = as_conf.get_project_type()

            submitter = Autosubmit._get_submitter(as_conf)
            submitter.load_platforms(as_conf)
            if len(submitter.platforms) == 0:
                return False

            pkl_dir = os.path.join(
                BasicConfig.LOCAL_ROOT_DIR, experiment_id, 'pkl')
            job_list = Autosubmit.load_job_list(
                experiment_id, as_conf, notransitive=notransitive)
            Log.debug("Job list restored from {0} files", pkl_dir)

            Autosubmit._load_parameters(as_conf, job_list, submitter.platforms)

            hpc_architecture = as_conf.get_platform()
            for job in job_list.get_job_list():
                if job.platform_name is None:
                    job.platform_name = hpc_architecture
                job.platform = submitter.platforms[job.platform_name.lower()]
                job.update_parameters(as_conf, job_list.parameters)
        except AutosubmitError:
            raise
        except BaseException as e:
            raise AutosubmitCritical("Checking incomplete due an unknown error. Please check the trace", 7070,
                                     e.message)

        return job_list.check_scripts(as_conf)

    @staticmethod
    def capitalize_keys(dictionary):
        upper_dictionary = defaultdict()
        for key in dictionary.keys():
            upper_key = key.upper()
            upper_dictionary[upper_key] = dictionary[key]
        return upper_dictionary

    @staticmethod
    def report(expid, template_file_path="", show_all_parameters=False, folder_path="", placeholders=False):
        """
        Show report for specified experiment
        :param expid: experiment identifier:
        :type str
        :param template_file_path: template filepath
        :type str
        :param show_all_parameters: Write all parameters of the experiment
        :type bool
        :param folder_path: Allows to put the report files on another folder
        :type str
        """
        try:
            exp_parameters = defaultdict()
            performance_metrics = None
            ignore_performance_keys = ["error_message",
                                       "warnings_job_data", "considered"]
            exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid)
            tmp_path = os.path.join(exp_path, BasicConfig.LOCAL_TMP_DIR)
            if folder_path is not None:
                tmp_path = folder_path
            import platform
            host = platform.node()
            # Gather experiment info
            as_conf = AutosubmitConfig(expid, BasicConfig, ConfigParserFactory())
            try:
                as_conf.check_conf_files(False)
            except Exception as e:
                raise AutosubmitCritical(
                    "Unable to gather the parameters from config files, check permissions.", 7012)
            # Performace Metrics call
            try:
                BasicConfig.read()
                request = requests.get(
                    "{0}/performance/{1}".format(BasicConfig.AUTOSUBMIT_API_URL, expid))
                performance_metrics = json.loads(request.text)
                # If error, then None
                performance_metrics = None if performance_metrics and performance_metrics[
                    "error"] == True else performance_metrics
                if performance_metrics:
                    for key in ignore_performance_keys:
                        performance_metrics.pop(key, None)
            except Exception as e:
                Log.printlog("Autosubmit couldn't retrieve performance metrics.")
                performance_metrics = None
            # Preparation for section parameters
            no_load_sections = False
            no_load_platforms = False

            job_list = Autosubmit.load_job_list(
                expid, as_conf, notransitive=False)

            try:
                submitter = Autosubmit._get_submitter(as_conf)
                submitter.load_platforms(as_conf)
            except Exception as e:
                no_load_platforms = True
                submitter = Autosubmit._get_submitter(as_conf)
                submitter.load_local_platform(as_conf)
            try:
                # Gathering parameters of autosubmit and expdef config files
                exp_parameters.update(as_conf.load_parameters())
                # Gathering parameters of project config file
                exp_parameters.update(as_conf.load_project_parameters())
                # Gathering common parameters of jobs and platform config file
                if not no_load_platforms:
                    Autosubmit._load_parameters(
                        as_conf, job_list, submitter.platforms)
                    exp_parameters.update(job_list.parameters)
                else:
                    Log.printlog(
                        "Incorrect platform configuration/insufficient permissions \nUnable to load common job_list variables \nJob section specific parameters will be tried to load regarless of this issue",
                        6013)
                # Gathering parameters of jobs divided by SECTION_PARAMETER
                if not no_load_sections:
                    exp_parameters.update(as_conf.load_section_parameters(
                        job_list, as_conf, submitter))
                else:
                    Log.printlog(
                        "Unable to load section jobs parameters, the report will have uncompleted parameters", 6014)

                # Gathering parameters of jobs divided by PLATFORM
                exp_parameters.update(as_conf.load_platform_parameters())
                # All parameters to upper_case to be easier to identify
                exp_parameters = Autosubmit.capitalize_keys(exp_parameters)
            except Exception as e:
                raise AutosubmitCritical(
                    "Couldn't gather the experiment parameters", 7012, str(e))

            if show_all_parameters:
                Log.info("Gathering all parameters (all keys are on upper_case)")
                parameter_output = '{0}_parameter_list_{1}.txt'.format(expid,
                                                                       datetime.datetime.today().strftime(
                                                                           '%Y%m%d-%H%M%S'))
                parameter_file = open(os.path.join(
                    tmp_path, parameter_output), 'w').close()
                parameter_file = open(os.path.join(
                    tmp_path, parameter_output), 'a')
                for key, value in exp_parameters.items():
                    if value is not None:
                        parameter_file.write(key + "=" + str(value) + "\n")
                    else:
                        if placeholders:
                            parameter_file.write(
                                key + "=" + "%" + key + "%" + "\n")
                        else:
                            parameter_file.write(key + "=" + "-" + "\n")

                if performance_metrics is not None:
                    for key in performance_metrics:
                        parameter_file.write("{0} = {1}\n".format(
                            key, performance_metrics.get(key, "-")))
                    parameter_file.close()

                os.chmod(os.path.join(tmp_path, parameter_output), 0o755)
                Log.result("A list of all parameters has been written on {0}".format(
                    os.path.join(tmp_path, parameter_output)))

            if template_file_path is not None:
                if os.path.exists(template_file_path):
                    Log.info(
                        "Gathering the selected parameters (all keys are on upper_case)")
                    template_file = open(template_file_path, 'r')
                    template_content = template_file.read()
                    for key, value in exp_parameters.items():
                        template_content = re.sub(
                            '%(?<!%%)' + key + '%(?!%%)', str(exp_parameters[key]), template_content)
                    # Performance metrics
                    if performance_metrics is not None:
                        for key in performance_metrics:
                            template_content = re.sub(
                                '%(?<!%%)' + key + '%(?!%%)', str(performance_metrics[key]), template_content)
                    template_content = template_content.replace("%%", "%")
                    if not placeholders:
                        template_content = re.sub(
                            r"\%[^% \n\t]+\%", "-", template_content)
                    report = '{0}_report_{1}.txt'.format(
                        expid, datetime.datetime.today().strftime('%Y%m%d-%H%M%S'))
                    open(os.path.join(tmp_path, report),
                         'w').write(template_content)
                    os.chmod(os.path.join(tmp_path, report), 0o755)
                    template_file.close()
                    Log.result("Report {0} has been created on {1}".format(
                        report, os.path.join(tmp_path, report)))
                else:
                    raise AutosubmitCritical(
                        "Template {0} doesn't exists ".format(template_file_path), 7014)
        except AutosubmitError as e:
            raise
        except AutosubmitCritical as e:
            raise
        except BaseException as e:
            raise AutosubmitCritical("Unknown error while reporting the parameters list, likely it is due IO issues",
                                     7040, e.message)

    @staticmethod
    def describe(experiment_id):
        """
        Show details for specified experiment

        :param experiment_id: experiment identifier:
        :type experiment_id: str
        """
        try:
            Log.info("Describing {0}", experiment_id)
            exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, experiment_id)

            as_conf = AutosubmitConfig(
                experiment_id, BasicConfig, ConfigParserFactory())
            as_conf.check_conf_files(False)

            user = os.stat(as_conf.experiment_file).st_uid
            try:
                user = pwd.getpwuid(user).pw_name
            except:
                Log.warning(
                    "The user does not exist anymore in the system, using id instead")

            created = datetime.datetime.fromtimestamp(
                os.path.getmtime(as_conf.experiment_file))

            project_type = as_conf.get_project_type()
            if (as_conf.get_svn_project_url()):
                model = as_conf.get_svn_project_url()
                branch = as_conf.get_svn_project_url()
            else:
                model = as_conf.get_git_project_origin()
                branch = as_conf.get_git_project_branch()
            if model is "":
                model = "Not Found"
            if branch is "":
                branch = "Not Found"

            submitter = Autosubmit._get_submitter(as_conf)
            submitter.load_platforms(as_conf)
            if len(submitter.platforms) == 0:
                return False
            hpc = as_conf.get_platform()

            Log.result("Owner: {0}", user)
            Log.result("Created: {0}", created)
            Log.result("Model: {0}", model)
            Log.result("Branch: {0}", branch)
            Log.result("HPC: {0}", hpc)
        except BaseException as e:
            raise AutosubmitCritical(
                "Couldn't get the details of this experiment. Contact with Autosubmit Developers through GitHub", 7001,
                e.message)
        return user, created, model, branch, hpc

    @staticmethod
    def configure(advanced, database_path, database_filename, local_root_path, platforms_conf_path, jobs_conf_path,
                  smtp_hostname, mail_from, machine, local):
        """
        Configure several paths for autosubmit: database, local root and others. Can be configured at system,
        user or local levels. Local level configuration precedes user level and user level precedes system
        configuration.

        :param database_path: path to autosubmit database
        :type database_path: str
        :param database_filename: database filename
        :type database_filename: str
        :param local_root_path: path to autosubmit's experiments' directory
        :type local_root_path: str
        :param platforms_conf_path: path to platforms conf file to be used as model for new experiments
        :type platforms_conf_path: str
        :param jobs_conf_path: path to jobs conf file to be used as model for new experiments
        :type jobs_conf_path: str
        :param machine: True if this configuration has to be stored for all the machine users
        :type machine: bool
        :param local: True if this configuration has to be stored in the local path
        :type local: bool
        :param mail_from:
        :type mail_from: str
        :param smtp_hostname:
        :type smtp_hostname: str
        """
        try:
            home_path = os.path.expanduser('~')
            autosubmitapi_url = "http://192.168.11.91:8081" + " # Replace me?"
            # Setting default values
            if not advanced and database_path is None and local_root_path is None:
                database_path = os.path.join(home_path, "autosubmit")
                local_root_path = os.path.join(home_path, "autosubmit")
                global_logs_path = os.path.join(home_path, "autosubmit", "logs")
                structures_path = os.path.join(home_path, "autosubmit", "metadata", "structures")
                historicdb_path = os.path.join(home_path, "autosubmit", "metadata", "data")
                historiclog_path = os.path.join(home_path, "autosubmit", "metadata", "logs")
                database_filename = "autosubmit.db"

            while database_path is None:
                database_path = raw_input("Introduce Database path: ")
                if database_path.find("~/") < 0:
                    database_path = None
                    Log.error("Not a valid path. You must include '~/' at the beginning.")
            database_path = database_path.replace('~', home_path)
            # if not os.path.exists(database_path):
            HUtils.create_path_if_not_exists(database_path)
            # Log.error("Database path does not exist.")
            # return False
            while database_filename is None:
                database_filename = raw_input("Introduce Database name: ")

            while local_root_path is None:
                local_root_path = raw_input("Introduce path to experiments: ")
                if local_root_path.find("~/") < 0:
                    local_root_path = None
                    Log.error("Not a valid path. You must include '~/' at the beginning.")
            local_root_path = local_root_path.replace('~', home_path)

            # if not os.path.exists(local_root_path):
            HUtils.create_path_if_not_exists(local_root_path)
            # Log.error("Local Root path does not exist.")
            # return False
            # else:
            global_logs_path = os.path.join(local_root_path, "logs")
            structures_path = os.path.join(local_root_path, "metadata", "structures")
            historicdb_path = os.path.join(local_root_path, "metadata", "data")
            historiclog_path = os.path.join(local_root_path, "metadata", "logs")

            if platforms_conf_path is not None:
                platforms_conf_path = platforms_conf_path.replace('~', home_path)
                if not os.path.exists(platforms_conf_path):
                    Log.error("platforms.conf path does not exist.")
                    return False
            if jobs_conf_path is not None:
                jobs_conf_path = jobs_conf_path.replace('~', home_path)
                if not os.path.exists(jobs_conf_path):
                    Log.error("jobs.conf path does not exist.")
                    return False

            if machine:
                rc_path = '/etc'
            elif local:
                rc_path = '.'
            else:
                rc_path = home_path
            rc_path = os.path.join(rc_path, '.autosubmitrc')

            config_file = open(rc_path, 'w')
            Log.info("Writing configuration file...")
            try:
                parser = SafeConfigParser()
                parser.add_section('database')
                parser.set('database', 'path', database_path)
                if database_filename is not None:
                    parser.set('database', 'filename', database_filename)
                parser.add_section('local')
                parser.set('local', 'path', local_root_path)
                if jobs_conf_path is not None or platforms_conf_path is not None:
                    parser.add_section('conf')
                    if jobs_conf_path is not None:
                        parser.set('conf', 'jobs', jobs_conf_path)
                    if platforms_conf_path is not None:
                        parser.set('conf', 'platforms', platforms_conf_path)
                if smtp_hostname is not None or mail_from is not None:
                    parser.add_section('mail')
                    parser.set('mail', 'smtp_server', smtp_hostname)
                    parser.set('mail', 'mail_from', mail_from)
                parser.add_section("globallogs")
                parser.set("globallogs", "path", global_logs_path)
                parser.add_section("structures")
                parser.set("structures", "path", structures_path)
                parser.add_section("historicdb")
                parser.set("historicdb", "path", historicdb_path)
                parser.add_section("historiclog")
                parser.set("historiclog", "path", historiclog_path)
                parser.add_section("autosubmitapi")
                parser.set("autosubmitapi", "url", autosubmitapi_url)
                # parser.add_section("hosts")
                # parser.set("hosts", "whitelist", " localhost # Add your machine names")
                parser.write(config_file)
                config_file.close()
                Log.result("Configuration file written successfully: \n\t{0}".format(rc_path))
                HUtils.create_path_if_not_exists(local_root_path)
                HUtils.create_path_if_not_exists(global_logs_path)
                HUtils.create_path_if_not_exists(structures_path)
                HUtils.create_path_if_not_exists(historicdb_path)
                HUtils.create_path_if_not_exists(historiclog_path)
                Log.result(
                    "Directories configured successfully: \n\t{5} \n\t{0} \n\t{1} \n\t{2} \n\t{3} \n\t{4}".format(
                        local_root_path,
                        global_logs_path,
                        structures_path,
                        historicdb_path,
                        historiclog_path,
                        database_path
                    ))
            except (IOError, OSError) as e:
                raise AutosubmitCritical(
                    "Can not write config file: {0}", 7012, e.message)
        except (AutosubmitCritical, AutosubmitError) as e:
            raise
        except BaseException as e:
            raise AutosubmitCritical(e.message, 7014)
        return True

    @staticmethod
    def configure_dialog():
        """
        Configure several paths for autosubmit interactively: database, local root and others.
        Can be configured at system, user or local levels. Local level configuration precedes user level and user level
        precedes system configuration.
        """

        not_enough_screen_size_msg = 'The size of your terminal is not enough to draw the configuration wizard,\n' \
                                     'so we\'ve closed it to prevent errors. Resize it and then try it again.'

        home_path = os.path.expanduser('~')

        try:
            d = dialog.Dialog(
                dialog="dialog", autowidgetsize=True, screen_color='GREEN')
        except dialog.DialogError:
            raise AutosubmitCritical(
                "Graphical visualization failed, not enough screen size", 7060)
        except Exception:
            raise AutosubmitCritical(
                "Dialog libs aren't found in your Operational system", 7060)

        d.set_background_title("Autosubmit configure utility")
        if os.geteuid() == 0:
            text = ''
            choice = [
                ("All", "All users on this machine (may require root privileges)")]
        else:
            text = "If you want to configure Autosubmit for all users, you will need to provide root privileges"
            choice = []

        choice.append(("User", "Current user"))
        choice.append(
            ("Local", "Only when launching Autosubmit from this path"))

        try:
            code, level = d.menu(text, choices=choice, width=60,
                                 title="Choose when to apply the configuration")
            if code != dialog.Dialog.OK:
                os.system('clear')
                return False
        except dialog.DialogError:
            raise AutosubmitCritical(
                "Graphical visualization failed, not enough screen size", 7060)

        filename = '.autosubmitrc'
        if level == 'All':
            path = '/etc'
            filename = 'autosubmitrc'
        elif level == 'User':
            path = home_path
        else:
            path = '.'
        path = os.path.join(path, filename)

        # Setting default values
        database_path = home_path
        local_root_path = home_path
        database_filename = 'autosubmit.db'
        jobs_conf_path = ''
        platforms_conf_path = ''

        d.infobox("Reading configuration file...", width=50, height=5)
        try:
            if os.path.isfile(path):
                parser = SafeConfigParser()
                parser.optionxform = str
                parser.read(path)

                if parser.has_option('database', 'path'):
                    database_path = parser.get('database', 'path')
                if parser.has_option('database', 'filename'):
                    database_filename = parser.get('database', 'filename')
                if parser.has_option('local', 'path'):
                    local_root_path = parser.get('local', 'path')
                if parser.has_option('conf', 'platforms'):
                    platforms_conf_path = parser.get('conf', 'platforms')
                if parser.has_option('conf', 'jobs'):
                    jobs_conf_path = parser.get('conf', 'jobs')

        except (IOError, OSError) as e:
            raise AutosubmitCritical(
                "Can not read config file", 7014, e.message)

        while True:
            try:
                code, database_path = d.dselect(database_path, width=80, height=20,
                                                title='\Zb\Z1Select path to database\Zn', colors='enable')
            except dialog.DialogError:
                raise AutosubmitCritical(
                    "Graphical visualization failed, not enough screen size", 7060)
            if Autosubmit._requested_exit(code, d):
                raise AutosubmitCritical(
                    "Graphical visualization failed, requested exit", 7060)
            elif code == dialog.Dialog.OK:
                database_path = database_path.replace('~', home_path)
                if not os.path.exists(database_path):
                    d.msgbox(
                        "Database path does not exist.\nPlease, insert the right path", width=50, height=6)
                else:
                    break

        while True:
            try:
                code, local_root_path = d.dselect(local_root_path, width=80, height=20,
                                                  title='\Zb\Z1Select path to experiments repository\Zn',
                                                  colors='enable')
            except dialog.DialogError:
                raise AutosubmitCritical(
                    "Graphical visualization failed, not enough screen size", 7060)

            if Autosubmit._requested_exit(code, d):
                raise AutosubmitCritical(
                    "Graphical visualization failed,requested exit", 7060)
            elif code == dialog.Dialog.OK:
                database_path = database_path.replace('~', home_path)
                if not os.path.exists(database_path):
                    d.msgbox(
                        "Local root path does not exist.\nPlease, insert the right path", width=50, height=6)
                else:
                    break
        while True:
            try:
                (code, tag) = d.form(text="",
                                     elements=[("Database filename", 1, 1, database_filename, 1, 40, 20, 20),
                                               (
                                                   "Default platform.conf path", 2, 1, platforms_conf_path, 2, 40, 40,
                                                   200),
                                               ("Default jobs.conf path", 3, 1, jobs_conf_path, 3, 40, 40, 200)],
                                     height=20,
                                     width=80,
                                     form_height=10,
                                     title='\Zb\Z1Just a few more options:\Zn', colors='enable')
            except dialog.DialogError:
                raise AutosubmitCritical(
                    "Graphical visualization failed, not enough screen size", 7060)

            if Autosubmit._requested_exit(code, d):
                raise AutosubmitCritical(
                    "Graphical visualization failed, _requested_exit", 7060)
            elif code == dialog.Dialog.OK:
                database_filename = tag[0]
                platforms_conf_path = tag[1]
                jobs_conf_path = tag[2]

                platforms_conf_path = platforms_conf_path.replace(
                    '~', home_path).strip()
                jobs_conf_path = jobs_conf_path.replace('~', home_path).strip()

                if platforms_conf_path and not os.path.exists(platforms_conf_path):
                    d.msgbox(
                        "Platforms conf path does not exist.\nPlease, insert the right path", width=50, height=6)
                elif jobs_conf_path and not os.path.exists(jobs_conf_path):
                    d.msgbox(
                        "Jobs conf path does not exist.\nPlease, insert the right path", width=50, height=6)
                else:
                    break

        smtp_hostname = "mail.bsc.es"
        mail_from = "automail@bsc.es"
        while True:
            try:
                (code, tag) = d.form(text="",
                                     elements=[("STMP server hostname", 1, 1, smtp_hostname, 1, 40, 20, 20),
                                               ("Notifications sender address", 2, 1, mail_from, 2, 40, 40, 200)],
                                     height=20,
                                     width=80,
                                     form_height=10,
                                     title='\Zb\Z1Mail notifications configuration:\Zn', colors='enable')
            except dialog.DialogError:
                raise AutosubmitCritical(
                    "Graphical visualization failed, not enough screen size", 7060)

            if Autosubmit._requested_exit(code, d):
                raise AutosubmitCritical(
                    "Graphical visualization failed, requested exit", 7060)
            elif code == dialog.Dialog.OK:
                smtp_hostname = tag[0]
                mail_from = tag[1]
                break
                # TODO: Check that is a valid config?

        config_file = open(path, 'w')
        d.infobox("Writing configuration file...", width=50, height=5)
        try:
            parser = SafeConfigParser()
            parser.add_section('database')
            parser.set('database', 'path', database_path)
            if database_filename:
                parser.set('database', 'filename', database_filename)
                parser.add_section('local')
            parser.set('local', 'path', local_root_path)
            if jobs_conf_path or platforms_conf_path:
                parser.add_section('conf')
                if jobs_conf_path:
                    parser.set('conf', 'jobs', jobs_conf_path)
                if platforms_conf_path:
                    parser.set('conf', 'platforms', platforms_conf_path)
            parser.add_section('mail')
            parser.set('mail', 'smtp_server', smtp_hostname)
            parser.set('mail', 'mail_from', mail_from)
            parser.write(config_file)
            config_file.close()
            d.msgbox("Configuration file written successfully",
                     width=50, height=5)
            os.system('clear')


        except (IOError, OSError) as e:
            raise AutosubmitCritical(
                "Can not write config file", 7012, e.message)
        return True

    @staticmethod
    def _requested_exit(code, d):
        if code != dialog.Dialog.OK:
            code = d.yesno(
                'Exit configure utility without saving?', width=50, height=5)
            if code == dialog.Dialog.OK:
                os.system('clear')
                return True
        return False

    @staticmethod
    def install():
        """
        Creates a new database instance for autosubmit at the configured path

        """
        if not os.path.exists(BasicConfig.DB_PATH):
            Log.info("Creating autosubmit database...")
            qry = resource_string('autosubmit.database', 'data/autosubmit.sql')
            if not create_db(qry):
                raise AutosubmitCritical("Can not write database file", 7004)
            Log.result("Autosubmit database created successfully")
        else:
            raise AutosubmitCritical("Database already exists.", 7004)
        return True

    @staticmethod
    def refresh(expid, model_conf, jobs_conf):
        """
        Refresh project folder for given experiment

        :param model_conf:
        :type model_conf: bool
        :param jobs_conf:
        :type jobs_conf: bool
        :param expid: experiment identifier
        :type expid: str
        """
        try:
            Autosubmit._check_ownership(expid, raise_error=True)
            as_conf = AutosubmitConfig(expid, BasicConfig, ConfigParserFactory())
            as_conf.reload()
            as_conf.check_conf_files()
        except (AutosubmitError, AutosubmitCritical):
            raise
        except BaseException as e:
            raise AutosubmitCritical("Error while reading the configuration files", 7064, e.message)
        try:
            if "Expdef" in as_conf.wrong_config:
                as_conf.show_messages()
            project_type = as_conf.get_project_type()
            if Autosubmit._copy_code(as_conf, expid, project_type, True):
                Log.result("Project folder updated")
            Autosubmit._create_project_associated_conf(
                as_conf, model_conf, jobs_conf)
        except (AutosubmitError, AutosubmitCritical):
            raise
        except BaseException as e:
            raise AutosubmitCritical("Download failed", 7064, e.message)
        return True

    @staticmethod
    def update_version(expid):
        """
        Refresh experiment version with the current autosubmit version
        :param expid: experiment identifier
        :type expid: str
        """
        Autosubmit._check_ownership(expid, raise_error=True)

        as_conf = AutosubmitConfig(expid, BasicConfig, ConfigParserFactory())
        as_conf.reload()
        as_conf.check_expdef_conf()

        Log.info("Changing {0} experiment version from {1} to  {2}",
                 expid, as_conf.get_version(), Autosubmit.autosubmit_version)
        as_conf.set_version(Autosubmit.autosubmit_version)
        return True

    @staticmethod
    def update_description(expid, new_description):
        Log.info("Checking if experiment exists...")
        check_experiment_exists(expid)
        Log.info("Experiment found.")
        Log.info("Setting {0} description to '{1}'".format(
            expid, new_description))
        result = update_experiment_descrip_version(
            expid, description=new_description)
        if result:
            Log.info("Update completed successfully.")
        else:
            Log.critical("Update failed.")
        return True

    @staticmethod
    def pkl_fix(expid):
        """
        Tries to find a backup of the pkl file and restores it. Verifies that autosubmit is not running on this experiment.  

        :param expid: experiment identifier
        :type expid: str
        :return:
        :rtype: 
        """
        exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid)
        tmp_path = os.path.join(exp_path, BasicConfig.LOCAL_TMP_DIR)
        pkl_folder_path = os.path.join(exp_path, "pkl")
        current_pkl_path = os.path.join(
            pkl_folder_path, "job_list_{}.pkl".format(expid))
        backup_pkl_path = os.path.join(
            pkl_folder_path, "job_list_{}_backup.pkl".format(expid))
        # remove any file that has 0 bytes inside the pkl_folder_path
        try:
            Autosubmit._check_ownership(expid, raise_error=True)
        except:
            Log.debug("User is not the owner of the experiment")
            return
        try:
            with portalocker.Lock(os.path.join(tmp_path, 'autosubmit.lock'), timeout=1):
                # Not locked
                stop = False
                Log.info("Looking for backup file {}".format(backup_pkl_path))
                if os.path.exists(backup_pkl_path):
                    # Backup file exists
                    Log.info("Backup file found.")
                    # Make sure backup file is not empty
                    _stat_b = os.stat(backup_pkl_path)
                    if _stat_b.st_size <= 6:
                        os.remove(backup_pkl_path)
                        # It is empty -> Return
                        Log.info(
                            "The backup file {} is empty. Pkl restore operation stopped. No changes have been made.".format(
                                backup_pkl_path))
                        stop = True

                    if os.path.exists(current_pkl_path):
                        # Pkl file exists
                        Log.info("Current pkl file {} found.".format(
                            current_pkl_path))
                        _stat = os.stat(current_pkl_path)
                        if _stat.st_size > 6:
                            # Greater than 6 bytes -> Not empty
                            if not stop and not Autosubmit._user_yes_no_query(
                                    "The current pkl file {0} is not empty. Do you want to continue?".format(
                                        current_pkl_path)):
                                # The user chooses not to continue. Operation stopped.
                                Log.info(
                                    "Pkl restore operation stopped. No changes have been made.")
                                stop = True
                        if stop:
                            return
                        result = None
                        if _stat.st_size > 6:
                            # File not empty: Archive
                            archive_pkl_name = os.path.join(pkl_folder_path, "{0}_job_list_{1}.pkl".format(
                                datetime.datetime.today().strftime("%d%m%Y%H%M%S"), expid))
                            # Waiting for completion
                            subprocess.call(
                                ["cp", current_pkl_path, archive_pkl_name])

                            if os.path.exists(archive_pkl_name):
                                Log.result("File {0} archived as {1}.".format(
                                    current_pkl_path, archive_pkl_name))
                        else:
                            # File empty: Delete
                            try:
                                os.remove(current_pkl_path)
                                Log.info("File {0} deleted.".format(
                                    current_pkl_path))
                            except:
                                pass
                    # Restore backup file
                    Log.info("Restoring {0} into {1}".format(
                        backup_pkl_path, current_pkl_path))
                    subprocess.call(["mv", backup_pkl_path, current_pkl_path])

                    if os.path.exists(current_pkl_path):
                        Log.result("Pkl restored.")
                else:
                    Log.info(
                        "Backup file not found. Pkl restore operation stopped. No changes have been made.")
        except portalocker.AlreadyLocked:
            message = "Another Autosubmit instance using the experiment\n. Stop other Autosubmit instances that are using the experiment or delete autosubmit.lock file located on the /tmp folder."
            raise AutosubmitCritical(message, 7000)
        except AutosubmitCritical as e:
            raise AutosubmitCritical(e.message, e.code, e.trace)
        except BaseException as e:
            raise

    @staticmethod
    def database_backup(expid):
        try:
            database_path = os.path.join(BasicConfig.JOBDATA_DIR, "job_data_{0}.db".format(expid))
            backup_path = os.path.join(BasicConfig.JOBDATA_DIR, "job_data_{0}.sql".format(expid))
            command = "sqlite3 {0} .dump > {1} ".format(database_path, backup_path)
            Log.debug("Backing up jobs_data...")
            out = subprocess.call(command, shell=True)
            Log.debug("Jobs_data database backup completed.")
        except BaseException as e:
            Log.debug("Jobs_data database backup failed.")

    @staticmethod
    def database_fix(expid):
        """
        Database methods. Performs a sql dump of the database and restores it.

        :param expid: experiment identifier
        :type expid: str
        :return:
        :rtype:        
        """
        os.umask(0)  # Overrides user permissions
        current_time = int(time.time())
        corrupted_db_path = os.path.join(BasicConfig.JOBDATA_DIR, "job_data_{0}_corrupted.db".format(expid))

        database_path = os.path.join(BasicConfig.JOBDATA_DIR, "job_data_{0}.db".format(expid))
        database_backup_path = os.path.join(BasicConfig.JOBDATA_DIR, "job_data_{0}.sql".format(expid))
        dump_file_name = 'job_data_{0}.sql'.format(expid, current_time)
        dump_file_path = os.path.join(BasicConfig.JOBDATA_DIR, dump_file_name)
        bash_command = 'cat {1} | sqlite3 {0}'.format(database_path, dump_file_path)
        try:
            if os.path.exists(database_path):
                result = os.popen("mv {0} {1}".format(database_path, corrupted_db_path)).read()
                time.sleep(1)
                Log.info("Original database moved.")
            try:
                exp_history = ExperimentHistory(expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR,
                                                historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR)
                Log.info("Restoring from sql")
                result = os.popen(bash_command).read()
                exp_history.initialize_database()

            except:
                Log.warning("It was not possible to restore the jobs_data.db file... , a new blank db will be created")
                result = os.popen("rm {0}".format(database_path)).read()

                exp_history = ExperimentHistory(expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR,
                                                historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR)
                exp_history.initialize_database()
        except Exception as exp:
            Log.critical(str(exp))

    @staticmethod
    def archive(expid, noclean=True, uncompress=True):
        """
        Archives an experiment: call clean (if experiment is of version 3 or later), compress folder
        to tar.gz and moves to year's folder

        :param clean,compress:
        :return:
        :param expid: experiment identifier
        :type expid: str
        """

        exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid)

        exp_folder = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid)

        if not noclean:
            # Cleaning to reduce file size.
            version = get_autosubmit_version(expid)
            if version is not None and version.startswith('3') and not Autosubmit.clean(expid, True, True, True):
                raise AutosubmitCritical(
                    "Can not archive project. Clean not successful", 7012)

        # Getting year of last completed. If not, year of expid folder
        year = None
        tmp_folder = os.path.join(exp_folder, BasicConfig.LOCAL_TMP_DIR)
        if os.path.isdir(tmp_folder):
            for filename in os.listdir(tmp_folder):
                if filename.endswith("COMPLETED"):
                    file_year = time.localtime(os.path.getmtime(
                        os.path.join(tmp_folder, filename))).tm_year
                    if year is None or year < file_year:
                        year = file_year

        if year is None:
            year = time.localtime(os.path.getmtime(exp_folder)).tm_year
        Log.info("Archiving in year {0}", year)

        # Creating tar file
        Log.info("Creating tar file ... ")
        try:
            year_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, str(year))
            if not os.path.exists(year_path):
                os.mkdir(year_path)
                os.chmod(year_path, 0o775)
            if not uncompress:
                compress_type = "w:gz"
                output_filepath = '{0}.tar.gz'.format(expid)
            else:
                compress_type = "w"
                output_filepath = '{0}.tar'.format(expid)
            with tarfile.open(os.path.join(year_path, output_filepath), compress_type) as tar:
                tar.add(exp_folder, arcname='')
                tar.close()
                os.chmod(os.path.join(year_path, output_filepath), 0o775)
        except Exception as e:
            raise AutosubmitCritical("Can not write tar file", 7012, str(e))

        Log.info("Tar file created!")

        try:
            shutil.rmtree(exp_folder)
        except Exception as e:
            Log.warning(
                "Can not fully remove experiments folder: {0}".format(str(e)))
            if os.stat(exp_folder):
                try:
                    tmp_folder = os.path.join(
                        BasicConfig.LOCAL_ROOT_DIR, "tmp")
                    tmp_expid = os.path.join(tmp_folder, expid + "_to_delete")
                    os.rename(exp_folder, tmp_expid)
                    Log.warning("Experiment folder renamed to: {0}".format(
                        exp_folder + "_to_delete "))
                except Exception as e:
                    Autosubmit.unarchive(expid, uncompressed=False)
                    raise AutosubmitCritical(
                        "Can not remove or rename experiments folder", 7012, str(e))

        Log.result("Experiment archived successfully")
        return True

    @staticmethod
    def unarchive(experiment_id, uncompressed=True):
        """
        Unarchives an experiment: uncompress folder from tar.gz and moves to experiments root folder

        :param experiment_id: experiment identifier
        :type experiment_id: str
        :type compress: boolean
        :type overwrite: boolean
        """
        exp_folder = os.path.join(BasicConfig.LOCAL_ROOT_DIR, experiment_id)

        # Searching by year. We will store it on database
        year = datetime.datetime.today().year
        archive_path = None
        if not uncompressed:
            compress_type = "r:gz"
            output_pathfile = '{0}.tar.gz'.format(experiment_id)
        else:
            compress_type = "r:"
            output_pathfile = '{0}.tar'.format(experiment_id)
        while year > 2000:
            archive_path = os.path.join(
                BasicConfig.LOCAL_ROOT_DIR, str(year), output_pathfile)
            if os.path.exists(archive_path):
                break
            year -= 1

        if year == 2000:
            Log.error("Experiment {0} is not archived", experiment_id)
            return False
        Log.info("Experiment located in {0} archive", year)

        # Creating tar file
        Log.info("Unpacking tar file ... ")
        if not os.path.isdir(exp_folder):
            os.mkdir(exp_folder)
        try:
            with tarfile.open(os.path.join(archive_path), compress_type) as tar:
                tar.extractall(exp_folder)
                tar.close()
        except Exception as e:
            shutil.rmtree(exp_folder, ignore_errors=True)
            Log.printlog("Can not extract tar file: {0}".format(str(e)), 6012)
            return False

        Log.info("Unpacking finished")

        try:
            os.remove(archive_path)
        except Exception as e:
            Log.printlog(
                "Can not remove archived file folder: {0}".format(e.message), 7012)
            Log.result("Experiment {0} unarchived successfully", experiment_id)
            return True

        Log.result("Experiment {0} unarchived successfully", experiment_id)
        return True

    @staticmethod
    def _create_project_associated_conf(as_conf, force_model_conf, force_jobs_conf):
        project_destiny = as_conf.project_file
        jobs_destiny = as_conf.jobs_file

        if as_conf.get_project_type() != 'none':
            if as_conf.get_file_project_conf():
                copy = True
                if os.path.exists(os.path.join(as_conf.get_project_dir(), as_conf.get_file_project_conf())):
                    if os.path.exists(project_destiny):
                        if force_model_conf:
                            os.rename(project_destiny, project_destiny + "_backup")
                        else:
                            copy = False
                    if copy:
                        shutil.copyfile(os.path.join(as_conf.get_project_dir(), as_conf.get_file_project_conf()),
                                        project_destiny)

            if as_conf.get_file_jobs_conf():
                copy = True
                if os.path.exists(os.path.join(as_conf.get_project_dir(), as_conf.get_file_jobs_conf())):
                    if os.path.exists(jobs_destiny):
                        if force_jobs_conf:
                            os.rename(jobs_destiny, jobs_destiny + "_backup")
                        else:
                            copy = False
                    if copy:
                        shutil.copyfile(os.path.join(as_conf.get_project_dir(), as_conf.get_file_jobs_conf()),
                                        jobs_destiny)

    @staticmethod
    def create(expid, noplot, hide, output='pdf', group_by=None, expand=list(), expand_status=list(),
               notransitive=False, check_wrappers=False, detail=False):
        """
        Creates job list for given experiment. Configuration files must be valid before executing this process.

        :param expid: experiment identifier
        :type expid: str
        :param noplot: if True, method omits final plotting of the jobs list. Only needed on large experiments when
        plotting time can be much larger than creation time.
        :type noplot: bool
        :return: True if successful, False if not
        :rtype: bool
        :param hide: hides plot window
        :type hide: bool
        :param hide: hides plot window
        :type hide: bool
        :param output: plot's file format. It can be pdf, png, ps or svg
        :type output: str

        """

        # checking if there is a lock file to avoid multiple running on the same expid
        try:
            Autosubmit._check_ownership(expid, raise_error=True)
            try:
                Log.info("Removing job_data db...")
                job_data_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, BasicConfig.JOBDATA_DIR,
                                             "job_data_{0}.db".format(expid))
                # Delete job_data if 0 bytes
                if os.path.exists(job_data_path) and os.stat(job_data_path).st_size <= 6:
                    Log.info("job_data db is corrupted. It will be removed.")
                    os.remove(job_data_path)
            except BaseException as e:
                Log.warning("Error removing job_data db: {0}, please remove it manually".format(e.message))
            exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid)
            tmp_path = os.path.join(exp_path, BasicConfig.LOCAL_TMP_DIR)
            # Encapsulating the lock
            with portalocker.Lock(os.path.join(tmp_path, 'autosubmit.lock'), timeout=1) as fh:
                pkl_folder_path = os.path.join(exp_path, "pkl")
                current_pkl_path = os.path.join(
                    pkl_folder_path, "job_list_{}.pkl".format(expid))
                if os.path.exists(current_pkl_path):
                    #stat < 6 bytes -> remove
                    _stat = os.stat(current_pkl_path)
                    if _stat.st_size <= 6:
                        os.remove(current_pkl_path)
                        Log.info("The pkl file {} is empty. It will be removed.".format(current_pkl_path))
                        Autosubmit.pkl_fix(expid)
                try:
                    Log.info(
                        "Preparing .lock file to avoid multiple instances with same expid.")

                    as_conf = AutosubmitConfig(
                        expid, BasicConfig, ConfigParserFactory())
                    as_conf.check_conf_files(False)

                    project_type = as_conf.get_project_type()
                    # Getting output type provided by the user in config, 'pdf' as default
                    output_type = as_conf.get_output_type()

                    if not Autosubmit._copy_code(as_conf, expid, project_type, False):
                        return False
                    if not os.path.exists(os.path.join(exp_path, "pkl")):
                        raise AutosubmitCritical(
                            "The pkl folder doesn't exists. Make sure that the 'pkl' folder exists in the following path: {}".format(
                                exp_path), code=6013)
                    if not os.path.exists(os.path.join(exp_path, "plot")):
                        raise AutosubmitCritical(
                            "The plot folder doesn't exists. Make sure that the 'plot' folder exists in the following path: {}".format(
                                exp_path), code=6013)

                    update_job = not os.path.exists(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl",
                                                                 "job_list_" + expid + ".pkl"))
                    Autosubmit._create_project_associated_conf(
                        as_conf, False, update_job)

                    # Load parameters
                    Log.info("Loading parameters...")
                    parameters = as_conf.load_parameters()

                    date_list = as_conf.get_date_list()
                    if len(date_list) != len(set(date_list)):
                        raise AutosubmitCritical('There are repeated start dates!', 7014)
                    num_chunks = as_conf.get_num_chunks()
                    chunk_ini = as_conf.get_chunk_ini()
                    member_list = as_conf.get_member_list()
                    run_only_members = as_conf.get_member_list(run_only=True)
                    # print("Run only members {0}".format(run_only_members))
                    if len(member_list) != len(set(member_list)):
                        raise AutosubmitCritical(
                            "There are repeated member names!")
                    rerun = as_conf.get_rerun()

                    Log.info("\nCreating the jobs list...")
                    job_list = JobList(expid, BasicConfig, ConfigParserFactory(),
                                       Autosubmit._get_job_list_persistence(expid, as_conf))
                    prev_job_list = Autosubmit.load_job_list(
                        expid, as_conf, notransitive=notransitive)

                    date_format = ''
                    if as_conf.get_chunk_size_unit() is 'hour':
                        date_format = 'H'
                    for date in date_list:
                        if date.hour > 1:
                            date_format = 'H'
                        if date.minute > 1:
                            date_format = 'M'
                    wrapper_jobs = dict()
                    if as_conf.get_wrapper_type() == "multi":
                        for wrapper_section in as_conf.get_wrapper_multi():
                            wrapper_jobs[wrapper_section] = as_conf.get_wrapper_jobs(wrapper_section)
                    wrapper_jobs["wrapper"] = as_conf.get_wrapper_jobs("wrapper")

                    job_list.generate(date_list, member_list, num_chunks, chunk_ini, parameters, date_format,
                                      as_conf.get_retrials(),
                                      as_conf.get_default_job_type(),
                                      as_conf.get_wrapper_type(), wrapper_jobs, notransitive=notransitive,
                                      update_structure=True, run_only_members=run_only_members)

                    if rerun == "true":
                        job_list.rerun(as_conf.get_rerun_jobs())
                    else:
                        job_list.remove_rerun_only_jobs(notransitive)
                    Log.info("\nSaving the jobs list...")
                    job_list.add_logs(prev_job_list.get_logs())
                    job_list.save()
                    JobPackagePersistence(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl"),
                                          "job_packages_" + expid).reset_table()
                    groups_dict = dict()

                    # Setting up job historical database header. Must create a new run.
                    # Historical Database: Setup new run
                    try:
                        exp_history = ExperimentHistory(expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR,
                                                        historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR)
                        exp_history.initialize_database()

                        # exp_history.create_new_experiment_run(as_conf.get_chunk_size_unit(), as_conf.get_chunk_size(), as_conf.get_full_config_as_json(), job_list.get_job_list())
                        exp_history.process_status_changes(job_list.get_job_list(),
                                                           chunk_unit=as_conf.get_chunk_size_unit(),
                                                           chunk_size=as_conf.get_chunk_size(),
                                                           current_config=as_conf.get_full_config_as_json(),
                                                           create=True)
                        Autosubmit.database_backup(expid)
                    except BaseException as e:
                        Log.printlog("Historic database seems corrupted, AS will repair it and resume the run",
                                     Log.INFO)
                        try:
                            Autosubmit.database_fix(expid)
                        except:
                            Log.warning(
                                "Couldn't recover the Historical database, AS will continue without it, GUI may be affected")
                    if not noplot:
                        if group_by:
                            status = list()
                            if expand_status:
                                for s in expand_status.split():
                                    status.append(
                                        Autosubmit._get_status(s.upper()))

                            job_grouping = JobGrouping(group_by, copy.deepcopy(job_list.get_job_list()), job_list,
                                                       expand_list=expand, expanded_status=status)
                            groups_dict = job_grouping.group_jobs()
                        # WRAPPERS

                        if as_conf.get_wrapper_type() != 'none' and check_wrappers:
                            as_conf.check_conf_files(True)
                            packages_persistence = JobPackagePersistence(
                                os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl"), "job_packages_" + expid)
                            packages_persistence.reset_table(True)
                            referenced_jobs_to_remove = set()
                            job_list_wrappers = copy.deepcopy(job_list)
                            jobs_wr = job_list_wrappers.get_job_list()
                            for job in jobs_wr:
                                for child in job.children:
                                    if child not in jobs_wr:
                                        referenced_jobs_to_remove.add(child)
                                for parent in job.parents:
                                    if parent not in jobs_wr:
                                        referenced_jobs_to_remove.add(parent)

                            for job in jobs_wr:
                                job.children = job.children - referenced_jobs_to_remove
                                job.parents = job.parents - referenced_jobs_to_remove
                            Autosubmit.generate_scripts_andor_wrappers(
                                as_conf, job_list_wrappers, jobs_wr, packages_persistence, True)

                            packages = packages_persistence.load(True)
                        else:
                            packages = None

                        Log.info("\nPlotting the jobs list...")
                        monitor_exp = Monitor()
                        # if output is set, use output
                        monitor_exp.generate_output(expid, job_list.get_job_list(),
                                                    os.path.join(
                                                        exp_path, "/tmp/LOG_", expid),
                                                    output if output is not None else output_type,
                                                    packages,
                                                    not hide,
                                                    groups=groups_dict,
                                                    job_list_object=job_list)
                    Log.result("\nJob list created successfully")
                    Log.warning(
                        "Remember to MODIFY the MODEL config files!")
                    fh.flush()
                    os.fsync(fh.fileno())

                    # Detail after lock has been closed.
                    if detail == True:
                        current_length = len(job_list.get_job_list())
                        if current_length > 1000:
                            Log.warning(
                                "-d option: Experiment has too many jobs to be printed in the terminal. Maximum job quantity is 1000, your experiment has " + str(
                                    current_length) + " jobs.")
                        else:
                            Log.info(job_list.print_with_status())
                            Log.status(job_list.print_with_status())
                    return True
                # catching Exception
                except (KeyboardInterrupt) as e:
                    # Setting signal handler to handle subsequent CTRL-C
                    signal.signal(signal.SIGINT, signal_handler_create)
                    fh.flush()
                    os.fsync(fh.fileno())
                    raise AutosubmitCritical("Stopped by user input", 7010)
                except (BaseException) as e:
                    raise
        except portalocker.AlreadyLocked:
            message = "We have detected that there is another Autosubmit instance using the experiment\n. Stop other Autosubmit instances that are using the experiment or delete autosubmit.lock file located on tmp folder"
            raise AutosubmitCritical(message, 7000)
        except AutosubmitError as e:
            if not e.trace:
                e.trace = traceback.format_exc()
            raise AutosubmitError(e.message, e.code, e.trace)
        except AutosubmitCritical as e:
            if not e.trace:
                e.trace = traceback.format_exc()
            raise AutosubmitCritical(e.message, e.code, e.trace)
        except BaseException as e:
            raise AutosubmitCritical(e.message, 7000)

    @staticmethod
    def _copy_code(as_conf, expid, project_type, force):
        """
        Method to copy code from experiment repository to project directory.

        :param as_conf: experiment configuration class
        :type as_conf: AutosubmitConfig
        :param expid: experiment identifier
        :type expid: str
        :param project_type: project type (git, svn, local)
        :type project_type: str
        :param force: if True, overwrites current data
        :return: True if succesful, False if not
        :rtype: bool
        """
        project_destination = as_conf.get_project_destination()
        if project_destination is None or len(project_destination) == 0:
            if project_type.lower() != "none":
                raise AutosubmitCritical("Autosubmit couldn't identify the project destination.", 7014)

        if project_type == "git":

            try:
                submitter = Autosubmit._get_submitter(as_conf)
                submitter.load_platforms(as_conf)
                hpcarch = submitter.platforms[as_conf.get_platform()]
            except BaseException as e:
                try:
                    hpcarch = submitter.platforms[as_conf.get_platform()]
                except:
                    hpcarch = "local"
                Log.warning("Remote clone may be disabled due to: " + e.message)
            return AutosubmitGit.clone_repository(as_conf, force, hpcarch)
        elif project_type == "svn":
            svn_project_url = as_conf.get_svn_project_url()
            svn_project_revision = as_conf.get_svn_project_revision()
            project_path = os.path.join(
                BasicConfig.LOCAL_ROOT_DIR, expid, BasicConfig.LOCAL_PROJ_DIR)
            if os.path.exists(project_path):
                Log.info("Using project folder: {0}", project_path)
                if not force:
                    Log.debug("The project folder exists. SKIPPING...")
                    return True
                else:
                    shutil.rmtree(project_path, ignore_errors=True)
            try:
                os.mkdir(project_path)
            except BaseException as e:
                raise AutosubmitCritical(
                    "Project path:{0} can't be created. Revise that the path is the correct one.".format(project_path),
                    7014, e.message)

            Log.debug("The project folder {0} has been created.", project_path)
            Log.info("Checking out revision {0} into {1}",
                     svn_project_revision + " " + svn_project_url, project_path)
            try:
                output = subprocess.check_output("cd " + project_path + "; svn --force-interactive checkout -r " +
                                                 svn_project_revision + " " + svn_project_url + " " +
                                                 project_destination, shell=True)
            except subprocess.CalledProcessError:
                try:
                    shutil.rmtree(project_path, ignore_errors=True)
                except:
                    pass
                raise AutosubmitCritical(
                    "Can not check out revision {0} into {1}".format(svn_project_revision + " " + svn_project_url,
                                                                     project_path), 7062)
            Log.debug("{0}", output)

        elif project_type == "local":
            local_project_path = as_conf.get_local_project_path()
            if local_project_path is None or len(local_project_path) == 0:
                raise AutosubmitCritical("Empty project path! please change this parameter to a valid one.", 7014)
            project_path = os.path.join(
                BasicConfig.LOCAL_ROOT_DIR, expid, BasicConfig.LOCAL_PROJ_DIR)
            local_destination = os.path.join(project_path, project_destination)

            if os.path.exists(project_path):
                Log.info("Using project folder: {0}", project_path)
                if os.path.exists(local_destination):
                    if force:
                        try:
                            cmd = ["rsync -ach --info=progress2 " +
                                   local_project_path + "/* " + local_destination]
                            subprocess.call(cmd, shell=True)
                        except (subprocess.CalledProcessError, IOError):
                            raise AutosubmitCritical("Can not rsync {0} into {1}. Exiting...".format(
                                local_project_path, project_path), 7063)
                else:
                    os.mkdir(local_destination)
                    try:
                        output = subprocess.check_output(
                            "cp -R " + local_project_path + "/* " + local_destination, shell=True)
                    except subprocess.CalledProcessError:
                        try:
                            shutil.rmtree(project_path)
                        except:
                            pass
                        raise AutosubmitCritical("Can not copy {0} into {1}. Exiting...".format(
                            local_project_path, project_path), 7063)
            else:
                os.mkdir(project_path)
                os.mkdir(local_destination)
                Log.debug(
                    "The project folder {0} has been created.", project_path)
                Log.info("Copying {0} into {1}",
                         local_project_path, project_path)
                try:
                    output = subprocess.check_output(
                        "cp -R " + local_project_path + "/* " + local_destination, shell=True)
                except subprocess.CalledProcessError:
                    try:
                        shutil.rmtree(project_path)
                    except:
                        pass
                    raise AutosubmitCritical(
                        "Can not copy {0} into {1}. Exiting...".format(local_project_path, project_path), 7063)
                Log.debug("{0}", output)
        return True

    @staticmethod
    def change_status(final, final_status, job, save):
        # type: (str, int, Job, bool) -> None
        """
        Set job status to final

        :param final:
        :param final_status:
        :param job:
        """
        if save:
            if job.status in [Status.SUBMITTED, Status.QUEUING, Status.HELD] and final_status not in [Status.QUEUING,
                                                                                                      Status.HELD,
                                                                                                      Status.SUSPENDED]:
                job.hold = False
                if job.platform_name and job.platform_name.lower() != "local":
                    try:
                        job.platform.send_command(job.platform.cancel_cmd + " " + str(job.id), ignore_log=True)
                    except:
                        pass
            elif job.status in [Status.QUEUING, Status.RUNNING, Status.SUBMITTED] and final_status == Status.SUSPENDED:
                if job.platform_name and job.platform_name.lower() != "local":
                    job.platform.send_command("scontrol hold " + "{0}".format(job.id), ignore_log=True)
            elif final_status in [Status.QUEUING, Status.RUNNING] and (job.status == Status.SUSPENDED):
                if job.platform_name and job.platform_name.lower() != "local":
                    job.platform.send_command("scontrol release " + "{0}".format(job.id), ignore_log=True)
        job.status = final_status
        Log.info("CHANGED: job: " + job.name + " status to: " + final)
        Log.status("CHANGED: job: " + job.name + " status to: " + final)

    @staticmethod
    def set_status(expid, noplot, save, final, lst, filter_chunks, filter_status, filter_section, filter_type_chunk,
                   hide, group_by=None,
                   expand=list(), expand_status=list(), notransitive=False, check_wrapper=False, detail=False):
        """
        Set status

        :param expid: experiment identifier
        :type expid: str
        :param save: if true, saves the new jobs list
        :type save: bool
        :param final: status to set on jobs
        :type final: str
        :param lst: list of jobs to change status
        :type lst: str
        :param filter_chunks: chunks to change status
        :type filter_chunks: str
        :param filter_status: current status of the jobs to change status
        :type filter_status: str
        :param filter_section: sections to change status
        :type filter_section: str
        :param hide: hides plot window
        :type hide: bool
        """
        Autosubmit._check_ownership(expid, raise_error=True)
        exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid)
        tmp_path = os.path.join(exp_path, BasicConfig.LOCAL_TMP_DIR)
        section_validation_message = " "
        # checking if there is a lock file to avoid multiple running on the same expid
        try:
            with portalocker.Lock(os.path.join(tmp_path, 'autosubmit.lock'), timeout=1):
                Log.info(
                    "Preparing .lock file to avoid multiple instances with same expid.")

                Log.debug('Exp ID: {0}', expid)
                Log.debug('Save: {0}', save)
                Log.debug('Final status: {0}', final)
                Log.debug('List of jobs to change: {0}', lst)
                Log.debug('Chunks to change: {0}', filter_chunks)
                Log.debug('Status of jobs to change: {0}', filter_status)
                Log.debug('Sections to change: {0}', filter_section)
                wrongExpid = 0
                as_conf = AutosubmitConfig(
                    expid, BasicConfig, ConfigParserFactory())
                as_conf.check_conf_files(True)

                # Getting output type from configuration
                output_type = as_conf.get_output_type()
                # Getting db connections

                # Validating job sections, if filter_section -ft has been set:
                if filter_section is not None:
                    section_validation_error = False
                    section_error = False
                    section_not_foundList = list()
                    section_validation_message = "\n## Section Validation Message ##"
                    countStart = filter_section.count('[')
                    countEnd = filter_section.count(']')
                    if countStart > 1 or countEnd > 1:
                        section_validation_error = True
                        section_validation_message += "\n\tList of sections has a format error. Perhaps you were trying to use -fc instead."
                    # countUnderscore =  filter_section.count('_')
                    # if countUnderscore > 1:
                    #    section_validation_error = True
                    #    section_validation_message += "\n\tList of sections provided has a format error. Perhaps you were trying to use -fl instead."
                    if section_validation_error == False:
                        if len(str(filter_section).strip()) > 0:
                            if len(filter_section.split()) > 0:
                                jobSections = as_conf.get_jobs_sections()
                                for section in filter_section.split():
                                    # print(section)
                                    # Provided section is not an existing section or it is not the keyword 'Any'
                                    if section not in jobSections and (section != "Any"):
                                        section_error = True
                                        section_not_foundList.append(section)
                        else:
                            section_validation_error = True
                            section_validation_message += "\n\tEmpty input. No changes performed."
                    if section_validation_error == True or section_error == True:
                        if section_error == True:
                            section_validation_message += "\n\tSpecified section(s) : [" + str(section_not_foundList) + \
                                                          "] not found in the experiment " + str(expid) + \
                                                          ".\n\tProcess stopped. Review the format of the provided input. Comparison is case sensitive." + \
                                                          "\n\tRemember that this option expects section names separated by a blank space as input."

                        raise AutosubmitCritical(
                            "Error in the supplied input for -ft.", 7011, section_validation_message)
                job_list = Autosubmit.load_job_list(
                    expid, as_conf, notransitive=notransitive)
                submitter = Autosubmit._get_submitter(as_conf)
                submitter.load_platforms(as_conf)
                hpcarch = as_conf.get_platform()
                for job in job_list.get_job_list():
                    if job.platform_name is None:
                        job.platform_name = hpcarch
                    # noinspection PyTypeChecker
                    job.platform = submitter.platforms[job.platform_name.lower(
                    )]
                platforms_to_test = set()
                platforms = submitter.platforms
                for job in job_list.get_job_list():
                    job.submitter = submitter
                    if job.platform_name is None:
                        job.platform_name = hpcarch
                    # noinspection PyTypeChecker
                    job.platform = platforms[job.platform_name.lower()]
                    # noinspection PyTypeChecker
                    if job.status in [Status.QUEUING, Status.SUBMITTED, Status.RUNNING]:
                        platforms_to_test.add(
                            platforms[job.platform_name.lower()])
                # establish the connection to all platforms
                definitive_platforms = list()
                for platform in platforms_to_test:
                    try:
                        Autosubmit.restore_platforms([platform])
                        definitive_platforms.append(platform.name)
                    except Exception as e:
                        pass

                # Validating list of jobs, if filter_list -fl has been set:
                # Seems that Autosubmit.load_job_list call is necessary before verification is executed
                if job_list is not None and lst is not None:
                    job_validation_error = False
                    job_error = False
                    job_not_foundList = list()
                    job_validation_message = "\n## Job Validation Message ##"
                    jobs = list()
                    countStart = lst.count('[')
                    countEnd = lst.count(']')
                    if countStart > 1 or countEnd > 1:
                        job_validation_error = True
                        job_validation_message += "\n\tList of jobs has a format error. Perhaps you were trying to use -fc instead."

                    if job_validation_error == False:
                        for job in job_list.get_job_list():
                            jobs.append(job.name)
                        if len(str(lst).strip()) > 0:
                            if len(lst.split()) > 0:
                                for sentJob in lst.split():
                                    # Provided job does not exist or it is not the keyword 'Any'
                                    if sentJob not in jobs and (sentJob != "Any"):
                                        job_error = True
                                        job_not_foundList.append(sentJob)
                        else:
                            job_validation_error = True
                            job_validation_message += "\n\tEmpty input. No changes performed."

                    if job_validation_error == True or job_error == True:
                        if job_error == True:
                            job_validation_message += "\n\tSpecified job(s) : [" + str(
                                job_not_foundList) + "] not found in the experiment " + \
                                                      str(expid) + ". \n\tProcess stopped. Review the format of the provided input. Comparison is case sensitive." + \
                                                      "\n\tRemember that this option expects job names separated by a blank space as input."
                        raise AutosubmitCritical(
                            "Error in the supplied input for -ft.", 7011, section_validation_message)

                # Validating fc if filter_chunks -fc has been set:
                if filter_chunks is not None:
                    fc_validation_message = "## -fc Validation Message ##"
                    fc_filter_is_correct = True
                    selected_sections = filter_chunks.split(",")[1:]
                    selected_formula = filter_chunks.split(",")[0]
                    current_sections = as_conf.get_jobs_sections()
                    fc_deserializedJson = object()
                    # Starting Validation
                    if len(str(selected_sections).strip()) == 0:
                        fc_filter_is_correct = False
                        fc_validation_message += "\n\tMust include a section (job type)."
                    else:
                        for section in selected_sections:
                            # section = section.strip()
                            # Validating empty sections
                            if len(str(section).strip()) == 0:
                                fc_filter_is_correct = False
                                fc_validation_message += "\n\tEmpty sections are not accepted."
                                break
                            # Validating existing sections
                            # Retrieve experiment data

                            if section not in current_sections:
                                fc_filter_is_correct = False
                                fc_validation_message += "\n\tSection " + section + \
                                                         " does not exist in experiment. Remember not to include blank spaces."

                    # Validating chunk formula
                    if len(selected_formula) == 0:
                        fc_filter_is_correct = False
                        fc_validation_message += "\n\tA formula for chunk filtering has not been provided."

                    # If everything is fine until this point
                    if fc_filter_is_correct == True:
                        # Retrieve experiment data
                        current_dates = as_conf._exp_parser.get_option(
                            'experiment', 'DATELIST', '').split()
                        current_members = as_conf.get_member_list()
                        # Parse json
                        try:
                            fc_deserializedJson = json.loads(
                                Autosubmit._create_json(selected_formula))
                        except:
                            fc_filter_is_correct = False
                            fc_validation_message += "\n\tProvided chunk formula does not have the right format. Were you trying to use another option?"
                        if fc_filter_is_correct == True:
                            for startingDate in fc_deserializedJson['sds']:
                                if startingDate['sd'] not in current_dates:
                                    fc_filter_is_correct = False
                                    fc_validation_message += "\n\tStarting date " + \
                                                             startingDate['sd'] + \
                                                             " does not exist in experiment."
                                for member in startingDate['ms']:
                                    if member['m'] not in current_members and member['m'].lower() != "any":
                                        fc_filter_is_correct = False
                                        fc_validation_message += "\n\tMember " + \
                                                                 member['m'] + \
                                                                 " does not exist in experiment."

                    # Ending validation
                    if fc_filter_is_correct == False:
                        section_validation_message = fc_validation_message
                        raise AutosubmitCritical(
                            "Error in the supplied input for -fc.", 7011, section_validation_message)
                # Validating status, if filter_status -fs has been set:
                # At this point we already have job_list from where we are getting the allows STATUS
                if filter_status is not None:
                    status_validation_error = False
                    status_validation_message = "\n## Status Validation Message ##"
                    # Trying to identify chunk formula
                    countStart = filter_status.count('[')
                    countEnd = filter_status.count(']')
                    if countStart > 1 or countEnd > 1:
                        status_validation_error = True
                        status_validation_message += "\n\tList of status provided has a format error. Perhaps you were trying to use -fc instead."
                    # Trying to identify job names, implying status names won't use more than 1 underscore _
                    # countUnderscore = filter_status.count('_')
                    # if countUnderscore > 1:
                    #    status_validation_error = True
                    #    status_validation_message += "\n\tList of status provided has a format error. Perhaps you were trying to use -fl instead."
                    # If everything is fine until this point
                    if status_validation_error == False:
                        status_filter = filter_status.split()
                        status_reference = Status()
                        status_list = list()
                        for job in job_list.get_job_list():
                            reference = status_reference.VALUE_TO_KEY[job.status]
                            if reference not in status_list:
                                status_list.append(reference)
                        for status in status_filter:
                            if status not in status_list:
                                status_validation_error = True
                                status_validation_message += "\n\t There are no jobs with status " + \
                                                             status + " in this experiment."
                    if status_validation_error == True:
                        raise AutosubmitCritical("Error in the supplied input for -fs.{0}".format(
                            status_validation_message), 7011, section_validation_message)

                jobs_filtered = []
                final_status = Autosubmit._get_status(final)
                if filter_section or filter_chunks:
                    if filter_section:
                        ft = filter_section.split()
                    else:
                        ft = filter_chunks.split(",")[1:]
                    if ft == 'Any':
                        for job in job_list.get_job_list():
                            Autosubmit.change_status(
                                final, final_status, job, save)
                    else:
                        for section in ft:
                            for job in job_list.get_job_list():
                                if job.section == section:
                                    if filter_chunks:
                                        jobs_filtered.append(job)
                                    else:
                                        Autosubmit.change_status(
                                            final, final_status, job, save)

                # New feature : Change status by section, member, and chunk; freely.
                # Including inner validation. Trying to make it independent.
                # 19601101 [ fc0 [1 2 3 4] Any [1] ] 19651101 [ fc0 [16-30] ] ],SIM,SIM2,SIM3
                if filter_type_chunk:
                    validation_message = "## -ftc Validation Message ##"
                    filter_is_correct = True
                    selected_sections = filter_type_chunk.split(",")[1:]
                    selected_formula = filter_type_chunk.split(",")[0]
                    deserializedJson = object()
                    performed_changes = dict()

                    # Starting Validation
                    if len(str(selected_sections).strip()) == 0:
                        filter_is_correct = False
                        validation_message += "\n\tMust include a section (job type). If you want to apply the changes to all sections, include 'Any'."
                    else:
                        for section in selected_sections:
                            # Validating empty sections
                            if len(str(section).strip()) == 0:
                                filter_is_correct = False
                                validation_message += "\n\tEmpty sections are not accepted."
                                break
                            # Validating existing sections
                            # Retrieve experiment data
                            current_sections = as_conf.get_jobs_sections()
                            if section not in current_sections and section != "Any":
                                filter_is_correct = False
                                validation_message += "\n\tSection " + \
                                                      section + " does not exist in experiment."

                    # Validating chunk formula
                    if len(selected_formula) == 0:
                        filter_is_correct = False
                        validation_message += "\n\tA formula for chunk filtering has not been provided. If you want to change all chunks, include 'Any'."

                    # If everything is fine until this point
                    if filter_is_correct == True:
                        # Retrieve experiment data
                        current_dates = as_conf._exp_parser.get_option(
                            'experiment', 'DATELIST', '').split()
                        current_members = as_conf.get_member_list()
                        # Parse json
                        try:
                            deserializedJson = json.loads(
                                Autosubmit._create_json(selected_formula))
                        except:
                            filter_is_correct = False
                            validation_message += "\n\tProvided chunk formula does not have the right format. Were you trying to use another option?"
                        if filter_is_correct == True:
                            for startingDate in deserializedJson['sds']:
                                if startingDate['sd'] not in current_dates:
                                    filter_is_correct = False
                                    validation_message += "\n\tStarting date " + \
                                                          startingDate['sd'] + \
                                                          " does not exist in experiment."
                                for member in startingDate['ms']:
                                    if member['m'] not in current_members and member['m'] != "Any":
                                        filter_is_correct_ = False
                                        validation_message += "\n\tMember " + \
                                                              member['m'] + \
                                                              " does not exist in experiment."

                    # Ending validation
                    if filter_is_correct == False:
                        raise AutosubmitCritical(
                            "Error in the supplied input for -ftc.", 7011, validation_message)

                    # If input is valid, continue.
                    record = dict()
                    final_list = []
                    # Get current list
                    working_list = job_list.get_job_list()
                    for section in selected_sections:
                        if section == "Any":
                            # Any section
                            section_selection = working_list
                            # Go through start dates
                            for starting_date in deserializedJson['sds']:
                                date = starting_date['sd']
                                date_selection = filter(lambda j: date2str(
                                    j.date) == date, section_selection)
                                # Members for given start date
                                for member_group in starting_date['ms']:
                                    member = member_group['m']
                                    if member == "Any":
                                        # Any member
                                        member_selection = date_selection
                                        chunk_group = member_group['cs']
                                        for chunk in chunk_group:
                                            filtered_job = filter(
                                                lambda j: j.chunk == int(chunk), member_selection)
                                            for job in filtered_job:
                                                final_list.append(job)
                                            # From date filter and sync is not None
                                            for job in filter(
                                                    lambda j: j.chunk == int(chunk) and j.synchronize is not None,
                                                    date_selection):
                                                final_list.append(job)
                                    else:
                                        # Selected members
                                        member_selection = filter(
                                            lambda j: j.member == member, date_selection)
                                        chunk_group = member_group['cs']
                                        for chunk in chunk_group:
                                            filtered_job = filter(
                                                lambda j: j.chunk == int(chunk), member_selection)
                                            for job in filtered_job:
                                                final_list.append(job)
                                            # From date filter and sync is not None
                                            for job in filter(
                                                    lambda j: j.chunk == int(chunk) and j.synchronize is not None,
                                                    date_selection):
                                                final_list.append(job)
                        else:
                            # Only given section
                            section_selection = filter(
                                lambda j: j.section == section, working_list)
                            # Go through start dates
                            for starting_date in deserializedJson['sds']:
                                date = starting_date['sd']
                                date_selection = filter(lambda j: date2str(
                                    j.date) == date, section_selection)
                                # Members for given start date
                                for member_group in starting_date['ms']:
                                    member = member_group['m']
                                    if member == "Any":
                                        # Any member
                                        member_selection = date_selection
                                        chunk_group = member_group['cs']
                                        for chunk in chunk_group:
                                            filtered_job = filter(
                                                lambda j: j.chunk is None or j.chunk == int(chunk), member_selection)
                                            for job in filtered_job:
                                                final_list.append(job)
                                            # From date filter and sync is not None
                                            for job in filter(
                                                    lambda j: j.chunk == int(chunk) and j.synchronize is not None,
                                                    date_selection):
                                                final_list.append(job)
                                    else:
                                        # Selected members
                                        member_selection = filter(
                                            lambda j: j.member == member, date_selection)
                                        chunk_group = member_group['cs']
                                        for chunk in chunk_group:
                                            filtered_job = filter(
                                                lambda j: j.chunk == int(chunk), member_selection)
                                            for job in filtered_job:
                                                final_list.append(job)
                                            # From date filter and sync is not None
                                            for job in filter(
                                                    lambda j: j.chunk == int(chunk) and j.synchronize is not None,
                                                    date_selection):
                                                final_list.append(job)
                    status = Status()
                    for job in final_list:
                        if job.status in [Status.QUEUING, Status.RUNNING,
                                          Status.SUBMITTED] and job.platform.name not in definitive_platforms:
                            Log.printlog("JOB: [{1}] is ignored as the [{0}] platform is currently offline".format(
                                job.platform.name, job.name), 6000)
                            continue
                        if job.status != final_status:
                            # Only real changes
                            performed_changes[job.name] = str(
                                Status.VALUE_TO_KEY[job.status]) + " -> " + str(final)
                            Autosubmit.change_status(
                                final, final_status, job, save)
                    # If changes have been performed
                    if len(performed_changes.keys()) > 0:
                        if detail == True:
                            current_length = len(job_list.get_job_list())
                            if current_length > 1000:
                                Log.warning(
                                    "-d option: Experiment has too many jobs to be printed in the terminal. Maximum job quantity is 1000, your experiment has " + str(
                                        current_length) + " jobs.")
                            else:
                                Log.info(job_list.print_with_status(
                                    statusChange=performed_changes))
                    else:
                        Log.warning("No changes were performed.")
                # End of New Feature

                if filter_chunks:
                    if len(jobs_filtered) == 0:
                        jobs_filtered = job_list.get_job_list()

                    fc = filter_chunks
                    Log.debug(fc)

                    if fc == 'Any':
                        for job in jobs_filtered:
                            Autosubmit.change_status(
                                final, final_status, job, save)
                    else:
                        # noinspection PyTypeChecker
                        data = json.loads(Autosubmit._create_json(fc))
                        for date_json in data['sds']:
                            date = date_json['sd']
                            if len(str(date)) < 9:
                                format = "D"
                            elif len(str(date)) < 11:
                                format = "H"
                            elif len(str(date)) < 13:
                                format = "M"
                            elif len(str(date)) < 15:
                                format = "S"
                            else:
                                format = "D"
                            jobs_date = filter(lambda j: date2str(
                                j.date, format) == date, jobs_filtered)

                            for member_json in date_json['ms']:
                                member = member_json['m']
                                jobs_member = filter(
                                    lambda j: j.member == member or member.lower() == "any", jobs_date)

                                for chunk_json in member_json['cs']:
                                    chunk = int(chunk_json)
                                    for job in filter(lambda j: (j.chunk == chunk or str(
                                            chunk).lower() == "any") and j.synchronize is not None, jobs_date):
                                        Autosubmit.change_status(
                                            final, final_status, job, save)

                                    for job in filter(lambda j: j.chunk == chunk, jobs_member):
                                        Autosubmit.change_status(
                                            final, final_status, job, save)

                if filter_status:
                    status_list = filter_status.split()

                    Log.debug("Filtering jobs with status {0}", filter_status)
                    if status_list == 'Any':
                        for job in job_list.get_job_list():
                            Autosubmit.change_status(
                                final, final_status, job, save)
                    else:
                        for status in status_list:
                            fs = Autosubmit._get_status(status)
                            for job in filter(lambda j: j.status == fs, job_list.get_job_list()):
                                Autosubmit.change_status(
                                    final, final_status, job, save)

                if lst:
                    jobs = lst.split()
                    expidJoblist = defaultdict(int)
                    for x in lst.split():
                        expidJoblist[str(x[0:4])] += 1

                    if str(expid) in expidJoblist:
                        wrongExpid = jobs.__len__() - expidJoblist[expid]
                    if wrongExpid > 0:
                        Log.warning(
                            "There are {0} job.name with an invalid Expid", wrongExpid)

                    if jobs == 'Any':
                        for job in job_list.get_job_list():
                            Autosubmit.change_status(
                                final, final_status, job, save)
                    else:
                        for job in job_list.get_job_list():
                            if job.name in jobs:
                                Autosubmit.change_status(
                                    final, final_status, job, save)

                job_list.update_list(as_conf, False, True)

                if save and wrongExpid == 0:
                    job_list.save()
                    exp_history = ExperimentHistory(expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR,
                                                    historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR)
                    exp_history.initialize_database()
                    exp_history.process_status_changes(job_list.get_job_list(),
                                                       chunk_unit=as_conf.get_chunk_size_unit(),
                                                       chunk_size=as_conf.get_chunk_size(),
                                                       current_config=as_conf.get_full_config_as_json())
                    Autosubmit.database_backup(expid)
                else:
                    Log.printlog(
                        "Changes NOT saved to the JobList!!!!:  use -s option to save", 3000)

                if as_conf.get_wrapper_type() != 'none' and check_wrapper:
                    packages_persistence = JobPackagePersistence(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl"),
                                                                 "job_packages_" + expid)
                    os.chmod(os.path.join(BasicConfig.LOCAL_ROOT_DIR,
                                          expid, "pkl", "job_packages_" + expid + ".db"), 0775)
                    packages_persistence.reset_table(True)
                    referenced_jobs_to_remove = set()
                    job_list_wrappers = copy.deepcopy(job_list)
                    jobs_wr = copy.deepcopy(job_list.get_job_list())
                    [job for job in jobs_wr if (
                            job.status != Status.COMPLETED)]
                    for job in jobs_wr:
                        for child in job.children:
                            if child not in jobs_wr:
                                referenced_jobs_to_remove.add(child)
                        for parent in job.parents:
                            if parent not in jobs_wr:
                                referenced_jobs_to_remove.add(parent)

                    for job in jobs_wr:
                        job.children = job.children - referenced_jobs_to_remove
                        job.parents = job.parents - referenced_jobs_to_remove
                    Autosubmit.generate_scripts_andor_wrappers(as_conf, job_list_wrappers, jobs_wr,
                                                               packages_persistence, True)

                    packages = packages_persistence.load(True)
                else:
                    packages = JobPackagePersistence(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl"),
                                                     "job_packages_" + expid).load()
                if not noplot:
                    groups_dict = dict()
                    if group_by:
                        status = list()
                        if expand_status:
                            for s in expand_status.split():
                                status.append(
                                    Autosubmit._get_status(s.upper()))

                        job_grouping = JobGrouping(group_by, copy.deepcopy(job_list.get_job_list()), job_list,
                                                   expand_list=expand,
                                                   expanded_status=status)
                        groups_dict = job_grouping.group_jobs()
                    Log.info("\nPloting joblist...")
                    monitor_exp = Monitor()
                    monitor_exp.generate_output(expid,
                                                job_list.get_job_list(),
                                                os.path.join(
                                                    exp_path, "/tmp/LOG_", expid),
                                                output_format=output_type,
                                                packages=packages,
                                                show=not hide,
                                                groups=groups_dict,
                                                job_list_object=job_list)

                if not filter_type_chunk and detail == True:
                    Log.warning("-d option only works with -ftc.")
                return True

        except portalocker.AlreadyLocked:
            message = "We have detected that there is another Autosubmit instance using the experiment\n. Stop other Autosubmit instances that are using the experiment or delete autosubmit.lock file located on tmp folder"
            raise AutosubmitCritical(message, 7000)
        except (AutosubmitError, AutosubmitCritical):
            raise
        except BaseException as e:
            raise AutosubmitCritical(
                "An Error has occurred while setting some of the workflow jobs, no changes were made", 7040, e.message)

    @staticmethod
    def _user_yes_no_query(question):
        """
        Utility function to ask user a yes/no question

        :param question: question to ask
        :type question: str
        :return: True if answer is yes, False if it is no
        :rtype: bool
        """
        sys.stdout.write('{0} [y/n]\n'.format(question))
        while True:
            try:
                if sys.version_info[0] == 3:
                    answer = raw_input()
                else:
                    # noinspection PyCompatibility
                    answer = raw_input()
                return strtobool(answer.lower())
            except EOFError as e:
                raise AutosubmitCritical("No input detected, the experiment won't be erased.", 7011, e.message)
            except ValueError:
                sys.stdout.write('Please respond with \'y\' or \'n\'.\n')

    @staticmethod
    def _prepare_conf_files(exp_id, hpc, autosubmit_version, dummy, copy_id):
        """
        Changes default configuration files to match new experiment values

        :param exp_id: experiment identifier
        :type exp_id: str
        :param hpc: hpc to use
        :type hpc: str
        :param autosubmit_version: current autosubmit's version
        :type autosubmit_version: str
        :param dummy: if True, creates a dummy experiment adding some default values
        :type dummy: bool
        """
        as_conf = AutosubmitConfig(exp_id, BasicConfig, ConfigParserFactory())
        as_conf.set_version(autosubmit_version)
        as_conf.set_expid(exp_id)
        as_conf.set_platform(hpc)

        if dummy or copy_id is None:
            content = open(as_conf.experiment_file).read()

            # Experiment
            content = content.replace(re.search('^DATELIST =.*', content, re.MULTILINE).group(0),
                                      "DATELIST = 20000101")
            content = content.replace(re.search('^MEMBERS =.*', content, re.MULTILINE).group(0),
                                      "MEMBERS = fc0")
            content = content.replace(re.search('^CHUNKSIZE =.*', content, re.MULTILINE).group(0),
                                      "CHUNKSIZE = 4")
            content = content.replace(re.search('^NUMCHUNKS =.*', content, re.MULTILINE).group(0),
                                      "NUMCHUNKS = 1")
            content = content.replace(re.search('^PROJECT_TYPE =.*', content, re.MULTILINE).group(0),
                                      "PROJECT_TYPE = none")

            open(as_conf.experiment_file, 'w').write(content)

    @staticmethod
    def _get_status(s):
        """
        Convert job status from str to Status

        :param s: status string
        :type s: str
        :return: status instance
        :rtype: Status
        """
        s = s.upper()
        if s == 'READY':
            return Status.READY
        elif s == 'COMPLETED':
            return Status.COMPLETED
        elif s == 'WAITING':
            return Status.WAITING
        elif s == 'HELD':
            return Status.HELD
        elif s == 'SUSPENDED':
            return Status.SUSPENDED
        elif s == 'FAILED':
            return Status.FAILED
        elif s == 'RUNNING':
            return Status.RUNNING
        elif s == 'QUEUING':
            return Status.QUEUING
        elif s == 'UNKNOWN':
            return Status.UNKNOWN

    @staticmethod
    def _get_members(out):
        """
        Function to get a list of members from json

        :param out: json member definition
        :type out: str
        :return: list of members
        :rtype: list
        """
        count = 0
        data = []
        # noinspection PyUnusedLocal
        for element in out:
            if count % 2 == 0:
                ms = {'m': out[count],
                      'cs': Autosubmit._get_chunks(out[count + 1])}
                data.append(ms)
                count += 1
            else:
                count += 1

        return data

    @staticmethod
    def _get_chunks(out):
        """
        Function to get a list of chunks from json

        :param out: json member definition
        :type out: str
        :return: list of chunks
        :rtype: list
        """
        data = []
        for element in out:
            if element.find("-") != -1:
                numbers = element.split("-")
                for count in xrange(int(numbers[0]), int(numbers[1]) + 1):
                    data.append(str(count))
            else:
                data.append(element)

        return data

    @staticmethod
    def _get_submitter(as_conf):
        """
        Returns the submitter corresponding to the communication defined on autosubmit's config file

        :return: submitter
        :rtype: Submitter
        """
        try:
            communications_library = as_conf.get_communications_library()
        except:
            communications_library = 'paramiko'
        if communications_library == 'paramiko':
            return ParamikoSubmitter()
        else:
            # only paramiko is avaliable right now so..
            return ParamikoSubmitter()

    @staticmethod
    def _get_job_list_persistence(expid, as_conf):
        """
        Returns the JobListPersistence corresponding to the storage type defined on autosubmit's config file

        :return: job_list_persistence
        :rtype: JobListPersistence
        """
        storage_type = as_conf.get_storage_type()
        if storage_type == 'pkl':
            return JobListPersistencePkl()
        elif storage_type == 'db':
            return JobListPersistenceDb(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl"),
                                        "job_list_" + expid)
        raise AutosubmitCritical('Storage type not known', 7014)

    @staticmethod
    def _create_json(text):
        """
        Function to parse rerun specification from json format

        :param text: text to parse
        :type text: list
        :return: parsed output
        """
        count = 0
        data = []

        # text = "[ 19601101 [ fc0 [1 2 3 4] fc1 [1] ] 16651101 [ fc0 [1-30 31 32] ] ]"

        def parse_date(datestring):
            result = []
            startindex = datestring.find('(')
            endindex = datestring.find(')')
            if startindex > 0 and endindex > 0:
                try:
                    startstring = datestring[:startindex]
                    startrange = datestring[startindex + 1:].split('-')[0]
                    endrange = datestring[startindex:-1].split('-')[1]
                    startday = int(startrange[-2:])
                    endday = int(endrange[-2:])

                    frommonth = int(startrange[:2])
                    tomonth = int(endrange[:2])

                    for i in range(frommonth, tomonth + 1):
                        for j in range(startday, endday + 1):
                            result.append(startstring + "%02d" %
                                          i + "%02d" % j)
                except Exception as exp:
                    raise AutosubmitCritical(
                        "Autosubmit couldn't parse your input format. Exception: {0}".format(exp))

            else:
                result = [datestring]
            return result

        out = nestedExpr('[', ']').parseString(text).asList()

        # noinspection PyUnusedLocal
        for element in out[0]:
            if count % 2 == 0:
                datelist = parse_date(out[0][count])
                for item in datelist:
                    sd = {'sd': item, 'ms': Autosubmit._get_members(
                        out[0][count + 1])}
                    data.append(sd)
                count += 1
            else:
                count += 1

        sds = {'sds': data}
        result = json.dumps(sds)
        return result

    @staticmethod
    def testcase(copy_id, description, chunks=None, member=None, start_date=None, hpc=None, branch=None):
        """
        Method to create a test case. It creates a new experiment whose id starts by 't'.


        :param copy_id: experiment identifier
        :type copy_id: str
        :param description: test case experiment description
        :type description: str
        :param chunks: number of chunks to be run by the experiment. If None, it uses configured chunk(s).
        :type chunks: int
        :param member: member to be used by the test. If None, it uses configured member(s).
        :type member: str
        :param start_date: start date to be used by the test. If None, it uses configured start date(s).
        :type start_date: str
        :param hpc: HPC to be used by the test. If None, it uses configured HPC.
        :type hpc: str
        :param branch: branch or revision to be used by the test. If None, it uses configured branch.
        :type branch: str
        :return: test case id
        :rtype: str
        """

        testcaseid = Autosubmit.expid(hpc, description, copy_id, False, True)
        if testcaseid == '':
            return False

        Autosubmit._change_conf(
            testcaseid, hpc, start_date, member, chunks, branch, False)

        return testcaseid

    @staticmethod
    def test(expid, chunks, member=None, start_date=None, hpc=None, branch=None):
        """
        Method to conduct a test for a given experiment. It creates a new experiment for a given experiment with a
        given number of chunks with a random start date and a random member to be run on a random HPC.


        :param expid: experiment identifier
        :type expid: str
        :param chunks: number of chunks to be run by the experiment
        :type chunks: int
        :param member: member to be used by the test. If None, it uses a random one from which are defined on
                       the experiment.
        :type member: str
        :param start_date: start date to be used by the test. If None, it uses a random one from which are defined on
                         the experiment.
        :type start_date: str
        :param hpc: HPC to be used by the test. If None, it uses a random one from which are defined on
                    the experiment.
        :type hpc: str
        :param branch: branch or revision to be used by the test. If None, it uses configured branch.
        :type branch: str
        :return: True if test was succesful, False otherwise
        :rtype: bool
        """
        testid = Autosubmit.expid(
            'test', 'test experiment for {0}'.format(expid), expid, False, True)
        if testid == '':
            return False

        Autosubmit._change_conf(testid, hpc, start_date,
                                member, chunks, branch, True)

        Autosubmit.create(testid, False, True)
        if not Autosubmit.run_experiment(testid):
            return False
        return True

    @staticmethod
    def _change_conf(testid, hpc, start_date, member, chunks, branch, random_select=False):
        as_conf = AutosubmitConfig(testid, BasicConfig, ConfigParserFactory())
        exp_parser = as_conf.get_parser(
            ConfigParserFactory(), as_conf.experiment_file)
        try:
            if exp_parser.get_bool_option('rerun', "RERUN", True):
                raise AutosubmitCritical('Can not test a RERUN experiment', 7014)
        except:
            pass
        content = open(as_conf.experiment_file).read()

        if random_select:
            if hpc is None:
                platforms_parser = as_conf.get_parser(
                    ConfigParserFactory(), as_conf.platforms_file)

                test_platforms = list()
                for section in platforms_parser.sections():
                    if platforms_parser.get_option(section, 'TEST_SUITE', 'false').lower() == 'true':
                        test_platforms.append(section)
                if len(test_platforms) == 0:
                    raise AutosubmitCritical(
                        "Missing hpcarch setting in expdef", 7014)

                hpc = random.choice(test_platforms)
            if member is None:
                member = random.choice(exp_parser.get(
                    'experiment', 'MEMBERS').split(' '))
            if start_date is None:
                start_date = random.choice(exp_parser.get(
                    'experiment', 'DATELIST').split(' '))
            if chunks is None:
                chunks = 1

        # Experiment
        content = content.replace(re.search('^EXPID =.*', content, re.MULTILINE).group(0),
                                  "EXPID = " + testid)
        if start_date is not None:
            content = content.replace(re.search('^DATELIST =.*', content, re.MULTILINE).group(0),
                                      "DATELIST = " + start_date)
        if member is not None:
            content = content.replace(re.search('^MEMBERS =.*', content, re.MULTILINE).group(0),
                                      "MEMBERS = " + member)
        if chunks is not None:
            # noinspection PyTypeChecker
            content = content.replace(re.search('^NUMCHUNKS =.*', content, re.MULTILINE).group(0),
                                      "NUMCHUNKS = " + chunks)
        if hpc is not None:
            content = content.replace(re.search('^HPCARCH =.*', content, re.MULTILINE).group(0),
                                      "HPCARCH = " + hpc)
        if branch is not None:
            content = content.replace(re.search('^PROJECT_BRANCH =.*', content, re.MULTILINE).group(0),
                                      "PROJECT_BRANCH = " + branch)
            content = content.replace(re.search('^PROJECT_REVISION =.*', content, re.MULTILINE).group(0),
                                      "PROJECT_REVISION = " + branch)

        open(as_conf.experiment_file, 'w').write(content)

    @staticmethod
    def load_job_list(expid, as_conf, notransitive=False, monitor=False):
        rerun = as_conf.get_rerun()

        job_list = JobList(expid, BasicConfig, ConfigParserFactory(),
                           Autosubmit._get_job_list_persistence(expid, as_conf))
        run_only_members = as_conf.get_member_list(run_only=True)
        date_list = as_conf.get_date_list()
        date_format = ''
        if as_conf.get_chunk_size_unit() is 'hour':
            date_format = 'H'
        for date in date_list:
            if date.hour > 1:
                date_format = 'H'
            if date.minute > 1:
                date_format = 'M'
        wrapper_jobs = dict()
        wrapper_jobs["wrapper"] = as_conf.get_wrapper_jobs()
        if as_conf.get_wrapper_type() == "multi":
            for wrapper_section in as_conf.get_wrapper_multi():
                wrapper_jobs[wrapper_section] = as_conf.get_wrapper_jobs(wrapper_section)

        job_list.generate(date_list, as_conf.get_member_list(), as_conf.get_num_chunks(), as_conf.get_chunk_ini(),
                          as_conf.load_parameters(), date_format, as_conf.get_retrials(),
                          as_conf.get_default_job_type(), as_conf.get_wrapper_type(), wrapper_jobs,
                          new=False, notransitive=notransitive, run_only_members=run_only_members)
        if rerun == "true":
            rerun_jobs = as_conf.get_rerun_jobs()
            job_list.rerun(rerun_jobs, monitor=monitor)
        else:
            job_list.remove_rerun_only_jobs(notransitive)

        return job_list

    @staticmethod
    def rerun_recovery(expid, job_list, rerun_list, as_conf):
        """
        Method to check all active jobs. If COMPLETED file is found, job status will be changed to COMPLETED,
        otherwise it will be set to WAITING. It will also update the jobs list.

        :param expid: identifier of the experiment to recover
        :type expid: str
        :param save: If true, recovery saves changes to the jobs list
        :type save: bool
        :param all_jobs: if True, it tries to get completed files for all jobs, not only active.
        :type all_jobs: bool
        :param hide: hides plot window
        :type hide: bool
        """

        hpcarch = as_conf.get_platform()
        submitter = Autosubmit._get_submitter(as_conf)
        try:
            submitter.load_platforms(as_conf)
            if submitter.platforms is None:
                raise AutosubmitCritical("platforms couldn't be loaded", 7014)
        except:
            raise AutosubmitCritical("platforms couldn't be loaded", 7014)
        platforms = submitter.platforms

        platforms_to_test = set()
        for job in job_list.get_job_list():
            if job.platform_name is None:
                job.platform_name = hpcarch
            # noinspection PyTypeChecker
            job.platform = platforms[job.platform_name.lower()]
            # noinspection PyTypeChecker
            platforms_to_test.add(platforms[job.platform_name.lower()])
        rerun_names = []

        [rerun_names.append(job.name) for job in rerun_list.get_job_list()]
        jobs_to_recover = [
            i for i in job_list.get_job_list() if i.name not in rerun_names]

        Log.info("Looking for COMPLETED files")
        start = datetime.datetime.now()
        for job in jobs_to_recover:
            if job.platform_name is None:
                job.platform_name = hpcarch
            # noinspection PyTypeChecker
            job.platform = platforms[job.platform_name.lower()]

            if job.platform.get_completed_files(job.name, 0):
                job.status = Status.COMPLETED
                Log.info(
                    "CHANGED job '{0}' status to COMPLETED".format(job.name))

            job.platform.get_logs_files(expid, job.remote_logs)
        return job_list
