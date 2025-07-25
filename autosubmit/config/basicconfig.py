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

try:
    # noinspection PyCompatibility
    from configparser import SafeConfigParser
except ImportError:
    # noinspection PyCompatibility
    from configparser import ConfigParser as SafeConfigParser
import inspect
import os
from pathlib import Path


class BasicConfig:
    """
    Class to manage configuration for Autosubmit path, database and default values for new experiments
    """

    def __init__(self):
        pass

    def props(self):
        pr = {}
        for name in dir(self):
            value = getattr(self, name)
            if not name.startswith('__') and not inspect.ismethod(value) and not inspect.isfunction(value):
                pr[name] = value
        return pr

    DB_DIR = os.path.join(os.path.expanduser('~'), 'debug', 'autosubmit')
    LOG_RECOVERY_TIMEOUT = 60
    STRUCTURES_DIR = os.path.join(
        '/esarchive', 'autosubmit', 'as_metadata', 'structures')
    GLOBAL_LOG_DIR = os.path.join(
        '/esarchive', 'autosubmit', 'Aslogs')
    DEFAULT_OUTPUT_DIR = os.path.join('/esarchive', 'autosubmit', 'as_output', 'stats')
    JOBDATA_DIR = os.path.join(
        '/esarchive', 'autosubmit', 'as_metadata', 'data')
    HISTORICAL_LOG_DIR = os.path.join('/esarchive', 'autosubmit', 'as_metadata', 'logs')
    AUTOSUBMIT_API_URL = "http://192.168.11.91:8081"
    DB_FILE = 'autosubmit.db'
    AS_TIMES_DB = 'as_times.db'
    DB_PATH = os.path.join(DB_DIR, DB_FILE)
    LOCAL_ROOT_DIR = DB_DIR
    LOCAL_TMP_DIR = 'tmp'
    LOCAL_ASLOG_DIR = 'ASLOGS'
    LOCAL_PROJ_DIR = 'proj'
    DEFAULT_PLATFORMS_CONF = ''
    CUSTOM_PLATFORMS_PATH = ''
    DEFAULT_JOBS_CONF = ''
    SMTP_SERVER = ''
    MAIL_FROM = ''
    ALLOWED_HOSTS = ''
    DENIED_HOSTS = ''
    CONFIG_FILE_FOUND = False
    DATABASE_BACKEND = "sqlite"
    DATABASE_CONN_URL = ""

    @staticmethod
    def expid_dir(exp_id):
        if not isinstance(exp_id, str) or len(exp_id) != 4 or "/" in exp_id:
            raise TypeError("Experiment ID must be a string of 4 characters without the folder separator symbol")
        return Path(BasicConfig.LOCAL_ROOT_DIR, exp_id)

    @staticmethod
    def expid_tmp_dir(exp_id):
        return BasicConfig.expid_dir(exp_id).joinpath(BasicConfig.LOCAL_TMP_DIR)

    @staticmethod
    def expid_log_dir(exp_id):
        return BasicConfig.expid_tmp_dir(exp_id).joinpath(f'LOG_{exp_id}')

    @staticmethod
    def expid_aslog_dir(exp_id):
        return BasicConfig.expid_tmp_dir(exp_id).joinpath(BasicConfig.LOCAL_ASLOG_DIR)

    @staticmethod
    def _update_config():
        """
        Updates commonly used composed paths
        """
        # Just one needed for the moment.
        BasicConfig.DB_PATH = os.path.join(
            BasicConfig.DB_DIR, BasicConfig.DB_FILE)

    @staticmethod
    def __read_file_config(file_path):
        """
        Reads configuration file. If configuration file dos not exist in given path,
        no error is raised. Configuration options also are not required to exist

        :param file_path: configuration file to read
        :type file_path: str
        """
        if not os.path.isfile(file_path):
            return
        else:
            BasicConfig.CONFIG_FILE_FOUND = True
        # print('Reading config from ' + file_path)
        parser = SafeConfigParser()
        parser.optionxform = str
        parser.read(file_path)

        if parser.has_option('database', 'path'):
            BasicConfig.DB_DIR = parser.get('database', 'path')
        if parser.has_option('database', 'filename'):
            BasicConfig.DB_FILE = parser.get('database', 'filename')
        if parser.has_option('local', 'path'):
            BasicConfig.LOCAL_ROOT_DIR = parser.get('local', 'path')
        if parser.has_option('conf', 'platforms'):
            BasicConfig.DEFAULT_PLATFORMS_CONF = parser.get(
                'conf', 'platforms')
        if parser.has_option('conf', 'custom_platforms'):
            BasicConfig.CUSTOM_PLATFORMS_PATH = parser.get(
                'conf', 'custom_platforms')
        if parser.has_option('conf', 'jobs'):
            BasicConfig.DEFAULT_JOBS_CONF = parser.get('conf', 'jobs')
        if parser.has_option('mail', 'smtp_server'):
            BasicConfig.SMTP_SERVER = parser.get('mail', 'smtp_server')
        if parser.has_option('mail', 'mail_from'):
            BasicConfig.MAIL_FROM = parser.get('mail', 'mail_from')
        if parser.has_option('hosts', 'authorized'):
            list_command_allowed = parser.get('hosts', 'authorized')

            list_command_allowed = list_command_allowed.split('] ')
            i = 0
            for _ in list_command_allowed:
                list_command_allowed[i] = list_command_allowed[i].strip('[]')
                i = i + 1
            restrictions = dict()
            for command_unparsed in list_command_allowed:
                command_allowed = command_unparsed.split(' ')
                if ',' in command_allowed[0]:
                    for command in command_allowed[0].split(','):
                        if ',' in command_allowed[1]:
                            restrictions[command] = command_allowed[1].split(',')
                        else:
                            restrictions[command] = [command_allowed[1]]
                else:
                    if ',' in command_allowed[1]:
                        restrictions[command_allowed[0]] = command_allowed[1].split(',')
                    else:
                        restrictions[command_allowed[0]] = [command_allowed[1]]
            BasicConfig.ALLOWED_HOSTS = restrictions
        if parser.has_option('hosts', 'forbidden'):
            list_command_allowed = parser.get('hosts', 'forbidden')
            list_command_allowed = list_command_allowed.split('] ')
            i = 0
            for _ in list_command_allowed:
                list_command_allowed[i] = list_command_allowed[i].strip('[]')
                i = i + 1
            restrictions = dict()
            for command_unparsed in list_command_allowed:
                command_allowed = command_unparsed.split(' ')
                if ',' in command_allowed[0]:
                    for command in command_allowed[0].split(','):
                        if ',' in command_allowed[1]:
                            restrictions[command] = command_allowed[1].split(',')
                        else:
                            restrictions[command] = [command_allowed[1]]
                else:
                    if ',' in command_allowed[1]:
                        restrictions[command_allowed[0]] = command_allowed[1].split(',')
                    else:
                        restrictions[command_allowed[0]] = [command_allowed[1]]
            BasicConfig.DENIED_HOSTS = restrictions
        if parser.has_option('structures', 'path'):
            BasicConfig.STRUCTURES_DIR = parser.get('structures', 'path')
        if parser.has_option('globallogs', 'path'):
            BasicConfig.GLOBAL_LOG_DIR = parser.get('globallogs', 'path')
        if parser.has_option('defaultstats', 'path'):
            BasicConfig.DEFAULT_OUTPUT_DIR = parser.get('defaultstats', 'path')
        if parser.has_option('historicdb', 'path'):
            BasicConfig.JOBDATA_DIR = parser.get('historicdb', 'path')
        if parser.has_option('historiclog', 'path'):
            BasicConfig.HISTORICAL_LOG_DIR = parser.get('historiclog', 'path')
        if parser.has_option('autosubmitapi', 'url'):
            BasicConfig.AUTOSUBMIT_API_URL = parser.get(
                'autosubmitapi', 'url')
        if parser.has_option('database', 'backend'):
            BasicConfig.DATABASE_BACKEND = parser.get('database', 'backend')
        if parser.has_option('database', 'connection_url'):
            BasicConfig.DATABASE_CONN_URL = parser.get('database', 'connection_url')
        if parser.has_option('config', 'log_recovery_timeout'):
            BasicConfig.LOG_RECOVERY_TIMEOUT = int(parser.get('config', 'log_recovery_timeout'))

    @staticmethod
    def read():
        """
        Reads configuration from .autosubmitrc files, first from /etc., then for user
        directory and last for current path.
        """
        filename = 'autosubmitrc'
        if 'AUTOSUBMIT_CONFIGURATION' in os.environ and os.path.exists(os.environ['AUTOSUBMIT_CONFIGURATION']):
            config_file_path = os.environ['AUTOSUBMIT_CONFIGURATION']
            # Call read_file_config with the value of the environment variable
            BasicConfig.__read_file_config(config_file_path)
        else:
            if os.path.exists(os.path.join('', '.' + filename)):
                BasicConfig.__read_file_config(os.path.join('', '.' + filename))
            elif os.path.exists(os.path.join(os.path.expanduser('~'), '.' + filename)):
                BasicConfig.__read_file_config(os.path.join(
                    os.path.expanduser('~'), '.' + filename))
            else:
                BasicConfig.__read_file_config(os.path.join('/etc', filename))

            # Check if the environment variable is defined

        BasicConfig._update_config()
        return
