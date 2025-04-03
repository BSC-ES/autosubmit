#!/usr/bin/env python3

# Copyright 2016 Earth Sciences Department, BSC-CNS

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

import logging
import os
import sys
from datetime import datetime


class LogFormatter:
    """
    Class to format log output.

    :param to_file: If True, creates a LogFormatter for files; if False, for console
    :type to_file: bool
    """
    RESULT = '\033[32m'
    WARNING = '\033[33m'
    ERROR = '\033[31m'
    CRITICAL = '\033[1m \033[31m'
    DEFAULT = '\033[0m\033[39m'

    def __init__(self, to_file=False):
        """
        Initializer for LogFormatter


        """
        self._file = to_file
        if self._file:
            self._formatter = logging.Formatter('%(asctime)s %(message)s')
        else:
            self._formatter = logging.Formatter('%(message)s')

    def format(self, record):
        """
        Format log output, adding labels if needed for log level. If logging to console, also manages font color.
        If logging to file adds timestamp

        :param record: log record to format
        :type record: LogRecord
        :return: formatted record
        :rtype: str
        """
        header = ''
        if record.levelno == Log.RESULT:
            if not self._file:
                header = LogFormatter.RESULT
        elif record.levelno == Log.warning:
            if not self._file:
                header = LogFormatter.WARNING
        elif record.levelno == Log.WARNING:
            if not self._file:
                header = LogFormatter.WARNING
            header += "[WARNING] "
        elif record.levelno == Log.ERROR:
            if not self._file:
                header = LogFormatter.ERROR
            header += "[ERROR] "
        elif record.levelno == Log.CRITICAL:
            if not self._file:
                header = LogFormatter.ERROR
            header += "[CRITICAL] "

        msg = self._formatter.format(record)
        if header != '' and not self._file:
            msg += LogFormatter.DEFAULT
        return header + msg


class Log:
    """
    Static class to manage the prints for the regression tests. Messages will be sent to console.
    Levels can be set for each output independently. These levels are (from lower to higher priority):
        - DEBUG
        - INFO
        - RESULT
        - USER_WARNING
        - WARNING
        - ERROR
        - CRITICAL
    """
    EVERYTHING = 0
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    RESULT = 25
    USER_WARNING = 29
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL
    NO_LOG = CRITICAL + 1

    logging.basicConfig()

    log = logging.Logger('Autosubmit', EVERYTHING)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(INFO)
    console_handler.setFormatter(LogFormatter(False))
    log.addHandler(console_handler)

    file_handler = None
    file_level = INFO

    @staticmethod
    def debug(msg, *args):
        """
        Prints debug information

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        print((msg.format(*args)))

    @staticmethod
    def info(msg, *args):
        """
        Prints information

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        print((msg.format(*args)))

    @staticmethod
    def result(msg, *args):
        """
        Prints results information. It will be shown in green in the console.

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        print((LogFormatter.RESULT + msg.format(*args) + LogFormatter.DEFAULT))

    @staticmethod
    def user_warning(msg, *args):
        """
        Prints warnings for the user. It will be shown in yellow in the console.

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        print((LogFormatter.WARNING + msg.format(*args) + LogFormatter.DEFAULT))

    @staticmethod
    def warning(msg, *args):
        """
        Prints program warnings. It will be shown in yellow in the console.

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        print((LogFormatter.WARNING + "[WARNING] " + msg.format(*args) + LogFormatter.DEFAULT))

    @staticmethod
    def error(msg, *args):
        """
        Prints errors to the log. It will be shown in red in the console.

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        print((LogFormatter.ERROR + "[ERROR] " + msg.format(*args) + LogFormatter.DEFAULT))

    @staticmethod
    def critical(msg, *args):
        """
        Prints critical errors to the log. It will be shown in red in the console.

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        print((LogFormatter.ERROR + "[CRITICAL] " + msg.format(*args) + LogFormatter.DEFAULT))
