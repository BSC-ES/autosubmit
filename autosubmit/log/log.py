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

import logging
import os
import sys
from datetime import datetime
from time import sleep
from typing import Any, Union


class AutosubmitError(Exception):
    """Exception raised for Autosubmit errors.

    Attributes:
        message (str): explanation of the error
        code (int): classified code
        trace (str): extra information about the error
    """

    def __init__(self, message="Unhandled Error", code=6000, trace: Union[None, str] = None):
        self.code = code
        self.message = message
        self.trace = trace

    @property
    def error_message(self) -> str:
        """
        Return the error message ready to be logged, with both trace
        (when present) and the message separated by a space. Or just
        the message if no trace is available.

        :return: ``trace`` and ``message`` separated by a space, or just the
                 ``message`` if no ``trace`` is available.
        :rtype: str
        """
        return self.message if not self.trace else f'{self.trace} {self.message}'

    def __str__(self):
        return " "


class AutosubmitCritical(Exception):
    """Exception raised for Autosubmit critical errors.

    Attributes:
        code -- Classified code
        message -- Explanation of the error
        trace -- Extra information about the error
    """

    def __init__(self, message="Unhandled Error", code=7000, trace=None):
        self.code = code
        self.message = message
        self.trace = trace

    def __str__(self) -> str:
        return " "


class LogFormatter:
    """
    Class to format log output.

    :param to_file: If True, creates a LogFormatter for files; if False, for console
    :type to_file: bool
    """
    __module__ = __name__
    yellow = "\x1b[33;20m"
    RESULT = '\x1b[32m'
    CRITICAL = '\x1b[1m \x1b[31m'
    DEFAULT = '\x1b[0m\x1b[39m'
    ERROR = '\033[38;5;214m'
    WARNING = "\x1b[33;20m"

    def __init__(self, to_file=False):
        """
        Initializer for LogFormatter

        :param to_file: Whether to write it to a file or not.
        """
        self._file = to_file
        if self._file:
            self._formatter = logging.Formatter('%(asctime)s %(message)s')
        else:
            self._formatter = logging.Formatter('%(message)s')

    def format(self, record: logging.LogRecord) -> str:
        """Format log output, adding labels if needed for log level.

        If logging to console, also manages font color.

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
        elif record.levelno == Log.WARNING:
            if not self._file:
                header = LogFormatter.WARNING
            header += '[WARNING] '
        elif record.levelno == Log.ERROR:
            if not self._file:
                header = LogFormatter.ERROR
            header += '[ERROR] '
        elif record.levelno == Log.CRITICAL:
            if not self._file:
                header = LogFormatter.CRITICAL
            header += '[CRITICAL] '
        msg = self._formatter.format(record)
        if header != '' and not self._file:
            msg += LogFormatter.DEFAULT
        return header + msg


class StatusFilter(logging.Filter):

    def filter(self, rec) -> bool:
        return rec.levelno == Log.STATUS


class StatusFailedFilter(logging.Filter):
    def filter(self, rec) -> bool:
        return rec.levelno == Log.STATUS_FAILED


class Log:
    """Static class to manage the log for the application.

    Messages will be sent to console and to file if it is configured.
    Levels can be set for each output independently. These levels are
    (from lower to higher priority):
    """

    date = '{0:%Y%m%d_%H%M%S}_'.format(datetime.now())
    file_path = ""
    __module__ = __name__
    EVERYTHING = 0
    STATUS_FAILED = 500
    STATUS = 1000
    DEBUG = 2000
    WARNING = 3000
    INFO = 4000
    RESULT = 5000
    ERROR = 6000
    CRITICAL = 7000
    NO_LOG = CRITICAL + 1000
    logging.basicConfig()
    log_dict_debug = logging.Logger.manager.loggerDict
    if 'Autosubmit' in list(logging.Logger.manager.loggerDict.keys()):
        log = logging.getLogger('Autosubmit')
    else:
        log = logging.Logger('Autosubmit', EVERYTHING)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(INFO)
    console_handler.setFormatter(LogFormatter(False))  # type: ignore
    log.addHandler(console_handler)

    def __init__(self):
        pass

    def init_variables(self, file_path="") -> None:
        self.file_path = file_path

    @staticmethod
    def shutdown_logger() -> None:
        """
        Shutdown logger module to prevent race issues on delete
        """
        logging.shutdown()

    @staticmethod
    def get_logger(name="Autosubmit") -> None:
        """
        Configure the file to store the log. If another file was specified earlier, new messages will only go to the
        new file.

        :param name: name of the logger
        :type name: str
        """
        logging.getLogger(name)

    @staticmethod
    def set_file(file_path: str, type='out', level="WARNING") -> None:
        """Configure the file to store the log.

        If another file was specified earlier, new messages will only go to the new file.

        :param file_path: file to store the log
        :type file_path: str
        :param type: file type
        :param level: log level
        """
        levels = {
            "STATUS_FAILED": 500,
            "STATUS": 1000,
            "DEBUG": 2000,
            "WARNING": 3000,
            "INFO": 4000,
            "RESULT": 5000,
            "ERROR": 6000,
            "CRITICAL": 7000
        }
        levels["NO_LOG"] = levels["CRITICAL"] + 1000

        level = levels.get(str(level).upper(), "DEBUG")

        max_retries = 3
        retries = 1
        timeout = 5

        while not os.path.exists(file_path) and retries < max_retries:
            try:
                directory, filename = os.path.split(file_path)
                # FIXME: This appears to be wrong. If the directory does not exist, we create it here;
                #        however, below we create a ``FileHandler``, but that fails if the ``file_path``
                #        does not exist -- which is the case when this directory is empty!
                if not os.path.exists(directory):
                    os.mkdir(directory)
                files = [f for f in os.listdir(directory) if os.path.isfile(
                    os.path.join(directory, f)) and f.endswith(filename)]
                if len(files) >= 10:
                    files.sort()
                    os.remove(os.path.join(directory, files[0]))
                file_path = os.path.join(
                    directory, Log.date + filename)
                if type == 'out':
                    file_handler = logging.FileHandler(file_path, 'w')
                    file_handler.setLevel(level)
                    file_handler.setFormatter(LogFormatter(True))
                    Log.log.addHandler(file_handler)
                elif type == 'err':
                    err_file_handler = logging.FileHandler(file_path, 'w')
                    err_file_handler.setLevel(Log.ERROR)
                    err_file_handler.setFormatter(LogFormatter(True))
                    Log.log.addHandler(err_file_handler)
                elif type == 'status':
                    custom_filter = StatusFilter()
                    file_path = os.path.join(directory, filename)
                    status_file_handler = logging.FileHandler(file_path, 'w')
                    status_file_handler.setLevel(Log.STATUS)
                    status_file_handler.setFormatter(LogFormatter(False))
                    status_file_handler.addFilter(custom_filter)
                    Log.log.addHandler(status_file_handler)
                elif type == 'status_failed':
                    custom_filter = StatusFailedFilter()
                    file_path = os.path.join(directory, filename)
                    status_file_handler = logging.FileHandler(file_path, 'w')
                    status_file_handler.setLevel(Log.STATUS_FAILED)
                    status_file_handler.setFormatter(LogFormatter(False))
                    status_file_handler.addFilter(custom_filter)
                    Log.log.addHandler(status_file_handler)
                os.chmod(file_path, 509)
            except Exception as e:  # retry again
                sleep(timeout * retries)

    @staticmethod
    def reset_status_file(file_path: str, type='status', level=WARNING) -> None:
        """
        Configure the file to store the log. If another file was specified earlier, new messages will only go to the
        new file.

        :param file_path: file to store the log
        :type file_path: str
        :param type: file type
        :param level: log level
        """
        try:
            # test = Log.log.handlers
            if type == 'status':
                while len(Log.log.handlers) > 3:
                    Log.log.handlers.pop()
                custom_filter = StatusFilter()
                status_file_handler = logging.FileHandler(file_path, 'w')
                status_file_handler.setLevel(Log.STATUS)
                status_file_handler.setFormatter(LogFormatter(False))
                status_file_handler.addFilter(custom_filter)
                Log.log.addHandler(status_file_handler)
            elif type == 'status_failed':
                custom_filter = StatusFailedFilter()
                status_file_handler = logging.FileHandler(file_path, 'w')
                status_file_handler.setLevel(Log.STATUS_FAILED)
                status_file_handler.setFormatter(LogFormatter(False))
                status_file_handler.addFilter(custom_filter)
                Log.log.addHandler(status_file_handler)
        except Exception:  # retry again
            pass

    @staticmethod
    def set_console_level(level: Union[int, str]) -> None:
        """Sets log level for logging to console.

        Every output of level equal or higher to parameter level will be printed on console

        :param level: new level for console
        :return: None
        """
        if type(level) is str:
            level = getattr(Log, level)
        Log.console_handler.level = level

    @staticmethod
    def _verify_args_message(msg: str, *args):
        """
        Verify if the message has arguments to format

        :param msg: message to show
        :param args: arguments for message formatting
        :return: message formatted
        """
        if args:
            msg = msg.format(*args)
        return msg

    @staticmethod
    def debug(msg: str, *args: Any) -> None:
        """Sends debug information to the log.

        :param msg: message to show
        :param args: arguments for message formating (it will be done using format() method on str)
        """
        msg = Log._verify_args_message(msg, *args)
        Log.log.log(Log.DEBUG, msg)

    @staticmethod
    def info(msg: str, *args: Any) -> None:
        """Sends information to the log.

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        msg = Log._verify_args_message(msg, *args)
        Log.log.log(Log.INFO, msg)

    @staticmethod
    def result(msg: str, *args: Any) -> None:
        """Sends results information to the log. It will be shown in green in the console.

        :param msg: message to show
        :param args: arguments for message formating (it will be done using format() method on str)
        """
        msg = Log._verify_args_message(msg, *args)
        Log.log.log(Log.RESULT, msg)

    @staticmethod
    def warning(msg: str, *args: Any) -> None:
        """Sends program warnings to the log. It will be shown in yellow in the console.

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        msg = Log._verify_args_message(msg, *args)
        Log.log.log(Log.WARNING, msg)

    @staticmethod
    def error(msg: str, *args: Any) -> None:
        """Sends errors to the log. It will be shown in red in the console.

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        msg = Log._verify_args_message(msg, *args)
        Log.log.log(Log.ERROR, msg)

    @staticmethod
    def critical(msg: str, *args: Any) -> None:
        """Sends critical errors to the log. It will be shown in red in the console.

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        msg = Log._verify_args_message(msg, *args)
        Log.log.log(Log.CRITICAL, msg)

    @staticmethod
    def status(msg: str, *args: Any) -> None:
        """Sends status of jobs to the log. It will be shown in white in the console.

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        msg = Log._verify_args_message(msg, *args)
        Log.log.log(Log.STATUS, msg)

    @staticmethod
    def status_failed(msg: str, *args: Any):
        """Sends failed status of jobs to the log. It will be shown in white in the console.

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        msg = Log._verify_args_message(msg, *args)
        Log.log.log(Log.STATUS_FAILED, msg)

    @staticmethod
    def printlog(message="Generic message", code=4000) -> None:
        """Log management for Autosubmit messages.

        Attributes:
            code -- Classified code
            message -- explanation
        """
        if 4000 <= code < 5000:
            Log.info("{0}", message)
        elif 5000 <= code < 6000:
            Log.result("{0}", message)
        elif 3000 <= code < 4000:
            Log.warning("{1}[eCode={0}]", code, message)
        elif 6000 <= code < 7000:
            Log.error("{1}[eCode={0}]", code, message)
        elif code <= 7000:
            Log.critical("{1}[eCode={0}]", code, message)
        else:
            Log.info("{0}", message)
