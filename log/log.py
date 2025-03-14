import logging
import os
import sys
from time import sleep
from datetime import datetime
from typing import Union


class AutosubmitError(Exception):
    """Exception raised for Autosubmit errors.

    Attributes:
        message (str): explanation of the error
        code (int): classified code
        trace (str): extra information about the error
    """

    def __init__(self, message="Unhandled Error", code=6000, trace: Union[None, str]=None):
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
    """Exception raised for Autosubmit critical errors .
    Attributes:
        errorcode -- Classified code
        message -- explanation of the error
    """

    def __init__(self, message="Unhandled Error", code=7000, trace=None):
        self.code = code
        self.message = message
        self.trace = trace

    def __str__(self):
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
    ERROR = '\x1b[31m'
    CRITICAL = '\x1b[1m \x1b[31m'
    DEFAULT = '\x1b[0m\x1b[39m'
    ERROR = '\033[38;5;214m'
    WARNING = "\x1b[33;20m"

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

    def filter(self, rec):
        return rec.levelno == Log.STATUS

class StatusFailedFilter(logging.Filter):
    def filter(self, rec):
        return rec.levelno == Log.STATUS_FAILED

class Log:
    """
    Static class to manage the log for the application. Messages will be sent to console and to file if it is
    configured. Levels can be set for each output independently. These levels are (from lower to higher priority):
    """

    date = ('{0:%Y%m%d_%H%M%S}_').format(datetime.now())

    def __init__(self):
        pass

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
    console_handler.setFormatter(LogFormatter(False))
    log.addHandler(console_handler)
    def init_variables(self,file_path=""):
        self.file_path = file_path

    @staticmethod
    def shutdown_logger():
        """
        Shutdown logger module to prevent race issues on delete
        """
        logging.shutdown()

    @staticmethod
    def get_logger(name="Autosubmit"):
        """
        Configure the file to store the log. If another file was specified earlier, new messages will only go to the
        new file.

        :param file_path: file to store the log
        :type file_path: str
        """
        logging.getLogger(name)

    @staticmethod
    def set_file(file_path, type='out', level="WARNING"):
        """
        Configure the file to store the log. If another file was specified earlier, new messages will only go to the
        new file.

        :param file_path: file to store the log
        :type file_path: str
        """
        levels = {}
        levels["STATUS_FAILED"] = 500
        levels["STATUS"] = 1000
        levels["DEBUG"] = 2000
        levels["WARNING"] = 3000
        levels["INFO"] = 4000
        levels["RESULT"] = 5000
        levels["ERROR"] = 6000
        levels["CRITICAL"] = 7000
        levels["NO_LOG"] = levels["CRITICAL"] + 1000

        level = levels.get(str(level).upper(),"DEBUG")

        max_retries = 3
        retries = 1
        timeout = 5

        while not os.path.exists(file_path) and retries < max_retries:
            try:
                directory, filename = os.path.split(file_path)
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
            except Exception: # retry again
                sleep(timeout * retries)

    @staticmethod
    def reset_status_file(file_path,type='status', level=WARNING):
        """
        Configure the file to store the log. If another file was specified earlier, new messages will only go to the
        new file.

        :param file_path: file to store the log
        :type file_path: str
        """
        try:
            #test = Log.log.handlers
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
    def set_console_level(level):
        """
        Sets log level for logging to console. Every output of level equal or higher to parameter level will be
        printed on console

        :param level: new level for console
        :return: None
        """
        if type(level) is str:
            level = getattr(Log, level)
        Log.console_handler.level = level

    @staticmethod
    def set_error_level(level):
        """
        Sets log level for logging to console. Every output of level equal or higher to parameter level will be
        printed on console

        :param level: new level for console
        :return: None
        """
        if type(level) is str:
            level = getattr(Log, level)
        Log.error.level = level

    @staticmethod
    def debug(msg, *args):
        """
        Sends debug information to the log

        :param msg: message to show
        :param args: arguments for message formating (it will be done using format() method on str)
        """
        Log.log.log(Log.DEBUG, msg.format(*args))

    @staticmethod
    def info(msg, *args):
        """
        Sends information to the log

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        Log.log.log(Log.INFO, msg.format(*args))

    @staticmethod
    def result(msg, *args):
        """
        Sends results information to the log. It will be shown in green in the console.

        :param msg: message to show
        :param args: arguments for message formating (it will be done using format() method on str)
        """
        Log.log.log(Log.RESULT, msg.format(*args))

    @staticmethod
    def warning(msg, *args):
        """
        Sends program warnings to the log. It will be shown in yellow in the console.

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        Log.log.log(Log.WARNING, msg.format(*args))

    @staticmethod
    def error(msg, *args):
        """
        Sends errors to the log. It will be shown in red in the console.

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        Log.log.log(Log.ERROR, msg.format(*args))

    @staticmethod
    def critical(msg, *args):
        """
        Sends critical errors to the log. It will be shown in red in the console.

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        Log.log.log(Log.CRITICAL, msg.format(*args))

    @staticmethod
    def status(msg, *args):
        """
        Sends status of jobs to the log. It will be shown in white in the console.

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        Log.log.log(Log.STATUS, msg.format(*args))

    @staticmethod
    def status_failed(msg, *args):
        """
        Sends failed status of jobs to the log. It will be shown in white in the console.

        :param msg: message to show
        :param args: arguments for message formatting (it will be done using format() method on str)
        """
        Log.log.log(Log.STATUS_FAILED, msg.format(*args))

    @staticmethod
    def printlog(message="Generic message", code=4000):
        """Log management for Autosubmit messages .
        Attributes:
            errorcode -- Classified code
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
