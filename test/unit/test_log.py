from unittest import TestCase
from log.log import AutosubmitError, AutosubmitCritical, Log

"""Tests for the log module."""

class TestLog(TestCase):

    def setUp(self):
        ...

    def test_autosubmit_error(self):
        ae = AutosubmitError()
        assert 'Unhandled Error' == ae.message
        assert 6000 == ae.code
        assert None is ae.trace
        assert 'Unhandled Error' == ae.error_message
        assert ' ' == str(ae)

    def test_autosubmit_error_error_message(self):
        ae = AutosubmitError(trace='ERROR!')
        assert 'ERROR! Unhandled Error' == ae.error_message

    def test_autosubmit_critical(self):
        ac = AutosubmitCritical()
        assert 'Unhandled Error' == ac.message
        assert 7000 == ac.code
        assert None is ac.trace
        assert ' ' == str(ac)

    def test_log_not_format(self):
        """
        Smoke test if the log messages are sent correctly
        when having a formattable message that it is not
        intended to be formatted
        """

        def _send_messages(msg: str):
            Log.debug(msg)
            Log.info(msg)
            Log.result(msg)
            Log.warning(msg)
            Log.error(msg)
            Log.critical(msg)
            Log.status(msg)
            Log.status_failed(msg)

        # Standard messages
        msg = "Test"
        _send_messages(msg)

        # Format messages
        msg = "Test {foo, bar}"
        _send_messages(msg)
