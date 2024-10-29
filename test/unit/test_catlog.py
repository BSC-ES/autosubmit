from unittest import TestCase

import io
import sys
from contextlib import suppress, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import pytest

from autosubmit.autosubmit import Autosubmit, AutosubmitCritical
from autosubmitconfigparser.config.basicconfig import BasicConfig

class TestJob(TestCase):

    def setUp(self):
        self.original_root_dir = BasicConfig.LOCAL_ROOT_DIR
        self.root_dir = TemporaryDirectory()
        BasicConfig.LOCAL_ROOT_DIR = self.root_dir.name
        
        self.exp_path = BasicConfig.expid_dir('a000') 
        self.tmp_dir = BasicConfig.expid_tmp_dir('a000')
        self.log_dir = BasicConfig.expid_log_dir('a000')
        self.aslogs_dir = BasicConfig.expid_aslog_dir('a000') 

        self.autosubmit = Autosubmit()
        # directories used when searching for logs to cat
        
        self.status_path = self.exp_path / 'status' 
        if not self.aslogs_dir.exists():
            self.aslogs_dir.mkdir(parents = True, exist_ok = True)
        if not self.status_path.exists():
            self.status_path.mkdir(parents = True, exist_ok = True)
        if not self.log_dir.exists():
            self.log_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        BasicConfig.LOCAL_ROOT_DIR = self.original_root_dir
        if self.root_dir is not None:
            self.root_dir.cleanup()

    def test_invalid_file(self):
        def _fn():
            self.autosubmit.cat_log(None, '8', None)  # type: ignore
        self.assertRaises(AutosubmitCritical, _fn)

    def test_invalid_mode(self):
        def _fn():
            self.autosubmit.cat_log(None, 'o', '8')  # type: ignore
        self.assertRaises(AutosubmitCritical, _fn)

    # -- workflow

    def test_is_workflow_invalid_file(self):
        def _fn():
            self.autosubmit.cat_log('a000', 'j', None)
        self.assertRaises(AutosubmitCritical, _fn)

    @patch('autosubmit.autosubmit.Log')
    def test_is_workflow_not_found(self, Log):
        self.autosubmit.cat_log('a000', 'o', 'c')
        assert Log.info.called
        assert Log.info.call_args[0][0] == 'No logs found.'

    def test_is_workflow_log_is_dir(self):
        log_file_actually_dir = self.aslogs_dir / 'log_run.log'
        log_file_actually_dir.mkdir(parents=True)
        def _fn():
            self.autosubmit.cat_log('a000', 'o', 'c')
        self.assertRaises(AutosubmitCritical, _fn)

    @patch('subprocess.Popen')
    def test_is_workflow_out_cat(self, popen):
        log_file = self.aslogs_dir / 'log_run.log'
        if log_file.is_dir(): # dir is created in previous test 
            log_file.rmdir()
        with open(log_file, 'w') as f:
            f.write('as test')
            f.flush()
            self.autosubmit.cat_log('a000', file=None, mode='c')
            assert popen.called
            args = popen.call_args[0][0]
            assert args[0] == 'cat'
            assert args[1] == str(log_file)

    @patch('subprocess.Popen')
    def test_is_workflow_status_tail(self, popen):
        log_file = self.status_path / 'a000_anything.txt'
        with open(log_file, 'w') as f:
            f.write('as test')
            f.flush()
            self.autosubmit.cat_log('a000', file='s', mode='t')
            assert popen.called
            args = popen.call_args[0][0]
            assert args[0] == 'tail'
            assert str(args[-1]) == str(log_file)

    # --- jobs

    @patch('autosubmit.autosubmit.Log')
    def test_is_jobs_not_found(self, Log):
        for file in ['j', 's', 'o']:
            self.autosubmit.cat_log('a000_INI', file=file, mode='c')
            assert Log.info.called
            assert Log.info.call_args[0][0] == 'No logs found.'
    
    def test_is_jobs_log_is_dir(self):
        log_file_actually_dir = self.log_dir / 'a000_INI.20000101.out'
        log_file_actually_dir.mkdir(parents=True)
        def _fn():
            self.autosubmit.cat_log('a000_INI', 'o', 'c')
        self.assertRaises(AutosubmitCritical, _fn)

    @patch('subprocess.Popen')
    def test_is_jobs_out_tail(self, popen):
        log_file = self.log_dir / 'a000_INI.20200101.out'
        if log_file.is_dir(): # dir is created in previous test 
            log_file.rmdir()
        with open(log_file, 'w') as f:
            f.write('as test')
            f.flush()
            self.autosubmit.cat_log('a000_INI', file=None, mode='t')
            assert popen.called
            args = popen.call_args[0][0]
            assert args[0] == 'tail'
            assert str(args[-1]) == str(log_file)

    # --- command-line

    def test_command_line_help(self):
        args = ['autosubmit', 'cat-log', '--help']
        with patch.object(sys, 'argv', args) as _, io.StringIO() as buf, redirect_stdout(buf):
            assert Autosubmit.parse_args()
            assert buf
            assert 'View workflow and job logs.' in buf.getvalue()
