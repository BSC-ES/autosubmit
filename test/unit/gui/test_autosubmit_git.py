from unittest import TestCase

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

from autosubmit.git.autosubmit_git import AutosubmitGit
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from log.log import AutosubmitCritical


class TestAutosubmitGit(TestCase):
    """Tests for ``AutosubmitGit``."""
    MockBasicConfig = MagicMock()
    mock_subprocess = MagicMock()

    def setUp(self) -> None:
        self.expid = 'a000'
        self.temp_dir = TemporaryDirectory()
        self.exp_dir = Path(self.temp_dir.name, f'{self.expid}')
        self.conf_dir = self.exp_dir / 'conf'
        self.conf_dir.mkdir(parents=True)
        self.MockBasicConfig.LOCAL_ROOT_DIR = self.temp_dir.name
        self.MockBasicConfig.LOCAL_PROJ_DIR = self.exp_dir / 'proj'
        self.MockBasicConfig.LOCAL_PROJ_DIR.mkdir(parents=True)

        self.hpcarch = MagicMock()

        def mocked_git_subprocess(*args, **kwargs):
            if args[0] == 'git --version':
                return "2251"

        self.mock_subprocess.check_output.side_effect = mocked_git_subprocess

    def tearDown(self) -> None:
        if self.temp_dir is not None:
            self.temp_dir.cleanup()

    def test_submodules_fails_with_invalid_as_conf(self):
        as_conf = MagicMock()
        as_conf.is_valid_git_repository.return_value = False
        with self.assertRaises(AutosubmitCritical):
            AutosubmitGit.clone_repository(as_conf=as_conf, force=True, hpcarch=self.hpcarch)

    @patch('autosubmit.git.autosubmit_git.BasicConfig', new=MockBasicConfig)
    @patch('autosubmitconfigparser.config.basicconfig.BasicConfig', new=MockBasicConfig)
    @patch('autosubmitconfigparser.config.configcommon.BasicConfig', new=MockBasicConfig)
    @patch('autosubmit.git.autosubmit_git.subprocess', new=mock_subprocess)
    def test_submodules_empty_string(self):
        """Verifies that submodules configuration is processed correctly with empty strings."""
        _as_conf = AutosubmitConfig(expid=self.expid, basic_config=self.MockBasicConfig)
        _as_conf.experiment_data = {
            'GIT': {
                'PROJECT_ORIGIN': 'https://earth.bsc.es/gitlab/es/autosubmit.git',
                'PROJECT_BRANCH': 'master',
                'PROJECT_COMMIT': '123',
                'REMOTE_CLONE_ROOT': 'workflow',
                'PROJECT_SUBMODULES': ''
            }
        }
        as_conf = MagicMock(wraps=_as_conf)
        as_conf.is_valid_git_repository.return_value = True
        as_conf.expid = self.expid

        force = False
        AutosubmitGit.clone_repository(as_conf=as_conf, force=force, hpcarch=self.hpcarch)

        # Should be the last command, but to make sure we iterate all the commands.
        # A good improvement would have to break the function called into smaller
        # parts, like ``get_git_version``, ``clone_submodules(recursive=True)``, etc.
        # as that would be a lot easier to test.
        recursive_in_any_call = any([call for call in self.hpcarch.method_calls if
                                     'git submodule update --init --recursive' in str(call)])

        assert recursive_in_any_call

    @patch('autosubmit.git.autosubmit_git.BasicConfig', new=MockBasicConfig)
    @patch('autosubmitconfigparser.config.basicconfig.BasicConfig', new=MockBasicConfig)
    @patch('autosubmitconfigparser.config.configcommon.BasicConfig', new=MockBasicConfig)
    @patch('autosubmit.git.autosubmit_git.subprocess', new=mock_subprocess)
    def test_submodules_list_not_empty(self):
        """Verifies that submodules configuration is processed correctly with a list of strings."""
        _as_conf = AutosubmitConfig(expid=self.expid, basic_config=self.MockBasicConfig)
        _as_conf.experiment_data = {
            'GIT': {
                'PROJECT_ORIGIN': 'https://earth.bsc.es/gitlab/es/autosubmit.git',
                'PROJECT_BRANCH': '',
                'PROJECT_COMMIT': '123',
                'REMOTE_CLONE_ROOT': 'workflow',
                'PROJECT_SUBMODULES': 'clone_me_a clone_me_b'
            }
        }
        as_conf = MagicMock(wraps=_as_conf)
        as_conf.is_valid_git_repository.return_value = True
        as_conf.expid = self.expid

        force = False
        AutosubmitGit.clone_repository(as_conf=as_conf, force=force, hpcarch=self.hpcarch)

        # Here the call happens in the hpcarch, not in subprocess
        clone_me_a_in_any_call = any([call for call in self.hpcarch.method_calls if
                                      'clone_me_a' in str(call)])

        assert clone_me_a_in_any_call

    @patch('autosubmit.git.autosubmit_git.BasicConfig', new=MockBasicConfig)
    @patch('autosubmitconfigparser.config.basicconfig.BasicConfig', new=MockBasicConfig)
    @patch('autosubmitconfigparser.config.configcommon.BasicConfig', new=MockBasicConfig)
    @patch('autosubmit.git.autosubmit_git.subprocess', new=mock_subprocess)
    def test_submodules_falsey_disables_submodules(self):
        """Verifies that submodules are not used when users pass a Falsey bool value."""
        _as_conf = AutosubmitConfig(expid=self.expid, basic_config=self.MockBasicConfig)
        _as_conf.experiment_data = {
            'GIT': {
                'PROJECT_ORIGIN': 'https://earth.bsc.es/gitlab/es/autosubmit.git',
                'PROJECT_BRANCH': '',
                'PROJECT_COMMIT': '123',
                'REMOTE_CLONE_ROOT': 'workflow',
                'PROJECT_SUBMODULES': False
            }
        }
        as_conf = MagicMock(wraps=_as_conf)
        as_conf.is_valid_git_repository.return_value = True
        as_conf.expid = self.expid

        force = False
        AutosubmitGit.clone_repository(as_conf=as_conf, force=force, hpcarch=self.hpcarch)

        # Because we have ``PROJECT_SUBMODULES: False``, there must be no calls
        # to git submodules.
        any_one_used_submodules = any([call for call in self.hpcarch.method_calls if
                                       'submodules' in str(call)])

        assert not any_one_used_submodules
