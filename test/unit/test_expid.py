import tempfile
import os
from unittest import TestCase
from mock import Mock, patch
from autosubmit.autosubmit import Autosubmit
from autosubmit.experiment.experiment_common import new_experiment, copy_experiment
from textwrap import dedent
from pathlib import Path
from autosubmitconfigparser.config.basicconfig import BasicConfig
from itertools import permutations, product


class TestExpid(TestCase):
    def setUp(self):
        self.description = "for testing"
        self.version = "test-version"

    @patch('autosubmit.experiment.experiment_common.db_common')
    def test_create_new_experiment(self, db_common_mock):
        current_experiment_id = "empty"
        self._build_db_mock(current_experiment_id, db_common_mock)
        experiment_id = new_experiment(self.description, self.version)
        self.assertEqual("a000", experiment_id)

    @patch('autosubmit.experiment.experiment_common.db_common')
    def test_create_new_test_experiment(self, db_common_mock):
        current_experiment_id = "empty"
        self._build_db_mock(current_experiment_id, db_common_mock)
        experiment_id = new_experiment(self.description, self.version, True)
        self.assertEqual("t000", experiment_id)

    @patch('autosubmit.experiment.experiment_common.db_common')
    def test_create_new_operational_experiment(self, db_common_mock):
        current_experiment_id = "empty"
        self._build_db_mock(current_experiment_id, db_common_mock)
        experiment_id = new_experiment(self.description, self.version, False, True)
        self.assertEqual("o000", experiment_id)

    @patch('autosubmit.experiment.experiment_common.db_common')
    def test_create_new_evaluation_experiment(self, db_common_mock):
        current_experiment_id = "empty"
        self._build_db_mock(current_experiment_id, db_common_mock)
        experiment_id = new_experiment(self.description, self.version, False, False, True)
        self.assertEqual("e000", experiment_id)

    @patch('autosubmit.experiment.experiment_common.db_common')
    def test_create_new_experiment_with_previous_one(self, db_common_mock):
        current_experiment_id = "a007"
        self._build_db_mock(current_experiment_id, db_common_mock)
        experiment_id = new_experiment(self.description, self.version)
        self.assertEqual("a007", experiment_id)

    @patch('autosubmit.experiment.experiment_common.db_common')
    def test_create_new_test_experiment_with_previous_one(self, db_common_mock):
        current_experiment_id = "t0ac"
        self._build_db_mock(current_experiment_id, db_common_mock)
        experiment_id = new_experiment(self.description, self.version, True)
        self.assertEqual("t0ac", experiment_id)

    @patch('autosubmit.experiment.experiment_common.db_common')
    def test_create_new_operational_experiment_with_previous_one(self, db_common_mock):
        current_experiment_id = "o113"
        self._build_db_mock(current_experiment_id, db_common_mock)
        experiment_id = new_experiment(self.description, self.version, False, True)
        self.assertEqual("o113", experiment_id)

    @patch('autosubmit.experiment.experiment_common.db_common')
    def test_create_new_evaluation_experiment_with_previous_one(self, db_common_mock):
        current_experiment_id = "e113"
        self._build_db_mock(current_experiment_id, db_common_mock)
        experiment_id = new_experiment(self.description, self.version, False, False, True)
        self.assertEqual("e113", experiment_id)

    @patch('autosubmit.experiment.experiment_common.db_common')
    def test_copy_experiment_new(self, db_common_mock):
        current_experiment_id = "empty"
        self._build_db_mock(current_experiment_id, db_common_mock)
        experiment_id = copy_experiment(current_experiment_id, self.description, self.version, False, False, True)
        self.assertEqual("", experiment_id)

    @patch('autosubmit.experiment.experiment_common.db_common')
    def test_create_new_evaluation_experiment_with_empty_current(self, db_common_mock):
        current_experiment_id = ""
        self._build_db_mock(current_experiment_id, db_common_mock)
        experiment_id = new_experiment(self.description, self.version, False, False, True)
        self.assertEqual("", experiment_id)

    @staticmethod
    def _build_db_mock(current_experiment_id, mock_db_common):
        mock_db_common.last_name_used = Mock(return_value=current_experiment_id)
        mock_db_common.check_experiment_exists = Mock(return_value=False)

    @patch('autosubmit.autosubmit.read_files')
    def test_autosubmit_generate_config(self, read_files_mock):
        expid = 'ff99'
        original_local_root_dir = BasicConfig.LOCAL_ROOT_DIR
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file_path = os.path.join(temp_dir, 'files')
            os.makedirs(temp_file_path)
            
            with tempfile.NamedTemporaryFile(dir=temp_file_path, suffix='.yaml', mode='w') as source_yaml:
                # Our processed and commented YAML output file must be written here
                Path(temp_dir, expid, 'conf').mkdir(parents=True)
                BasicConfig.LOCAL_ROOT_DIR = temp_dir

                source_yaml.write(
                dedent('''
                        JOB:
                            JOBNAME: SIM
                            PLATFORM: local
                        CONFIG:
                            TEST: The answer?
                            ROOT: No'''))
                source_yaml.flush()
                read_files_mock.return_value = Path(temp_dir) 

                parameters = {
                    'JOB': {
                        'JOBNAME': 'sim'
                    },
                    'CONFIG': {
                        'CONFIG.TEST': '42'
                    }
                }
                Autosubmit.generate_as_config(exp_id=expid, parameters=parameters)

                source_text = Path(source_yaml.name).read_text()
                source_name = Path(source_yaml.name)
                output_text = Path(temp_dir, expid, 'conf', f'{source_name.stem}_{expid}.yml').read_text()

                self.assertNotEqual(source_text, output_text)
                self.assertFalse('# sim' in source_text)
                self.assertTrue('# sim' in output_text)
                self.assertFalse('# 42' in source_text)
                print(output_text)
                self.assertTrue('# 42' in output_text)

        # Reset the local root dir.
        BasicConfig.LOCAL_ROOT_DIR = original_local_root_dir

    @patch('autosubmit.autosubmit.YAML.dump')
    @patch('autosubmit.autosubmit.read_files')
    def test_autosubmit_generate_config_resource_listdir_order(
            self,
            read_files_mock,
            yaml_mock
    ) -> None:
        """
        In https://earth.bsc.es/gitlab/es/autosubmit/-/issues/1063 we had a bug
        where we relied on the order of returned entries of ``pkg_resources.resource_listdir``
        (which is actually undefined per https://importlib-resources.readthedocs.io/en/latest/migration.html).

        We use the arrays below to test that defining a git minimal, we process only
        the expected files. We permute and then product the arrays to get as many test
        cases as possible.

        For every test case, we know that for dummy and minimal we get just one configuration
        template file used. But for other cases we get as many files as we have that are not
        ``*minimal.yml`` nor ``*dummy.yml``. In our test cases here, when not dummy and not minimal
        we must get 2 files, since we have ``include_me_please.yml`` and ``me_too.yml``.

        :param resource_filename_mock: mocked resource_listdir
        :param resource_listdir_mock: mocked resource_filename
        :param yaml_mock: mocked YAML dump function
        :return: None
        """

        # unique lists of resources, no repetitions
        resources = permutations(['dummy.yml', 'local-minimal.yml', 'git-minimal.yml', 'include_me_please.yml', 'me_too.yml'])
        dummy = [True, False]
        local = [True, False]
        minimal_configuration = [True, False]
        test_cases = product(resources, dummy, local, minimal_configuration)
        keys = ['resources', 'dummy', 'local', 'minimal_configuration']

        for test_case in test_cases:
            test = dict(zip(keys, test_case))
            expid = 'ff99'
            original_local_root_dir = BasicConfig.LOCAL_ROOT_DIR

            with tempfile.TemporaryDirectory() as temp_dir:
                Path(temp_dir, expid, 'conf').mkdir(parents=True)
                temp_file_path = os.path.join(temp_dir, 'files')
                os.makedirs(temp_file_path)
                
                BasicConfig.LOCAL_ROOT_DIR = temp_dir

                resources_return = []
                filenames_return = []

                for file_name in test['resources']:
                    input_path = Path(temp_file_path, file_name)
                    with open(input_path, 'w+') as source_yaml:

                        source_yaml.write('TEST: YES')
                        source_yaml.flush()

                        resources_return.append(input_path.name)  # path
                        filenames_return.append(source_yaml.name)  # textiowrapper

                read_files_mock.return_value = Path(temp_dir) 

                Autosubmit.generate_as_config(
                    exp_id=expid,
                    dummy=test['dummy'],
                    minimal_configuration=test['minimal_configuration'],
                    local=test['local'])

                msg = f'Incorrect call count for resources={",".join(resources_return)}, dummy={test["dummy"]}, minimal_configuration={test["minimal_configuration"]}, local={test["local"]}'
                expected = 2 if (not test['dummy'] and not test['minimal_configuration']) else 1
                self.assertEqual(yaml_mock.call_count, expected, msg=msg)
                yaml_mock.reset_mock()

            # Reset the local root dir.
            BasicConfig.LOCAL_ROOT_DIR = original_local_root_dir
