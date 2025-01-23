import datetime
import json
import tempfile
from pathlib import Path
from subprocess import CalledProcessError
from tempfile import TemporaryDirectory
from unittest import TestCase

from mock import Mock, patch
from rocrate.rocrate import File
from rocrate.rocrate import ROCrate
from ruamel.yaml import YAML
from ruamel.yaml.representer import RepresenterError

from autosubmit.autosubmit import Autosubmit
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.provenance.rocrate import (
    _add_dir_and_files,
    _get_action_status,
    _create_formal_parameter,
    _create_parameter,
    _get_project_entity,
    _get_git_branch_and_commit,
    create_rocrate_archive
)
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from log.log import AutosubmitCritical


class TestRoCrate(TestCase):

    def setUp(self):
        self.empty_rocrate = ROCrate()
        self.as_conf = Mock(spec=AutosubmitConfig)
        self.expid = 'zzzz'
        self.project_path = str(Path(__file__).parent.joinpath('../../../'))
        self.project_url = 'https://earth.bsc.es/gitlab/es/autosubmit.git'
        self.as_conf.get_project_dir = Mock(return_value=self.project_path)

    def tearDown(self) -> None:
        self.empty_rocrate = None

    def _create_conf_dir(self, parent, as_conf=None):
        if not as_conf:
            as_conf = self.as_conf
        conf_dir = Path(parent, 'conf')
        conf_dir.mkdir(exist_ok=True)
        Path(conf_dir, 'metadata').mkdir()
        unified_config = Path(conf_dir, 'metadata/experiment_data.yml')
        unified_config.touch()
        yaml = YAML(typ='rt')
        with open(unified_config, 'w') as f:
            yaml.dump(dict(as_conf.experiment_data), f)
        as_conf.current_loaded_files = {unified_config: 0}

    def test_add_dir_and_files_empty_folder(self):
        with TemporaryDirectory() as d:
            _add_dir_and_files(
                crate=self.empty_rocrate,
                base_path=d,
                relative_path=d,
                encoding_format=None
            )
        self.assertEqual(1, len(self.empty_rocrate.data_entities))

    def test_add_dir_and_files(self):
        with TemporaryDirectory() as d:
            sub_path = Path(d, 'files')
            sub_path.mkdir(parents=True)
            with open(sub_path / 'file.txt', 'w+') as f:
                f.write('hello')
                f.flush()

                _add_dir_and_files(
                    crate=self.empty_rocrate,
                    base_path=d,
                    relative_path=str(sub_path),
                    encoding_format=None
                )
        self.assertEqual(2, len(self.empty_rocrate.data_entities))
        for entity in self.empty_rocrate.data_entities:
            if entity.source.name == 'file.txt':
                properties = entity.properties()
                self.assertTrue(properties['sdDatePublished'])
                self.assertTrue(properties['dateModified'])
                self.assertEqual(properties['encodingFormat'], 'text/plain')
                break
        else:
            self.fail('Failed to locate the entity for files/file.txt')

    def test_add_dir_and_files_set_encoding(self):
        encoding = 'image/jpeg'
        with TemporaryDirectory() as d:
            with TemporaryDirectory() as d:
                sub_path = Path(d, 'files')
                sub_path.mkdir(parents=True)
                with open(sub_path / 'file.txt', 'w+') as f:
                    f.write('hello')
                    f.flush()

                    _add_dir_and_files(
                        crate=self.empty_rocrate,
                        base_path=d,
                        relative_path=str(sub_path),
                        encoding_format=encoding
                    )
            self.assertEqual(2, len(self.empty_rocrate.data_entities))
            for entity in self.empty_rocrate.data_entities:
                if entity.source.name == 'file.txt':
                    properties = entity.properties()
                    self.assertTrue(properties['sdDatePublished'])
                    self.assertTrue(properties['dateModified'])
                    self.assertEqual(properties['encodingFormat'], encoding)
                    break
            else:
                self.fail('Failed to locate the entity for files/file.txt')

    def test_get_action_status(self):
        for tests in [
            ([], 'PotentialActionStatus'),
            ([Job('a', 'a', Status.FAILED, 1), Job('b', 'b', Status.COMPLETED, 1)], 'FailedActionStatus'),
            ([Job('a', 'a', Status.COMPLETED, 1), Job('b', 'b', Status.COMPLETED, 1)], 'CompletedActionStatus'),
            ([Job('a', 'a', Status.DELAYED, 1)], 'PotentialActionStatus')
        ]:
            jobs = tests[0]
            expected = tests[1]
            result = _get_action_status(jobs)
            self.assertEqual(expected, result)

    def test_create_formal_parameter(self):
        formal_parameter = _create_formal_parameter(self.empty_rocrate, 'Name')
        properties = formal_parameter.properties()
        self.assertEqual('#Name-param', properties['@id'])
        self.assertEqual('FormalParameter', properties['@type'])
        self.assertEqual('Name', properties['name'])

    def test_create_parameter(self):
        formal_parameter = _create_formal_parameter(self.empty_rocrate, 'Answer')
        parameter = _create_parameter(
            self.empty_rocrate,
            'Answer',
            42,
            formal_parameter,
            'PropertyValue',
            extra='test'
        )
        properties = parameter.properties()
        self.assertEqual(42, properties['value'])
        self.assertEqual('test', properties['extra'])

    def test_get_local_project_entity(self):
        project_path = '/tmp/project'
        project_url = f'file://{project_path}'
        self.as_conf.experiment_data = {
            'PROJECT': {
                'PROJECT_TYPE': 'LOCAL'
            },
            'LOCAL': {
                'PROJECT_PATH': project_path
            }
        }
        project_entity = _get_project_entity(
            self.as_conf,
            self.empty_rocrate
        )

        self.assertEqual(project_entity['@id'], project_url)
        self.assertEqual(project_entity['targetProduct'], 'Autosubmit')
        self.assertEqual(project_entity['codeRepository'], project_url)
        self.assertEqual(project_entity['version'], '')

    def test_get_dummy_project_entity(self):
        project_url = ''
        self.as_conf.experiment_data = {
            'PROJECT': {
                'PROJECT_TYPE': 'NONE'
            }
        }
        project_entity = _get_project_entity(
            self.as_conf,
            self.empty_rocrate
        )

        self.assertEqual(project_entity['@id'], project_url)
        self.assertEqual(project_entity['targetProduct'], 'Autosubmit')
        self.assertEqual(project_entity['codeRepository'], project_url)
        self.assertEqual(project_entity['version'], '')

    def test_get_subversion_or_other_project_entity(self):
        for key in ['SVN', 'SUBVERSION', 'MERCURY', '', ' ']:
            self.as_conf.experiment_data = {
                'PROJECT': {
                    'PROJECT_TYPE': key
                },
                key: {
                    'PROJECT_PATH': ''
                }
            }
            with self.assertRaises(AutosubmitCritical):
                _get_project_entity(
                    self.as_conf,
                    self.empty_rocrate
                )

    def test_get_git_project_entity(self):
        self.as_conf.experiment_data = {
            'PROJECT': {
                'PROJECT_TYPE': 'GIT'
            },
            'GIT': {
                'PROJECT_PATH': self.project_path,
                'PROJECT_ORIGIN': self.project_url
            }
        }
        project_entity = _get_project_entity(
            self.as_conf,
            self.empty_rocrate
        )
        self.assertEqual(project_entity['@id'], self.project_url)
        self.assertEqual(project_entity['targetProduct'], 'Autosubmit')
        self.assertEqual(project_entity['codeRepository'], self.project_url)
        self.assertTrue(len(project_entity['version']) > 0)

    @patch('subprocess.check_output')
    def test_get_git_branch_and_commit(self, mocked_check_output: Mock):
        error = CalledProcessError(1, '')
        mocked_check_output.side_effect = [error]
        with self.assertRaises(AutosubmitCritical) as cm:
            _get_git_branch_and_commit(project_path='')

        self.assertEqual(cm.exception.message, 'Failed to retrieve project branch...')

        mocked_check_output.reset_mock()
        mocked_check_output.side_effect = ['master', error]
        with self.assertRaises(AutosubmitCritical) as cm:
            _get_git_branch_and_commit(project_path='')

        self.assertEqual(cm.exception.message, 'Failed to retrieve project commit SHA...')

    @patch('autosubmit.provenance.rocrate.BasicConfig')
    @patch('autosubmit.provenance.rocrate.get_experiment_descrip')
    @patch('autosubmit.provenance.rocrate.get_autosubmit_version')
    def test_rocrate(
            self,
            mocked_get_autosubmit_version: Mock,
            mocked_get_experiment_descrip: Mock,
            mocked_BasicConfig: Mock):
        with tempfile.TemporaryDirectory() as temp_dir:
            mocked_BasicConfig.LOCAL_ROOT_DIR = temp_dir
            mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
            experiment_path = Path(mocked_BasicConfig.LOCAL_ROOT_DIR, self.expid)
            experiment_path.mkdir()
            mocked_BasicConfig.LOCAL_TMP_DIR = Path(experiment_path, 'tmp')
            mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
            project_path = Path(experiment_path, 'proj')
            project_path.mkdir()
            # some outputs
            for output_file in ['graph_1.png', 'graph_2.gif', 'graph_3.gif', 'graph.jpg']:
                Path(project_path, output_file).touch()
            # required paths for AS
            for other_required_path in ['conf', 'pkl', 'plot', 'status']:
                Path(experiment_path, other_required_path).mkdir()
            self.as_conf.experiment_data = {
                'DEFAULT': {
                    'EXPID': self.expid
                },
                'EXPERIMENT': {},
                'CONFIG': {
                    'PRE': [
                        '%PROJ%/conf/bootstrap/include.yml'
                    ]
                },
                'ROOTDIR': str(experiment_path),
                'PROJECT': {
                    'PROJECT_DESTINATION': '',
                    'PROJECT_TYPE': 'LOCAL'
                },
                'LOCAL': {
                    'PROJECT_PATH': str(project_path)
                },
                'APP': {
                    'INPUT_1': 1,
                    'INPUT_2': 2
                }
            }
            rocrate_json = {
                'INPUTS': ['APP'],
                'OUTPUTS': [
                    'graph_*.gif'
                ],
                'PATCH': json.dumps({
                    '@graph': [
                        {
                            '@id': './',
                            "license": "Apache-2.0"
                        }
                    ]
                })
            }
            self._create_conf_dir(experiment_path)
            jobs = []
            start_time = ''
            end_time = ''

            mocked_get_autosubmit_version.return_value = '4.0.0b0'
            mocked_get_experiment_descrip.return_value = [
                ['mocked test project']
            ]

            crate = create_rocrate_archive(
                as_conf=self.as_conf,
                rocrate_json=rocrate_json,
                jobs=jobs,
                start_time=start_time,
                end_time=end_time,
                path=Path(temp_dir)
            )
            self.assertIsNotNone(crate)

    @patch('autosubmit.provenance.rocrate._get_project_entity')
    @patch('autosubmit.provenance.rocrate.BasicConfig')
    @patch('autosubmit.provenance.rocrate.get_experiment_descrip')
    @patch('autosubmit.provenance.rocrate.get_autosubmit_version')
    def test_rocrate_invalid_project(
            self,
            mocked_get_autosubmit_version: Mock,
            mocked_get_experiment_descrip: Mock,
            mocked_BasicConfig: Mock,
            mocked_get_project_entity: Mock):
        mocked_get_project_entity.side_effect = ValueError
        with tempfile.TemporaryDirectory() as temp_dir:
            mocked_BasicConfig.LOCAL_ROOT_DIR = temp_dir
            mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
            experiment_path = Path(mocked_BasicConfig.LOCAL_ROOT_DIR, self.expid)
            experiment_path.mkdir()
            mocked_BasicConfig.LOCAL_TMP_DIR = Path(experiment_path, 'tmp')
            mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
            project_path = Path(experiment_path, 'proj')
            project_path.mkdir()
            # some outputs
            for output_file in ['graph_1.png', 'graph_2.gif', 'graph_3.gif', 'graph.jpg']:
                Path(project_path, output_file).touch()
            # required paths for AS
            for other_required_path in ['conf', 'pkl', 'plot', 'status']:
                Path(experiment_path, other_required_path).mkdir()
            self.as_conf.experiment_data = {
                'DEFAULT': {
                    'EXPID': self.expid
                },
                'EXPERIMENT': {},
                'CONFIG': {},
                'ROOTDIR': str(experiment_path),
                'PROJECT': {
                    'PROJECT_DESTINATION': '',
                    'PROJECT_TYPE': 'GIT'
                },
                'GIT': {
                    'PROJECT_PATH': str(project_path),
                    'PROJECT_ORIGIN': self.project_url
                }
            }
            rocrate_json = {}
            self._create_conf_dir(experiment_path)
            jobs = []

            mocked_get_autosubmit_version.return_value = '4.0.0b0'
            mocked_get_experiment_descrip.return_value = [
                ['mocked test project']
            ]

            with self.assertRaises(AutosubmitCritical) as cm:
                create_rocrate_archive(
                    as_conf=self.as_conf,
                    rocrate_json=rocrate_json,
                    jobs=jobs,
                    start_time=None,
                    end_time=None,
                    path=Path(temp_dir)
                )

            self.assertEqual(cm.exception.message, 'Failed to read the Autosubmit Project for RO-Crate...')

    @patch('autosubmit.provenance.rocrate.BasicConfig')
    @patch('autosubmit.provenance.rocrate.get_experiment_descrip')
    @patch('autosubmit.provenance.rocrate.get_autosubmit_version')
    def test_rocrate_invalid_parameter_type(
            self,
            mocked_get_autosubmit_version: Mock,
            mocked_get_experiment_descrip: Mock,
            mocked_BasicConfig: Mock):
        """NOTE: This is not possible at the moment, as we are using ruamel.yaml
        to parse the YAML, and we are not supporting objects. But you never know
        what the code will do in the future, so we just make sure we fail nicely."""
        with tempfile.TemporaryDirectory() as temp_dir:
            mocked_BasicConfig.LOCAL_ROOT_DIR = temp_dir
            mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
            experiment_path = Path(mocked_BasicConfig.LOCAL_ROOT_DIR, self.expid)
            experiment_path.mkdir()
            mocked_BasicConfig.LOCAL_TMP_DIR = Path(experiment_path, 'tmp')
            mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
            project_path = Path(experiment_path, 'proj')
            project_path.mkdir()
            # some outputs
            for output_file in ['graph_1.png', 'graph_2.gif', 'graph_3.gif', 'graph.jpg']:
                Path(project_path, output_file).touch()
            # required paths for AS
            for other_required_path in ['conf', 'pkl', 'plot', 'status']:
                Path(experiment_path, other_required_path).mkdir()
            self.as_conf.experiment_data = {
                'DEFAULT': {
                    'EXPID': self.expid
                },
                'EXPERIMENT': {},
                'CONFIG': {},
                'ROOTDIR': str(experiment_path),
                'PROJECT': {
                    'PROJECT_DESTINATION': '',
                    'PROJECT_TYPE': 'GIT'
                },
                'GIT': {
                    'PROJECT_PATH': str(project_path),
                    'PROJECT_ORIGIN': self.project_url
                },
                'APP': {
                    'OBJ': object()
                }
            }
            rocrate_json = {
                'INPUTS': [
                    'APP'
                ]
            }

            mocked_get_autosubmit_version.return_value = '4.0.0b0'
            mocked_get_experiment_descrip.return_value = [
                ['mocked test project']
            ]

            with self.assertRaises(RepresenterError) as cm:
                self._create_conf_dir(experiment_path)

            self.assertTrue('cannot represent an object' in str(cm.exception))

    @patch('autosubmit.autosubmit.Log')
    @patch('autosubmit.autosubmit.AutosubmitConfig')
    def test_rocrate_main_fail_missing_rocrate(
            self,
            mocked_AutosubmitConfig: Mock,
            mocked_Log: Mock):
        mocked_as_conf = Mock(autospec=AutosubmitConfig)
        mocked_as_conf.experiment_data = {}
        mocked_AutosubmitConfig.return_value = mocked_as_conf

        mocked_Log.error = Mock()
        mocked_Log.error.return_value = ''

        autosubmit = Autosubmit()
        with self.assertRaises(AutosubmitCritical) as cm, tempfile.TemporaryDirectory() as temp_dir:
            autosubmit.rocrate(self.expid, path=Path(path=Path(temp_dir)))

        self.assertEqual(cm.exception.message, 'You must provide an ROCRATE configuration key when using RO-Crate...')
        self.assertEqual(mocked_Log.error.call_count, 1)

    @patch('autosubmit.autosubmit.JobList')
    @patch('autosubmit.autosubmit.AutosubmitConfig')
    @patch('autosubmit.provenance.rocrate.BasicConfig')
    @patch('autosubmit.provenance.rocrate.get_experiment_descrip')
    @patch('autosubmit.provenance.rocrate.get_autosubmit_version')
    def test_rocrate_main(
            self,
            mocked_get_autosubmit_version: Mock,
            mocked_get_experiment_descrip: Mock,
            mocked_BasicConfig: Mock,
            mocked_AutosubmitConfig: Mock,
            mocked_JobList: Mock):
        with tempfile.TemporaryDirectory() as temp_dir:
            mocked_BasicConfig.LOCAL_ROOT_DIR = temp_dir
            mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
            experiment_path = Path(mocked_BasicConfig.LOCAL_ROOT_DIR, self.expid)
            experiment_path.mkdir()
            mocked_BasicConfig.LOCAL_TMP_DIR = Path(experiment_path, 'tmp')
            mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
            project_path = Path(experiment_path, 'proj')
            project_path.mkdir()
            # some outputs
            for output_file in ['graph_1.png', 'graph_2.gif', 'graph_3.gif', 'graph.jpg']:
                Path(project_path, output_file).touch()
            # required paths for AS
            for other_required_path in ['conf', 'pkl', 'plot', 'status']:
                Path(experiment_path, other_required_path).mkdir()
            mocked_as_conf = Mock(autospec=AutosubmitConfig)
            mocked_AutosubmitConfig.return_value = mocked_as_conf
            mocked_as_conf.experiment_data = {
                'DEFAULT': {
                    'EXPID': self.expid
                },
                'EXPERIMENT': {},
                'CONFIG': {},
                'ROOTDIR': str(experiment_path),
                'PROJECT': {
                    'PROJECT_DESTINATION': '',
                    'PROJECT_TYPE': 'LOCAL'
                },
                'LOCAL': {
                    'PROJECT_PATH': str(project_path)
                },
                'APP': {
                    'INPUT_1': 1,
                    'INPUT_2': 2
                },
                'ROCRATE': {
                    'INPUTS': ['APP'],
                    'OUTPUTS': [
                        'graph_*.gif'
                    ],
                    'PATCH': json.dumps({
                        '@graph': [
                            {
                                '@id': './',
                                "license": "Apache-2.0"
                            }
                        ]
                    })
                }
            }
            self._create_conf_dir(experiment_path, as_conf=mocked_as_conf)
            mocked_as_conf.get_storage_type.return_value = 'pkl'
            mocked_as_conf.get_date_list.return_value = []

            mocked_get_autosubmit_version.return_value = '4.0.0b0'
            mocked_get_experiment_descrip.return_value = [
                ['mocked test project']
            ]

            mocked_job_list = Mock()
            mocked_JobList.return_value = mocked_job_list

            job1 = Mock(autospec=Job)
            job1_submit_time = datetime.datetime.strptime("21/11/06 16:30", "%d/%m/%y %H:%M")
            job1_start_time = datetime.datetime.strptime("21/11/06 16:40", "%d/%m/%y %H:%M")
            job1_finished_time = datetime.datetime.strptime("21/11/06 16:50", "%d/%m/%y %H:%M")
            job1.get_last_retrials.return_value = [
                [job1_submit_time, job1_start_time, job1_finished_time, 'COMPLETED']]
            job1.name = 'job1'
            job1.date = '2006'
            job1.member = 'fc0'
            job1.section = 'JOB'
            job1.chunk = '1'
            job1.processors = '1'

            job2 = Mock(autospec=Job)
            job2_submit_time = datetime.datetime.strptime("21/11/06 16:40", "%d/%m/%y %H:%M")
            job2_start_time = datetime.datetime.strptime("21/11/06 16:50", "%d/%m/%y %H:%M")
            job2_finished_time = datetime.datetime.strptime("21/11/06 17:00", "%d/%m/%y %H:%M")
            job2.get_last_retrials.return_value = [
                [job2_submit_time, job2_start_time, job2_finished_time, 'COMPLETED']]
            job2.name = 'job2'
            job2.date = '2006'
            job2.member = 'fc1'
            job2.section = 'JOB'
            job2.chunk = '1'
            job2.processors = '1'

            mocked_job_list.get_job_list.return_value = [job1, job2]
            mocked_job_list.get_ready.return_value = [] # Mock due the new addition in the job_list.load()
            mocked_job_list.get_waiting.return_value = [] # Mocked due the new addition in the job_list.load()
            autosubmit = Autosubmit()
            r = autosubmit.rocrate(self.expid, path=Path(temp_dir))
            self.assertTrue(r)

    @patch('autosubmit.provenance.rocrate.BasicConfig')
    @patch('autosubmit.provenance.rocrate.get_experiment_descrip')
    @patch('autosubmit.provenance.rocrate.get_autosubmit_version')
    def test_custom_config_loaded_file(
            self,
            mocked_get_autosubmit_version: Mock,
            mocked_get_experiment_descrip: Mock,
            mocked_BasicConfig: Mock):
        with tempfile.TemporaryDirectory() as temp_dir:
            mocked_BasicConfig.LOCAL_ROOT_DIR = temp_dir
            mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
            experiment_path = Path(mocked_BasicConfig.LOCAL_ROOT_DIR, self.expid)
            experiment_path.mkdir()
            mocked_BasicConfig.LOCAL_TMP_DIR = Path(experiment_path, 'tmp')
            mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
            project_path = Path(experiment_path, 'proj')
            project_path.mkdir()
            # required paths for AS
            for other_required_path in ['conf', 'pkl', 'plot', 'status']:
                Path(experiment_path, other_required_path).mkdir()

            # custom config file
            project_conf = Path(project_path, 'conf')
            project_conf.mkdir()
            custom_config = Path(project_conf, 'include.yml')
            custom_config.touch()
            custom_config.write_text('CUSTOM_CONFIG_LOADED: True')

            self.as_conf.experiment_data = {
                'DEFAULT': {
                    'EXPID': self.expid
                },
                'EXPERIMENT': {},
                'CONFIG': {
                    'PRE': [
                        str(project_conf)
                    ]
                },
                'ROOTDIR': str(experiment_path),
                'PROJECT': {
                    'PROJECT_DESTINATION': '',
                    'PROJECT_TYPE': 'LOCAL'
                },
                'LOCAL': {
                    'PROJECT_PATH': str(project_path)
                },
                'APP': {
                    'INPUT_1': 1,
                    'INPUT_2': 2
                }
            }
            rocrate_json = {
                'INPUTS': ['APP'],
                'OUTPUTS': [
                    'graph_*.gif'
                ],
                'PATCH': json.dumps({
                    '@graph': [
                        {
                            '@id': './',
                            "license": "Apache-2.0"
                        }
                    ]
                })
            }
            self._create_conf_dir(experiment_path)
            # adding both directory and file to the list of loaded files
            self.as_conf.current_loaded_files[str(project_conf)] = 0
            self.as_conf.current_loaded_files[str(custom_config)] = 0
            jobs = []
            start_time = ''
            end_time = ''

            mocked_get_autosubmit_version.return_value = '4.0.0b0'
            mocked_get_experiment_descrip.return_value = [
                ['mocked test project']
            ]

            crate = create_rocrate_archive(
                as_conf=self.as_conf,
                rocrate_json=rocrate_json,
                jobs=jobs,
                start_time=start_time,
                end_time=end_time,
                path=Path(temp_dir)
            )
            self.assertIsNotNone(crate)
            data_entities_ids = [data_entity['@id'] for data_entity in crate.data_entities]
            self.assertTrue(File(crate, f'file://{str(project_conf)}/').id in data_entities_ids)
            self.assertTrue(File(crate, f'file://{str(custom_config)}').id in data_entities_ids)

    @patch('autosubmit.provenance.rocrate.BasicConfig')
    @patch('autosubmit.provenance.rocrate.get_experiment_descrip')
    @patch('autosubmit.provenance.rocrate.get_autosubmit_version')
    def test_no_duplicate_ids(
            self,
            mocked_get_autosubmit_version: Mock,
            mocked_get_experiment_descrip: Mock,
            mocked_BasicConfig: Mock):
        with tempfile.TemporaryDirectory() as temp_dir:
            mocked_BasicConfig.LOCAL_ROOT_DIR = temp_dir
            mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
            experiment_path = Path(mocked_BasicConfig.LOCAL_ROOT_DIR, self.expid)
            experiment_path.mkdir()
            mocked_BasicConfig.LOCAL_TMP_DIR = Path(experiment_path, 'tmp')
            mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
            project_path = Path(experiment_path, 'proj')
            project_path.mkdir()
            # required paths for AS
            for other_required_path in ['conf', 'pkl', 'plot', 'status']:
                Path(experiment_path, other_required_path).mkdir()

            # custom config file
            project_conf = Path(project_path, 'conf')
            project_conf.mkdir()
            custom_config = Path(project_conf, 'include.yml')
            custom_config.touch()
            custom_config.write_text('CUSTOM_CONFIG_LOADED: True')

            self.as_conf.experiment_data = {
                'DEFAULT': {
                    'EXPID': self.expid
                },
                'EXPERIMENT': {},
                'CONFIG': {
                    'PRE': [
                        str(project_conf)
                    ]
                },
                'ROOTDIR': str(experiment_path),
                'PROJECT': {
                    'PROJECT_DESTINATION': '',
                    'PROJECT_TYPE': 'LOCAL'
                },
                'LOCAL': {
                    'PROJECT_PATH': str(project_path)
                },
                'APP': {
                    'INPUT_1': 1,
                    'INPUT_2': 2
                }
            }
            rocrate_json = {
                'INPUTS': ['APP'],
                'OUTPUTS': [
                    'graph_*.gif'
                ],
                'PATCH': json.dumps({
                    '@graph': [
                        {
                            '@id': './',
                            "license": "Apache-2.0"
                        }
                    ]
                })
            }
            self._create_conf_dir(experiment_path)
            # adding both directory and file to the list of loaded files
            self.as_conf.current_loaded_files[str(project_conf)] = 0
            self.as_conf.current_loaded_files[str(custom_config)] = 0
            jobs = []
            start_time = ''
            end_time = ''

            mocked_get_autosubmit_version.return_value = '4.0.0b0'
            mocked_get_experiment_descrip.return_value = [
                ['mocked test project']
            ]

            crate = create_rocrate_archive(
                as_conf=self.as_conf,
                rocrate_json=rocrate_json,
                jobs=jobs,
                start_time=start_time,
                end_time=end_time,
                path=Path(temp_dir)
            )
            self.assertIsNotNone(crate)
            data_entities_ids = [data_entity['@id'] for data_entity in crate.data_entities]
            self.assertEqual(len(data_entities_ids), len(set(data_entities_ids)), f'Duplicate IDs found in the RO-Crate data entities: {str(data_entities_ids)}')
