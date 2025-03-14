import inspect
import os
import sys
import tempfile
from pathlib import Path


from contextlib import suppress

from autosubmit.job.job_list_persistence import JobListPersistencePkl
import datetime

# compatibility with both versions (2 & 3)
from sys import version_info
from textwrap import dedent
from unittest import TestCase
from autosubmit.job.job_utils import calendar_chunk_section

from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.configcommon import BasicConfig, YAMLParserFactory
from mock import Mock, MagicMock
from mock import patch

import log.log
from autosubmit.autosubmit import Autosubmit
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.platforms.platform import Platform

if version_info.major == 2:
    import builtins as builtins
else:
    import builtins

# import the exception. Three dots means two folders up the hierarchy
# reference: https://peps.python.org/pep-0328/
from log.log import AutosubmitCritical

class TestJob(TestCase):
    def setUp(self):
        self.experiment_id = 'random-id'
        self.job_name = 'random-name'
        self.job_id = 999
        self.job_priority = 0
        self.as_conf = MagicMock()
        self.as_conf.experiment_data = dict()
        self.as_conf.experiment_data["JOBS"] = dict()
        self.as_conf.jobs_data = self.as_conf.experiment_data["JOBS"]
        self.as_conf.experiment_data["PLATFORMS"] = dict()
        self.job = Job(self.job_name, self.job_id, Status.WAITING, self.job_priority)
        self.job.processors = 2
        self.as_conf.load_project_parameters = Mock(return_value=dict())


    def test_when_the_job_has_more_than_one_processor_returns_the_parallel_platform(self):
        platform = Platform(self.experiment_id, 'parallel-platform', FakeBasicConfig().props())
        platform.serial_platform = 'serial-platform'

        self.job._platform = platform
        self.job.processors = 999

        returned_platform = self.job.platform

        self.assertEqual(platform, returned_platform)

    def test_when_the_job_has_only_one_processor_returns_the_serial_platform(self):
        platform = Platform(self.experiment_id, 'parallel-platform', FakeBasicConfig().props())
        platform.serial_platform = 'serial-platform'

        self.job._platform = platform
        self.job.processors = 1

        returned_platform = self.job.platform

        self.assertEqual('serial-platform', returned_platform)

    def test_set_platform(self):
        dummy_platform = Platform('whatever', 'rand-name', FakeBasicConfig().props())
        self.assertNotEqual(dummy_platform, self.job.platform)

        self.job.platform = dummy_platform

        self.assertEqual(dummy_platform, self.job.platform)

    def test_when_the_job_has_a_queue_returns_that_queue(self):
        dummy_queue = 'whatever'
        self.job._queue = dummy_queue

        returned_queue = self.job.queue

        self.assertEqual(dummy_queue, returned_queue)

    def test_when_the_job_has_not_a_queue_and_some_processors_returns_the_queue_of_the_platform(self):
        dummy_queue = 'whatever-parallel'
        dummy_platform = Platform('whatever', 'rand-name', FakeBasicConfig().props())
        dummy_platform.queue = dummy_queue
        self.job.platform = dummy_platform

        self.assertIsNone(self.job._queue)

        returned_queue = self.job.queue

        self.assertIsNotNone(returned_queue)
        self.assertEqual(dummy_queue, returned_queue)

    def test_when_the_job_has_not_a_queue_and_one_processor_returns_the_queue_of_the_serial_platform(self):
        serial_queue = 'whatever-serial'
        parallel_queue = 'whatever-parallel'

        dummy_serial_platform = Platform('whatever', 'serial', FakeBasicConfig().props())
        dummy_serial_platform.serial_queue = serial_queue

        dummy_platform = Platform('whatever', 'parallel', FakeBasicConfig().props())
        dummy_platform.serial_platform = dummy_serial_platform
        dummy_platform.queue = parallel_queue
        dummy_platform.processors_per_node = "1"
        #dummy_platform.hyperthreading = "false"

        self.job._platform = dummy_platform
        self.job.processors = '1'

        self.assertIsNone(self.job._queue)

        returned_queue = self.job.queue

        self.assertIsNotNone(returned_queue)
        self.assertEqual(serial_queue, returned_queue)
        self.assertNotEqual(parallel_queue, returned_queue)

    def test_set_queue(self):
        dummy_queue = 'whatever'
        self.assertNotEqual(dummy_queue, self.job._queue)

        self.job.queue = dummy_queue

        self.assertEqual(dummy_queue, self.job.queue)

    def test_that_the_increment_fails_count_only_adds_one(self):
        initial_fail_count = self.job.fail_count
        self.job.inc_fail_count()
        incremented_fail_count = self.job.fail_count

        self.assertEqual(initial_fail_count + 1, incremented_fail_count)

    def test_parents_and_children_management(self):
        random_job1 = Job('dummy-name', 111, Status.WAITING, 0)
        random_job2 = Job('dummy-name2', 222, Status.WAITING, 0)
        random_job3 = Job('dummy-name3', 333, Status.WAITING, 0)

        self.job.add_parent(random_job1,
                            random_job2,
                            random_job3)

        # assert added
        self.assertEqual(3, len(self.job.parents))
        self.assertEqual(1, len(random_job1.children))
        self.assertEqual(1, len(random_job2.children))
        self.assertEqual(1, len(random_job3.children))

        # assert contains
        self.assertTrue(self.job.parents.__contains__(random_job1))
        self.assertTrue(self.job.parents.__contains__(random_job2))
        self.assertTrue(self.job.parents.__contains__(random_job3))

        self.assertTrue(random_job1.children.__contains__(self.job))
        self.assertTrue(random_job2.children.__contains__(self.job))
        self.assertTrue(random_job3.children.__contains__(self.job))

        # assert has
        self.assertFalse(self.job.has_children())
        self.assertTrue(self.job.has_parents())

        # assert deletions
        self.job.delete_parent(random_job3)
        self.assertEqual(2, len(self.job.parents))

        random_job1.delete_child(self.job)
        self.assertEqual(0, len(random_job1.children))

    @patch('autosubmitconfigparser.config.basicconfig.BasicConfig')
    def test_header_tailer(self, mocked_global_basic_config: Mock):
        """Test if header and tailer are being properly substituted onto the final .cmd file without
        a bunch of mocks

        Copied from Aina's and Bruno's test for the reservation key. Hence, the following code still
        applies: "Actually one mock, but that's for something in the AutosubmitConfigParser that can
        be modified to remove the need of that mock."
        """

        # set up

        expid = 'zzyy'

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, expid).mkdir()
            # FIXME: (Copied from Bruno) Not sure why but the submitted and Slurm were using the $expid/tmp/ASLOGS folder?
            for path in [f'{expid}/tmp', f'{expid}/tmp/ASLOGS', f'{expid}/tmp/ASLOGS_{expid}', f'{expid}/proj',
                         f'{expid}/conf', f'{expid}/proj/project_files']:
                Path(temp_dir, path).mkdir()
            # loop over the host script's type
            for script_type in ["Bash", "Python", "Rscript"]:
                # loop over the position of the extension
                for extended_position in ["header", "tailer", "header tailer", "neither"]:
                    # loop over the extended type
                    for extended_type in ["Bash", "Python", "Rscript", "Bad1", "Bad2", "FileNotFound"]:
                            BasicConfig.LOCAL_ROOT_DIR = str(temp_dir)

                            header_file_name = ""
                            # this is the part of the script that executes
                            header_content = ""
                            tailer_file_name = ""
                            tailer_content = ""

                            # create the extended header and tailer scripts
                            if "header" in extended_position:
                                if extended_type == "Bash":
                                    header_content = 'echo "header bash"'
                                    full_header_content = dedent(f'''\
                                                                    #!/usr/bin/bash
                                                                    {header_content}
                                                                    ''')
                                    header_file_name = "header.sh"
                                elif extended_type == "Python":
                                    header_content = 'print("header python")'
                                    full_header_content = dedent(f'''\
                                                                    #!/usr/bin/python
                                                                    {header_content}
                                                                    ''')
                                    header_file_name = "header.py"
                                elif extended_type == "Rscript":
                                    header_content = 'print("header R")'
                                    full_header_content = dedent(f'''\
                                                                    #!/usr/bin/env Rscript
                                                                    {header_content}
                                                                    ''')
                                    header_file_name = "header.R"
                                elif extended_type == "Bad1":
                                    header_content = 'this is a script without #!'
                                    full_header_content = dedent(f'''\
                                                                    {header_content}
                                                                    ''')
                                    header_file_name = "header.bad1"
                                elif extended_type == "Bad2":
                                    header_content = 'this is a header with a bath executable'
                                    full_header_content = dedent(f'''\
                                                                    #!/does/not/exist
                                                                    {header_content}
                                                                    ''')
                                    header_file_name = "header.bad2"
                                else:  # file not found case
                                    header_file_name = "non_existent_header"

                                if extended_type != "FileNotFound":
                                    # build the header script if we need to
                                    with open(Path(temp_dir, f'{expid}/proj/project_files/{header_file_name}'), 'w+') as header:
                                        header.write(full_header_content)
                                        header.flush()
                                else:
                                    # make sure that the file does not exist
                                    for file in os.listdir(Path(temp_dir, f'{expid}/proj/project_files/')):
                                        os.remove(Path(temp_dir, f'{expid}/proj/project_files/{file}'))

                            if "tailer" in extended_position:
                                if extended_type == "Bash":
                                    tailer_content = 'echo "tailer bash"'
                                    full_tailer_content = dedent(f'''\
                                                                    #!/usr/bin/bash
                                                                    {tailer_content}
                                                                    ''')
                                    tailer_file_name = "tailer.sh"
                                elif extended_type == "Python":
                                    tailer_content = 'print("tailer python")'
                                    full_tailer_content = dedent(f'''\
                                                                    #!/usr/bin/python
                                                                    {tailer_content}
                                                                    ''')
                                    tailer_file_name = "tailer.py"
                                elif extended_type == "Rscript":
                                    tailer_content = 'print("header R")'
                                    full_tailer_content = dedent(f'''\
                                                                    #!/usr/bin/env Rscript
                                                                    {tailer_content}
                                                                    ''')
                                    tailer_file_name = "tailer.R"
                                elif extended_type == "Bad1":
                                    tailer_content = 'this is a script without #!'
                                    full_tailer_content = dedent(f'''\
                                                                    {tailer_content}
                                                                    ''')
                                    tailer_file_name = "tailer.bad1"
                                elif extended_type == "Bad2":
                                    tailer_content = 'this is a tailer with a bath executable'
                                    full_tailer_content = dedent(f'''\
                                                                    #!/does/not/exist
                                                                    {tailer_content}
                                                                    ''')
                                    tailer_file_name = "tailer.bad2"
                                else:  # file not found case
                                    tailer_file_name = "non_existent_tailer"

                                if extended_type != "FileNotFound":
                                    # build the tailer script if we need to
                                    with open(Path(temp_dir, f'{expid}/proj/project_files/{tailer_file_name}'), 'w+') as tailer:
                                        tailer.write(full_tailer_content)
                                        tailer.flush()
                                else:
                                    # clear the content of the project file
                                    for file in os.listdir(Path(temp_dir, f'{expid}/proj/project_files/')):
                                        os.remove(Path(temp_dir, f'{expid}/proj/project_files/{file}'))

                            # configuration file

                            with open(Path(temp_dir, f'{expid}/conf/configuration.yml'), 'w+') as configuration:
                                configuration.write(dedent(f'''\
DEFAULT:
    EXPID: {expid}
    HPCARCH: local
JOBS:
    A:
        FILE: a
        TYPE: {script_type if script_type != "Rscript" else "R"}
        PLATFORM: local
        RUNNING: once
        EXTENDED_HEADER_PATH: {header_file_name}
        EXTENDED_TAILER_PATH: {tailer_file_name}
PLATFORMS:
    test:
        TYPE: slurm
        HOST: localhost
        PROJECT: abc
        QUEUE: debug
        USER: me
        SCRATCH_DIR: /anything/
        ADD_PROJECT_TO_HOST: False
        MAX_WALLCLOCK: '00:55'
        TEMP_DIR: ''
CONFIG:
    RETRIALS: 0
                                '''))

                                configuration.flush()

                            mocked_basic_config = FakeBasicConfig
                            mocked_basic_config.read = MagicMock()

                            mocked_basic_config.LOCAL_ROOT_DIR = str(temp_dir)
                            mocked_basic_config.STRUCTURES_DIR = '/dummy/structures/dir'

                            mocked_global_basic_config.LOCAL_ROOT_DIR.return_value = str(temp_dir)

                            config = AutosubmitConfig(expid, basic_config=mocked_basic_config, parser_factory=YAMLParserFactory())
                            config.reload(True)

                            # act

                            parameters = config.load_parameters()
                            joblist_persistence = JobListPersistencePkl()

                            job_list_obj = JobList(expid, config, YAMLParserFactory(),joblist_persistence)

                            job_list_obj.generate(
                                as_conf=config,
                                date_list=[],
                                member_list=[],
                                num_chunks=1,
                                chunk_ini=1,
                                parameters=parameters,
                                date_format='M',
                                default_retrials=config.get_retrials(),
                                default_job_type=config.get_default_job_type(),
                                wrapper_jobs={},
                                new=True,
                                run_only_members=config.get_member_list(run_only=True),
                                show_log=True,
                            )
                            job_list = job_list_obj.get_job_list()

                            submitter = Autosubmit._get_submitter(config)
                            submitter.load_platforms(config)

                            hpcarch = config.get_platform()
                            for job in job_list:
                                if job.platform_name == "" or job.platform_name is None:
                                    job.platform_name = hpcarch
                                job.platform = submitter.platforms[job.platform_name]

                            # pick ur single job
                            job = job_list[0]

                            if extended_position == "header" or extended_position == "tailer" or extended_position == "header tailer":
                                if extended_type == script_type:
                                    # load the parameters
                                    job.check_script(config, parameters)
                                    # create the script
                                    job.create_script(config)
                                    with open(Path(temp_dir, f'{expid}/tmp/zzyy_A.cmd'), 'r') as file:
                                        full_script = file.read()
                                        if "header" in extended_position:
                                            self.assertTrue(header_content in full_script)
                                        if "tailer" in extended_position:
                                            self.assertTrue(tailer_content in full_script)
                                else:  # extended_type != script_type
                                    if extended_type == "FileNotFound":
                                        with self.assertRaises(AutosubmitCritical) as context:
                                            job.check_script(config, parameters)
                                        self.assertEqual(context.exception.code, 7014)
                                        if extended_position == "header tailer" or extended_position == "header":
                                            self.assertEqual(context.exception.message,
                                                             f"Extended header script: failed to fetch [Errno 2] No such file or directory: '{temp_dir}/{expid}/proj/project_files/{header_file_name}' \n")
                                        else:  # extended_position == "tailer":
                                            self.assertEqual(context.exception.message,
                                                             f"Extended tailer script: failed to fetch [Errno 2] No such file or directory: '{temp_dir}/{expid}/proj/project_files/{tailer_file_name}' \n")
                                    elif extended_type == "Bad1" or extended_type == "Bad2":
                                        # we check if a script without hash bang fails or with a bad executable
                                        with self.assertRaises(AutosubmitCritical) as context:
                                            job.check_script(config, parameters)
                                        self.assertEqual(context.exception.code, 7011)
                                        if extended_position == "header tailer" or extended_position == "header":
                                            self.assertEqual(context.exception.message,
                                                             f"Extended header script: couldn't figure out script {header_file_name} type\n")
                                        else:
                                            self.assertEqual(context.exception.message,
                                                             f"Extended tailer script: couldn't figure out script {tailer_file_name} type\n")
                                    else:  # if extended type is any but the script_type and the malformed scripts
                                        with self.assertRaises(AutosubmitCritical) as context:
                                            job.check_script(config, parameters)
                                        self.assertEqual(context.exception.code, 7011)
                                        # if we have both header and tailer, it will fail at the header first
                                        if extended_position == "header tailer" or extended_position == "header":
                                            self.assertEqual(context.exception.message,
                                                             f"Extended header script: script {header_file_name} seems "
                                                             f"{extended_type} but job zzyy_A.cmd isn't\n")
                                        else:  # extended_position == "tailer"
                                            self.assertEqual(context.exception.message,
                                                             f"Extended tailer script: script {tailer_file_name} seems "
                                                             f"{extended_type} but job zzyy_A.cmd isn't\n")
                            else: # extended_position == "neither"
                                # assert it doesn't exist
                                # load the parameters
                                job.check_script(config, parameters)
                                # create the script
                                job.create_script(config)
                                # finally, if we don't have scripts, check if the placeholders have been removed
                                with open(Path(temp_dir, f'{expid}/tmp/zzyy_A.cmd'), 'r') as file:
                                    final_script = file.read()
                                    self.assertFalse("%EXTENDED_HEADER%" in final_script)
                                    self.assertFalse("%EXTENDED_TAILER%" in final_script)

    @patch('autosubmitconfigparser.config.basicconfig.BasicConfig')
    def test_hetjob(self, mocked_global_basic_config: Mock):
        """
        Test job platforms with a platform. Builds job and platform using YAML data, without mocks.
        :param mocked_global_basic_config:
        :return:
        """
        expid = "zzyy"
        with tempfile.TemporaryDirectory() as temp_dir:
            BasicConfig.LOCAL_ROOT_DIR = str(temp_dir)
            Path(temp_dir, expid).mkdir()
            for path in [f'{expid}/tmp', f'{expid}/tmp/ASLOGS', f'{expid}/tmp/ASLOGS_{expid}', f'{expid}/proj',
                         f'{expid}/conf']:
                Path(temp_dir, path).mkdir()
            with open(Path(temp_dir, f'{expid}/conf/experiment_data.yml'), 'w+') as experiment_data:
                experiment_data.write(dedent(f'''\
                            CONFIG:
                              RETRIALS: 0 
                            DEFAULT:
                              EXPID: {expid}
                              HPCARCH: test
                            PLATFORMS:
                              test:
                                TYPE: slurm
                                HOST: localhost
                                PROJECT: abc
                                QUEUE: debug
                                USER: me
                                SCRATCH_DIR: /anything/
                                ADD_PROJECT_TO_HOST: False
                                MAX_WALLCLOCK: '00:55'
                                TEMP_DIR: ''
                            '''))
                experiment_data.flush()
            # For could be added here to cover more configurations options
            with open(Path(temp_dir, f'{expid}/conf/hetjob.yml'), 'w+') as hetjob:
                hetjob.write(dedent(f'''\
                            JOBS:
                                HETJOB_A:
                                    FILE: a
                                    PLATFORM: test
                                    RUNNING: once
                                    WALLCLOCK: '00:30'
                                    MEMORY: 
                                        - 0
                                        - 0
                                    NODES:
                                        - 3
                                        - 1
                                    TASKS:
                                        - 32
                                        - 32 
                                    THREADS:
                                        - 4
                                        - 4
                                    CUSTOM_DIRECTIVES: 
                                        - ['#SBATCH --export=ALL', '#SBATCH --distribution=block:cyclic', '#SBATCH --exclusive']
                                        - ['#SBATCH --export=ALL', '#SBATCH --distribution=block:cyclic:fcyclic', '#SBATCH --exclusive']
                '''))

            basic_config = FakeBasicConfig()
            basic_config.read()
            basic_config.LOCAL_ROOT_DIR = str(temp_dir)

            config = AutosubmitConfig(expid, basic_config=basic_config, parser_factory=YAMLParserFactory())
            config.reload(True)
            parameters = config.load_parameters()
            job_list_obj = JobList(expid, config, YAMLParserFactory(),
                                   Autosubmit._get_job_list_persistence(expid, config))

            job_list_obj.generate(
                as_conf=config,
                date_list=[],
                member_list=[],
                num_chunks=1,
                chunk_ini=1,
                parameters=parameters,
                date_format='M',
                default_retrials=config.get_retrials(),
                default_job_type=config.get_default_job_type(),
                wrapper_jobs={},
                new=True,
                run_only_members=[],
                #config.get_member_list(run_only=True),
                show_log=True,
                create=True,
            )

            job_list = job_list_obj.get_job_list()
            self.assertEqual(1, len(job_list))

            submitter = Autosubmit._get_submitter(config)
            submitter.load_platforms(config)

            hpcarch = config.get_platform()
            for job in job_list:
                if job.platform_name == "" or job.platform_name is None:
                    job.platform_name = hpcarch
                job.platform = submitter.platforms[job.platform_name]

            job = job_list[0]

            # This is the final header
            parameters = job.update_parameters(config, set_attributes=True)
            job.update_content(config, parameters)

            # Asserts the script is valid. There shouldn't be variables in the script that aren't in the parameters.
            checked = job.check_script(config, parameters)
            self.assertTrue(checked)

    @patch('autosubmitconfigparser.config.basicconfig.BasicConfig')
    def test_header_tailer(self, mocked_global_basic_config: Mock):
        """Test if header and tailer are being properly substituted onto the final .cmd file without
        a bunch of mocks

        Copied from Aina's and Bruno's test for the reservation key. Hence, the following code still
        applies: "Actually one mock, but that's for something in the AutosubmitConfigParser that can
        be modified to remove the need of that mock."
        """

        # set up

        expid = 'zzyy'

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, expid).mkdir()
            # FIXME: (Copied from Bruno) Not sure why but the submitted and Slurm were using the $expid/tmp/ASLOGS folder?
            for path in [f'{expid}/tmp', f'{expid}/tmp/ASLOGS', f'{expid}/tmp/ASLOGS_{expid}', f'{expid}/proj',
                         f'{expid}/conf', f'{expid}/proj/project_files']:
                Path(temp_dir, path).mkdir()
            # loop over the host script's type
            for script_type in ["Bash", "Python", "Rscript"]:
                # loop over the position of the extension
                for extended_position in ["header", "tailer", "header tailer", "neither"]:
                    # loop over the extended type
                    for extended_type in ["Bash", "Python", "Rscript", "Bad1", "Bad2", "FileNotFound"]:
                            BasicConfig.LOCAL_ROOT_DIR = str(temp_dir)

                            header_file_name = ""
                            # this is the part of the script that executes
                            header_content = ""
                            tailer_file_name = ""
                            tailer_content = ""

                            # create the extended header and tailer scripts
                            if "header" in extended_position:
                                if extended_type == "Bash":
                                    header_content = 'echo "header bash"'
                                    full_header_content = dedent(f'''\
                                                                    #!/usr/bin/bash
                                                                    {header_content}
                                                                    ''')
                                    header_file_name = "header.sh"
                                elif extended_type == "Python":
                                    header_content = 'print("header python")'
                                    full_header_content = dedent(f'''\
                                                                    #!/usr/bin/python
                                                                    {header_content}
                                                                    ''')
                                    header_file_name = "header.py"
                                elif extended_type == "Rscript":
                                    header_content = 'print("header R")'
                                    full_header_content = dedent(f'''\
                                                                    #!/usr/bin/env Rscript
                                                                    {header_content}
                                                                    ''')
                                    header_file_name = "header.R"
                                elif extended_type == "Bad1":
                                    header_content = 'this is a script without #!'
                                    full_header_content = dedent(f'''\
                                                                    {header_content}
                                                                    ''')
                                    header_file_name = "header.bad1"
                                elif extended_type == "Bad2":
                                    header_content = 'this is a header with a bath executable'
                                    full_header_content = dedent(f'''\
                                                                    #!/does/not/exist
                                                                    {header_content}
                                                                    ''')
                                    header_file_name = "header.bad2"
                                else:  # file not found case
                                    header_file_name = "non_existent_header"

                                if extended_type != "FileNotFound":
                                    # build the header script if we need to
                                    with open(Path(temp_dir, f'{expid}/proj/project_files/{header_file_name}'), 'w+') as header:
                                        header.write(full_header_content)
                                        header.flush()
                                else:
                                    # make sure that the file does not exist
                                    for file in os.listdir(Path(temp_dir, f'{expid}/proj/project_files/')):
                                        os.remove(Path(temp_dir, f'{expid}/proj/project_files/{file}'))

                            if "tailer" in extended_position:
                                if extended_type == "Bash":
                                    tailer_content = 'echo "tailer bash"'
                                    full_tailer_content = dedent(f'''\
                                                                    #!/usr/bin/bash
                                                                    {tailer_content}
                                                                    ''')
                                    tailer_file_name = "tailer.sh"
                                elif extended_type == "Python":
                                    tailer_content = 'print("tailer python")'
                                    full_tailer_content = dedent(f'''\
                                                                    #!/usr/bin/python
                                                                    {tailer_content}
                                                                    ''')
                                    tailer_file_name = "tailer.py"
                                elif extended_type == "Rscript":
                                    tailer_content = 'print("header R")'
                                    full_tailer_content = dedent(f'''\
                                                                    #!/usr/bin/env Rscript
                                                                    {tailer_content}
                                                                    ''')
                                    tailer_file_name = "tailer.R"
                                elif extended_type == "Bad1":
                                    tailer_content = 'this is a script without #!'
                                    full_tailer_content = dedent(f'''\
                                                                    {tailer_content}
                                                                    ''')
                                    tailer_file_name = "tailer.bad1"
                                elif extended_type == "Bad2":
                                    tailer_content = 'this is a tailer with a bath executable'
                                    full_tailer_content = dedent(f'''\
                                                                    #!/does/not/exist
                                                                    {tailer_content}
                                                                    ''')
                                    tailer_file_name = "tailer.bad2"
                                else:  # file not found case
                                    tailer_file_name = "non_existent_tailer"

                                if extended_type != "FileNotFound":
                                    # build the tailer script if we need to
                                    with open(Path(temp_dir, f'{expid}/proj/project_files/{tailer_file_name}'), 'w+') as tailer:
                                        tailer.write(full_tailer_content)
                                        tailer.flush()
                                else:
                                    # clear the content of the project file
                                    for file in os.listdir(Path(temp_dir, f'{expid}/proj/project_files/')):
                                        os.remove(Path(temp_dir, f'{expid}/proj/project_files/{file}'))

                            # configuration file

                            with open(Path(temp_dir, f'{expid}/conf/configuration.yml'), 'w+') as configuration:
                                configuration.write(dedent(f'''\
DEFAULT:
    EXPID: {expid}
    HPCARCH: local
JOBS:
    A:
        FILE: a
        TYPE: {script_type if script_type != "Rscript" else "R"}
        PLATFORM: local
        RUNNING: once
        EXTENDED_HEADER_PATH: {header_file_name}
        EXTENDED_TAILER_PATH: {tailer_file_name}
PLATFORMS:
    test:
        TYPE: slurm
        HOST: localhost
        PROJECT: abc
        QUEUE: debug
        USER: me
        SCRATCH_DIR: /anything/
        ADD_PROJECT_TO_HOST: False
        MAX_WALLCLOCK: '00:55'
        TEMP_DIR: ''
CONFIG:
    RETRIALS: 0
                                '''))

                                configuration.flush()

                            mocked_basic_config = FakeBasicConfig
                            mocked_basic_config.read = MagicMock()

                            mocked_basic_config.LOCAL_ROOT_DIR = str(temp_dir)
                            mocked_basic_config.STRUCTURES_DIR = '/dummy/structures/dir'

                            mocked_global_basic_config.LOCAL_ROOT_DIR.return_value = str(temp_dir)

                            config = AutosubmitConfig(expid, basic_config=mocked_basic_config, parser_factory=YAMLParserFactory())
                            config.reload(True)

                            # act

                            parameters = config.load_parameters()
                            joblist_persistence = JobListPersistencePkl()

                            job_list_obj = JobList(expid, config, YAMLParserFactory(),joblist_persistence)

                            job_list_obj.generate(
                                as_conf=config,
                                date_list=[],
                                member_list=[],
                                num_chunks=1,
                                chunk_ini=1,
                                parameters=parameters,
                                date_format='M',
                                default_retrials=config.get_retrials(),
                                default_job_type=config.get_default_job_type(),
                                wrapper_jobs={},
                                new=True,
                                run_only_members=config.get_member_list(run_only=True),
                                show_log=True,
                                create=True,
                            )
                            job_list = job_list_obj.get_job_list()

                            submitter = Autosubmit._get_submitter(config)
                            submitter.load_platforms(config)

                            hpcarch = config.get_platform()
                            for job in job_list:
                                if job.platform_name == "" or job.platform_name is None:
                                    job.platform_name = hpcarch
                                job.platform = submitter.platforms[job.platform_name]

                            # pick ur single job
                            job = job_list[0]
                            with suppress(Exception):
                                job.update_parameters(config, set_attributes=True) # TODO quick fix. This sets some attributes and eventually fails, should be fixed in the future

                            if extended_position == "header" or extended_position == "tailer" or extended_position == "header tailer":
                                if extended_type == script_type:
                                    # load the parameters
                                    job.check_script(config, parameters)
                                    # create the script
                                    job.create_script(config)
                                    with open(Path(temp_dir, f'{expid}/tmp/zzyy_A.cmd'), 'r') as file:
                                        full_script = file.read()
                                        if "header" in extended_position:
                                            self.assertTrue(header_content in full_script)
                                        if "tailer" in extended_position:
                                            self.assertTrue(tailer_content in full_script)
                                else:  # extended_type != script_type
                                    if extended_type == "FileNotFound":
                                        with self.assertRaises(AutosubmitCritical) as context:
                                            job.check_script(config, parameters)
                                        self.assertEqual(context.exception.code, 7014)
                                        if extended_position == "header tailer" or extended_position == "header":
                                            self.assertEqual(context.exception.message,
                                                             f"Extended header script: failed to fetch [Errno 2] No such file or directory: '{temp_dir}/{expid}/proj/project_files/{header_file_name}' \n")
                                        else:  # extended_position == "tailer":
                                            self.assertEqual(context.exception.message,
                                                             f"Extended tailer script: failed to fetch [Errno 2] No such file or directory: '{temp_dir}/{expid}/proj/project_files/{tailer_file_name}' \n")
                                    elif extended_type == "Bad1" or extended_type == "Bad2":
                                        # we check if a script without hash bang fails or with a bad executable
                                        with self.assertRaises(AutosubmitCritical) as context:
                                            job.check_script(config, parameters)
                                        self.assertEqual(context.exception.code, 7011)
                                        if extended_position == "header tailer" or extended_position == "header":
                                            self.assertEqual(context.exception.message,
                                                             f"Extended header script: couldn't figure out script {header_file_name} type\n")
                                        else:
                                            self.assertEqual(context.exception.message,
                                                             f"Extended tailer script: couldn't figure out script {tailer_file_name} type\n")
                                    else:  # if extended type is any but the script_type and the malformed scripts
                                        with self.assertRaises(AutosubmitCritical) as context:
                                            job.check_script(config, parameters)
                                        self.assertEqual(context.exception.code, 7011)
                                        # if we have both header and tailer, it will fail at the header first
                                        if extended_position == "header tailer" or extended_position == "header":
                                            self.assertEqual(context.exception.message,
                                                             f"Extended header script: script {header_file_name} seems "
                                                             f"{extended_type} but job zzyy_A.cmd isn't\n")
                                        else:  # extended_position == "tailer"
                                            self.assertEqual(context.exception.message,
                                                             f"Extended tailer script: script {tailer_file_name} seems "
                                                             f"{extended_type} but job zzyy_A.cmd isn't\n")
                            else: # extended_position == "neither"
                                # assert it doesn't exist
                                # load the parameters
                                job.check_script(config, parameters)
                                # create the script
                                job.create_script(config)
                                # finally, if we don't have scripts, check if the placeholders have been removed
                                with open(Path(temp_dir, f'{expid}/tmp/zzyy_A.cmd'), 'r') as file:
                                    final_script = file.read()
                                    self.assertFalse("%EXTENDED_HEADER%" in final_script)
                                    self.assertFalse("%EXTENDED_TAILER%" in final_script)

    @patch('autosubmitconfigparser.config.basicconfig.BasicConfig')
    def test_job_parameters(self, mocked_global_basic_config: Mock):
        """Test job platforms with a platform. Builds job and platform using YAML data, without mocks.

        Actually one mock, but that's for something in the AutosubmitConfigParser that can
        be modified to remove the need of that mock.
        """

        expid = 'zzyy'

        for reservation in [None, '', '  ', 'some-string', 'a', '123', 'True']:
            reservation_string = '' if not reservation else f'RESERVATION: "{reservation}"'
            with tempfile.TemporaryDirectory() as temp_dir:
                BasicConfig.LOCAL_ROOT_DIR = str(temp_dir)
                Path(temp_dir, expid).mkdir()
                # FIXME: Not sure why but the submitted and Slurm were using the $expid/tmp/ASLOGS folder?
                for path in [f'{expid}/tmp', f'{expid}/tmp/ASLOGS', f'{expid}/tmp/ASLOGS_{expid}', f'{expid}/proj',
                             f'{expid}/conf']:
                    Path(temp_dir, path).mkdir()
                with open(Path(temp_dir, f'{expid}/conf/minimal.yml'), 'w+') as minimal:
                    minimal.write(dedent(f'''\
                    CONFIG:
                      RETRIALS: 0 
                    DEFAULT:
                      EXPID: {expid}
                      HPCARCH: test
                    JOBS:
                      A:
                        FILE: a
                        PLATFORM: test
                        RUNNING: once
                        {reservation_string}
                    PLATFORMS:
                      test:
                        TYPE: slurm
                        HOST: localhost
                        PROJECT: abc
                        QUEUE: debug
                        USER: me
                        SCRATCH_DIR: /anything/
                        ADD_PROJECT_TO_HOST: False
                        MAX_WALLCLOCK: '00:55'
                        TEMP_DIR: ''
                    '''))
                    minimal.flush()

                basic_config = FakeBasicConfig()
                basic_config.read()
                basic_config.LOCAL_ROOT_DIR = str(temp_dir)

                config = AutosubmitConfig(expid, basic_config=basic_config, parser_factory=YAMLParserFactory())
                config.reload(True)
                parameters = config.load_parameters()

                job_list_obj = JobList(expid, config, YAMLParserFactory(),
                                       Autosubmit._get_job_list_persistence(expid, config))
                job_list_obj.generate(
                    as_conf=config,
                    date_list=[],
                    member_list=[],
                    num_chunks=1,
                    chunk_ini=1,
                    parameters=parameters,
                    date_format='M',
                    default_retrials=config.get_retrials(),
                    default_job_type=config.get_default_job_type(),
                    wrapper_jobs={},
                    new=True,
                    run_only_members=config.get_member_list(run_only=True),
                    show_log=True,
                    create=True,
                )
                job_list = job_list_obj.get_job_list()
                self.assertEqual(1, len(job_list))

                submitter = Autosubmit._get_submitter(config)
                submitter.load_platforms(config)

                hpcarch = config.get_platform()
                for job in job_list:
                    if job.platform_name == "" or job.platform_name is None:
                        job.platform_name = hpcarch
                    job.platform = submitter.platforms[job.platform_name]

                job = job_list[0]
                parameters = job.update_parameters(config, set_attributes=True)
                # Asserts the script is valid.
                checked = job.check_script(config, parameters)
                self.assertTrue(checked)

                # Asserts the configuration value is propagated as-is to the job parameters.
                # Finally, asserts the header created is correct.
                if not reservation:
                    self.assertTrue('JOBS.A.RESERVATION' not in parameters)
                    template_content, additional_templates = job.update_content(config, parameters)
                    self.assertFalse(additional_templates)

                    self.assertFalse(f'#SBATCH --reservation' in template_content)
                else:
                    self.assertEqual(reservation, parameters['JOBS.A.RESERVATION'])

                    template_content, additional_templates = job.update_content(config, parameters)
                    self.assertFalse(additional_templates)
                    self.assertTrue(f'#SBATCH --reservation={reservation}' in template_content)

    # def test_exists_completed_file_then_sets_status_to_completed(self):
    #     # arrange
    #     exists_mock = Mock(return_value=True)
    #     sys.modules['os'].path.exists = exists_mock
    #
    #     # act
    #     self.job.check_completion()
    #
    #     # assert
    #     exists_mock.assert_called_once_with(os.path.join(self.job._tmp_path, self.job.name + '_COMPLETED'))
    #     self.assertEqual(Status.COMPLETED, self.job.status)

    # def test_completed_file_not_exists_then_sets_status_to_failed(self):
    #     # arrange
    #     exists_mock = Mock(return_value=False)
    #     sys.modules['os'].path.exists = exists_mock
    #
    #     # act
    #     self.job.check_completion()
    #
    #     # assert
    #     exists_mock.assert_called_once_with(os.path.join(self.job._tmp_path, self.job.name + '_COMPLETED'))
    #     self.assertEqual(Status.FAILED, self.job.status)

    def test_total_processors(self):
        for test in [
            {
                'processors': '',
                'nodes': 0,
                'expected': 1
            },
            {
                'processors': '',
                'nodes': 10,
                'expected': ''
            },
            {
                'processors': '42',
                'nodes': 2,
                'expected': 42
            },
            {
                'processors': '1:9',
                'nodes': 0,
                'expected': 10
            }
        ]:
            self.job.processors = test['processors']
            self.job.nodes = test['nodes']
            self.assertEqual(self.job.total_processors, test['expected'])

    def test_job_script_checking_contains_the_right_variables(self):
        # This test (and feature) was implemented in order to avoid
        # false positives on the checking process with auto-ecearth3
        # Arrange
        parameters = {}
        section = "RANDOM-SECTION"
        self.job._init_runtime_parameters()
        self.job.section = section
        parameters['ROOTDIR'] = "none"
        parameters['PROJECT_TYPE'] = "none"
        processors = 80
        threads = 1
        tasks = 16
        memory = 80
        wallclock = "00:30"
        self.as_conf.get_member_list = Mock(return_value = [])
        custom_directives = '["whatever"]'
        options = {
            'PROCESSORS': processors,
            'THREADS': threads,
            'TASKS': tasks,
            'MEMORY': memory,
            'WALLCLOCK': wallclock,
            'CUSTOM_DIRECTIVES': custom_directives,
            'SCRATCH_FREE_SPACE': 0,
            'PLATFORM': 'dummy_platform',
        }
        self.as_conf.jobs_data[section] = options

        dummy_serial_platform = MagicMock()
        dummy_serial_platform.name = 'serial'
        dummy_platform = MagicMock()
        dummy_platform.serial_platform = dummy_serial_platform
        dummy_platform.name = 'dummy_platform'
        dummy_platform.max_wallclock = '00:55'

        self.as_conf.substitute_dynamic_variables = MagicMock()
        default = {'d': '%d%', 'd_': '%d_%', 'Y': '%Y%', 'Y_': '%Y_%',
                                              'M': '%M%', 'M_': '%M_%', 'm': '%m%', 'm_': '%m_%'}
        self.as_conf.substitute_dynamic_variables.return_value = default
        dummy_platform.custom_directives = '["whatever"]'
        self.as_conf.dynamic_variables = {}
        self.as_conf.parameters = MagicMock()
        self.as_conf.return_value = {}
        self.as_conf.normalize_parameters_keys = MagicMock()
        self.as_conf.normalize_parameters_keys.return_value = default
        self.job._platform = dummy_platform
        self.as_conf.platforms_data = { "DUMMY_PLATFORM":{ "whatever":"dummy_value", "whatever2":"dummy_value2"} }

        parameters = {}
        # Act
        parameters = self.job.update_parameters(self.as_conf, set_attributes=True)
        # Assert
        self.assertTrue('CURRENT_WHATEVER' in parameters)
        self.assertTrue('CURRENT_WHATEVER2' in parameters)

        self.assertEqual('dummy_value', parameters['CURRENT_WHATEVER'])
        self.assertEqual('dummy_value2', parameters['CURRENT_WHATEVER2'])
        self.assertTrue('d' in parameters)
        self.assertTrue('d_' in parameters)
        self.assertTrue('Y' in parameters)
        self.assertTrue('Y_' in parameters)
        self.assertEqual('%d%', parameters['d'])
        self.assertEqual('%d_%', parameters['d_'])
        self.assertEqual('%Y%', parameters['Y'])
        self.assertEqual('%Y_%', parameters['Y_'])
        # update parameters when date is not none and chunk is none
        self.job.date = datetime.datetime(1975, 5, 25, 22, 0, 0, 0, datetime.timezone.utc)
        self.job.chunk = None
        parameters = self.job.update_parameters(self.as_conf, set_attributes=True)
        self.assertEqual(1,parameters['CHUNK'])
        # update parameters when date is not none and chunk is not none
        self.job.date = datetime.datetime(1975, 5, 25, 22, 0, 0, 0, datetime.timezone.utc)
        self.job.chunk = 1
        self.job.date_format = 'H'
        parameters = self.job.update_parameters(self.as_conf, set_attributes=True)
        self.assertEqual(1, parameters['CHUNK'])
        self.assertEqual("TRUE", parameters['CHUNK_FIRST'])
        self.assertEqual("TRUE", parameters['CHUNK_LAST'])
        self.assertEqual("1975", parameters['CHUNK_START_YEAR'])
        self.assertEqual("05", parameters['CHUNK_START_MONTH'])
        self.assertEqual("25", parameters['CHUNK_START_DAY'])
        self.assertEqual("22", parameters['CHUNK_START_HOUR'])
        self.assertEqual("1975", parameters['CHUNK_END_YEAR'])
        self.assertEqual("05", parameters['CHUNK_END_MONTH'])
        self.assertEqual("26", parameters['CHUNK_END_DAY'])
        self.assertEqual("22", parameters['CHUNK_END_HOUR'])
        self.assertEqual("1975", parameters['CHUNK_SECOND_TO_LAST_YEAR'])

        self.assertEqual("05", parameters['CHUNK_SECOND_TO_LAST_MONTH'])
        self.assertEqual("25", parameters['CHUNK_SECOND_TO_LAST_DAY'])
        self.assertEqual("22", parameters['CHUNK_SECOND_TO_LAST_HOUR'])
        self.assertEqual('1975052522', parameters['CHUNK_START_DATE'])
        self.assertEqual('1975052622', parameters['CHUNK_END_DATE'])
        self.assertEqual('1975052522', parameters['CHUNK_SECOND_TO_LAST_DATE'])
        self.assertEqual('1975052422', parameters['DAY_BEFORE'])
        self.assertEqual('1', parameters['RUN_DAYS'])

        self.job.chunk = 2
        parameters = {"EXPERIMENT.NUMCHUNKS": 3, "EXPERIMENT.CHUNKSIZEUNIT": "hour"}
        parameters = self.job.update_parameters(self.as_conf, set_attributes=True)
        self.assertEqual(2, parameters['CHUNK'])
        self.assertEqual("FALSE", parameters['CHUNK_FIRST'])
        self.assertEqual("FALSE", parameters['CHUNK_LAST'])



    def test_sdate(self):
        """Test that the property getter for ``sdate`` works as expected."""
        for test in [
            [None, None, ''],
            [datetime.datetime(1975, 5, 25, 22, 0, 0, 0, datetime.timezone.utc), 'H', '1975052522'],
            [datetime.datetime(1975, 5, 25, 22, 30, 0, 0, datetime.timezone.utc), 'M', '197505252230'],
            [datetime.datetime(1975, 5, 25, 22, 30, 0, 0, datetime.timezone.utc), 'S', '19750525223000'],
            [datetime.datetime(1975, 5, 25, 22, 30, 0, 0, datetime.timezone.utc), None, '19750525']
        ]:
            self.job.date = test[0]
            self.job.date_format = test[1]
            self.assertEqual(test[2], self.job.sdate)

    def test__repr__(self):
        self.job.name = "dummy-name"
        self.job.status = "dummy-status"
        self.assertEqual("dummy-name STATUS: dummy-status", self.job.__repr__())

    def test_add_child(self):
        child = Job("child", 1, Status.WAITING, 0)
        self.job.add_children([child])
        self.assertEqual(1, len(self.job.children))
        self.assertEqual(child, list(self.job.children)[0])

    def test_auto_calendar_split(self):
        self.experiment_data = {
            'EXPERIMENT': {
                'DATELIST': '20000101',
                'MEMBERS': 'fc0',
                'CHUNKSIZEUNIT': 'day',
                'CHUNKSIZE': '1',
                'NUMCHUNKS': '2',
                'CALENDAR': 'standard'
            },
            'JOBS': {
                'A': {
                    'FILE': 'a',
                    'PLATFORM': 'test',
                    'RUNNING': 'chunk',
                    'SPLITS': 'auto',
                    'SPLITSIZE': 1
                },
                'B': {
                    'FILE': 'b',
                    'PLATFORM': 'test',
                    'RUNNING': 'chunk',
                    'SPLITS': 'auto',
                    'SPLITSIZE': 2
                }
            }
            }
        section = "A"
        date = datetime.datetime.strptime("20000101", "%Y%m%d")
        chunk = 1
        splits = calendar_chunk_section(self.experiment_data, section, date, chunk)
        self.assertEqual(splits, 24)
        splits = calendar_chunk_section(self.experiment_data, "B", date, chunk)
        self.assertEqual(splits, 12)
        self.experiment_data['EXPERIMENT']['CHUNKSIZEUNIT'] = 'hour'
        with self.assertRaises(AutosubmitCritical):
            calendar_chunk_section(self.experiment_data, "A", date, chunk)

        self.experiment_data['EXPERIMENT']['CHUNKSIZEUNIT'] = 'month'
        splits = calendar_chunk_section(self.experiment_data, "A", date, chunk)
        self.assertEqual(splits, 31)
        splits = calendar_chunk_section(self.experiment_data, "B", date, chunk)
        self.assertEqual(splits, 16)

        self.experiment_data['EXPERIMENT']['CHUNKSIZEUNIT'] = 'year'
        splits = calendar_chunk_section(self.experiment_data, "A", date, chunk)
        self.assertEqual(splits, 31)
        splits = calendar_chunk_section(self.experiment_data, "B", date, chunk)
        self.assertEqual(splits, 16)





    def test_calendar(self):
        split = 12
        splitsize = 2
        expid = 'zzyy'
        with tempfile.TemporaryDirectory() as temp_dir:
            BasicConfig.LOCAL_ROOT_DIR = str(temp_dir)
            Path(temp_dir, expid).mkdir()
            for path in [f'{expid}/tmp', f'{expid}/tmp/ASLOGS', f'{expid}/tmp/ASLOGS_{expid}', f'{expid}/proj',
                         f'{expid}/conf']:
                Path(temp_dir, path).mkdir()
            with open(Path(temp_dir, f'{expid}/conf/minimal.yml'), 'w+') as minimal:
                minimal.write(dedent(f'''\
                CONFIG:
                  RETRIALS: 0 
                DEFAULT:
                  EXPID: {expid}
                  HPCARCH: test
                EXPERIMENT:
                  # List of start dates
                  DATELIST: '20000101'
                  # List of members.
                  MEMBERS: fc0
                  # Unit of the chunk size. Can be hour, day, month, or year.
                  CHUNKSIZEUNIT: day
                  # Size of each chunk.
                  CHUNKSIZE: '4'
                  # Size of each split
                  SPLITSIZE: {splitsize}
                  # Number of chunks of the experiment.
                  NUMCHUNKS: '2'
                  CHUNKINI: ''
                  # Calendar used for the experiment. Can be standard or noleap.
                  CALENDAR: standard
                    
                JOBS:
                  A:
                    FILE: a
                    PLATFORM: test
                    RUNNING: chunk
                    SPLITS: {split}
                    SPLITSIZE: {splitsize}
                PLATFORMS:
                  test:
                    TYPE: slurm
                    HOST: localhost
                    PROJECT: abc
                    QUEUE: debug
                    USER: me
                    SCRATCH_DIR: /anything/
                    ADD_PROJECT_TO_HOST: False
                    MAX_WALLCLOCK: '00:55'
                    TEMP_DIR: ''
                '''))
                minimal.flush()

            basic_config = FakeBasicConfig()
            basic_config.read()
            basic_config.LOCAL_ROOT_DIR = str(temp_dir)

            config = AutosubmitConfig(expid, basic_config=basic_config, parser_factory=YAMLParserFactory())
            config.reload(True)
            parameters = config.load_parameters()

            job_list = JobList(expid, config, YAMLParserFactory(),
                                   Autosubmit._get_job_list_persistence(expid, config))
            job_list.generate(
                as_conf=config,
                date_list=[datetime.datetime.strptime("20000101", "%Y%m%d")],
                member_list=["fc0"],
                num_chunks=2,
                chunk_ini=1,
                parameters=parameters,
                date_format='',
                default_retrials=config.get_retrials(),
                default_job_type=config.get_default_job_type(),
                wrapper_jobs={},
                new=True,
                run_only_members=config.get_member_list(run_only=True),
                show_log=True,
                create=True,
            )
            job_list = job_list.get_job_list()
            self.assertEqual(24, len(job_list))

            submitter = Autosubmit._get_submitter(config)
            submitter.load_platforms(config)

            hpcarch = config.get_platform()
            for job in job_list:
                job.date_format = ""
                if job.platform_name == "" or job.platform_name is None:
                    job.platform_name = hpcarch
                job.platform = submitter.platforms[job.platform_name]

            # Check splits
            # Assert general
            job = job_list[0]
            parameters = job.update_parameters(config, set_attributes=True)
            self.assertEqual(job.splits, 12)
            self.assertEqual(job.running, 'chunk')

            self.assertEqual(parameters['SPLIT'], 1)
            self.assertEqual(parameters['SPLITSIZE'], splitsize)
            self.assertEqual(parameters['SPLITSIZEUNIT'], 'hour')
            self.assertEqual(parameters['SPLITSCALENDAR'], 'standard')
            # assert parameters
            next_start = "00"
            for i,job in enumerate(job_list[0:12]):
                parameters = job.update_parameters(config, set_attributes=True)
                end_hour = str(parameters['SPLIT'] * splitsize ).zfill(2)
                if end_hour == "24":
                    end_hour = "00"
                self.assertEqual(parameters['SPLIT'], i+1)
                self.assertEqual(parameters['SPLITSIZE'], splitsize)
                self.assertEqual(parameters['SPLITSIZEUNIT'], 'hour')
                self.assertEqual(parameters['SPLIT_START_DATE'], '20000101')
                self.assertEqual(parameters['SPLIT_START_YEAR'], '2000')
                self.assertEqual(parameters['SPLIT_START_MONTH'], '01')
                self.assertEqual(parameters['SPLIT_START_DAY'], '01')
                self.assertEqual(parameters['SPLIT_START_HOUR'], next_start)
                if parameters['SPLIT'] == 12:
                    self.assertEqual(parameters['SPLIT_END_DATE'], '20000102')
                    self.assertEqual(parameters['SPLIT_END_DAY'], '02')
                    self.assertEqual(parameters['SPLIT_END_DATE'], '20000102')
                    self.assertEqual(parameters['SPLIT_END_DAY'], '02')
                    self.assertEqual(parameters['SPLIT_END_YEAR'], '2000')
                    self.assertEqual(parameters['SPLIT_END_MONTH'], '01')
                    self.assertEqual(parameters['SPLIT_END_HOUR'], end_hour)
                else:
                    self.assertEqual(parameters['SPLIT_END_DATE'], '20000101')
                    self.assertEqual(parameters['SPLIT_END_DAY'], '01')
                    self.assertEqual(parameters['SPLIT_END_YEAR'], '2000')
                    self.assertEqual(parameters['SPLIT_END_MONTH'], '01')
                    self.assertEqual(parameters['SPLIT_END_HOUR'], end_hour)
                next_start = parameters['SPLIT_END_HOUR']
            next_start = "00"
            for i,job in enumerate(job_list[12:24]):
                parameters = job.update_parameters(config, set_attributes=True)
                end_hour = str(parameters['SPLIT'] * splitsize ).zfill(2)
                if end_hour == "24":
                    end_hour = "00"
                self.assertEqual(parameters['SPLIT'], i+1)
                self.assertEqual(parameters['SPLITSIZE'], splitsize)
                self.assertEqual(parameters['SPLITSIZEUNIT'], 'hour')
                self.assertEqual(parameters['SPLIT_START_DATE'], '20000105')
                self.assertEqual(parameters['SPLIT_START_YEAR'], '2000')
                self.assertEqual(parameters['SPLIT_START_MONTH'], '01')
                self.assertEqual(parameters['SPLIT_START_DAY'], '05')
                self.assertEqual(parameters['SPLIT_START_HOUR'], next_start)
                if parameters['SPLIT'] == 12:
                    self.assertEqual(parameters['SPLIT_END_DATE'], '20000106')
                    self.assertEqual(parameters['SPLIT_END_DAY'], '06')
                    self.assertEqual(parameters['SPLIT_END_YEAR'], '2000')
                    self.assertEqual(parameters['SPLIT_END_MONTH'], '01')
                    self.assertEqual(parameters['SPLIT_END_HOUR'], end_hour)
                else:
                    self.assertEqual(parameters['SPLIT_END_DATE'], '20000105')
                    self.assertEqual(parameters['SPLIT_END_DAY'], '05')
                    self.assertEqual(parameters['SPLIT_END_YEAR'], '2000')
                    self.assertEqual(parameters['SPLIT_END_MONTH'], '01')
                    self.assertEqual(parameters['SPLIT_END_HOUR'], end_hour)
                next_start = parameters['SPLIT_END_HOUR']



class FakeBasicConfig:
    def __init__(self):
        pass
    def props(self):
        pr = {}
        for name in dir(self):
            value = getattr(self, name)
            if not name.startswith('__') and not inspect.ismethod(value) and not inspect.isfunction(value):
                pr[name] = value
        return pr
    def read(self):
        FakeBasicConfig.DB_DIR = '/dummy/db/dir'
        FakeBasicConfig.DB_FILE = '/dummy/db/file'
        FakeBasicConfig.DB_PATH = '/dummy/db/path'
        FakeBasicConfig.LOCAL_ROOT_DIR = '/dummy/local/root/dir'
        FakeBasicConfig.LOCAL_TMP_DIR = '/dummy/local/temp/dir'
        FakeBasicConfig.LOCAL_PROJ_DIR = '/dummy/local/proj/dir'
        FakeBasicConfig.DEFAULT_PLATFORMS_CONF = ''
        FakeBasicConfig.DEFAULT_JOBS_CONF = ''
        FakeBasicConfig.STRUCTURES_DIR = '/dummy/structures/dir'
    DB_DIR = '/dummy/db/dir'
    DB_FILE = '/dummy/db/file'
    DB_PATH = '/dummy/db/path'
    LOCAL_ROOT_DIR = '/dummy/local/root/dir'
    LOCAL_TMP_DIR = '/dummy/local/temp/dir'
    LOCAL_PROJ_DIR = '/dummy/local/proj/dir'
    DEFAULT_PLATFORMS_CONF = ''
    DEFAULT_JOBS_CONF = ''
    STRUCTURES_DIR = '/dummy/structures/dir'


def test_update_stat_file():
    job = Job("dummyname", 1, Status.WAITING, 0)
    job.fail_count = 0
    job.script_name = "dummyname.cmd"
    job.wrapper_type = None
    job.update_stat_file()
    assert job.stat_file == "dummyname_STAT_"
    job.fail_count = 1
    job.update_stat_file()
    assert job.stat_file == "dummyname_STAT_"



def test_pytest_check_script(mocker):
    job = Job("job1", "1", Status.READY, 0)
    # arrange
    parameters = dict()
    parameters['NUMPROC'] = 999
    parameters['NUMTHREADS'] = 777
    parameters['NUMTASK'] = 666
    parameters['RESERVATION'] = "random-string"
    mocker.patch("autosubmit.job.job.Job.update_content", return_value=(
    'some-content: %NUMPROC%, %NUMTHREADS%, %NUMTASK%', 'some-content: %NUMPROC%, %NUMTHREADS%, %NUMTASK%'))
    mocker.patch("autosubmit.job.job.Job.update_parameters", return_value=parameters)
    job._init_runtime_parameters()

    config = Mock(spec=AutosubmitConfig)
    config.default_parameters = {}
    config.get_project_dir = Mock(return_value='/project/dir')

    # act
    checked = job.check_script(config, parameters)

    # todo
    # update_parameters_mock.assert_called_with(config, parameters)
    # update_content_mock.assert_called_with(config)

    # assert
    assert checked


def test_pytest_create_script(mocker):
    # arrange
    job = Job("job1", "1", Status.READY, 0)
    # arrange
    parameters = dict()
    parameters['NUMPROC'] = 999
    parameters['NUMTHREADS'] = 777
    parameters['NUMTASK'] = 666

    job._tmp_path = '/dummy/tmp/path'
    job.additional_files = '/dummy/tmp/path_additional_file'
    mocker.patch("autosubmit.job.job.Job.update_content", return_value=(
    'some-content: %NUMPROC%, %NUMTHREADS%, %NUMTASK% %% %%',
    ['some-content: %NUMPROC%, %NUMTHREADS%, %NUMTASK% %% %%']))
    mocker.patch("autosubmit.job.job.Job.update_parameters", return_value=parameters)

    config = Mock(spec=AutosubmitConfig)
    config.default_parameters = {}

    config.get_project_dir = Mock(return_value='/project/dir')

    chmod_mock = Mock()
    sys.modules['os'].chmod = chmod_mock

    write_mock = Mock().write = Mock()
    open_mock = Mock(return_value=write_mock)
    with patch.object(builtins, "open", open_mock):
        # act
        job.create_script(config)
    # TODO asserts _slots_
    # # assert
    # update_content_mock.assert_called_with(config)
    # # TODO add assert for additional files
    # open_mock.assert_called_with(os.path.join(job._tmp_path, job.name + '.cmd'), 'wb')
    # # Expected values: %% -> %, %KEY% -> KEY.VALUE without %
    # write_mock.write.assert_called_with(b'some-content: 999, 777, 666 % %')
    # chmod_mock.assert_called_with(os.path.join(job._tmp_path, job.name + '.cmd'), 0o755)


def test_reset_logs(autosubmit_config):
    experiment_data = {
        'AUTOSUBMIT': {
            'WORKFLOW_COMMIT': "dummy-commit",
        },
    }
    as_conf = autosubmit_config("test-expid", experiment_data)
    job = Job("job1", "1", Status.READY, 0)
    job.reset_logs(as_conf)
    assert job.workflow_commit == "dummy-commit"
    assert job.updated_log is False
    assert job.packed_during_building is False

def test_pytest_that_check_script_returns_false_when_there_is_an_unbound_template_variable(mocker):
    job = Job("job1", "1", Status.READY, 0)
    # arrange
    job._init_runtime_parameters()
    parameters = {}
    mocker.patch("autosubmit.job.job.Job.update_content",
                 return_value=('some-content: %UNBOUND%', 'some-content: %UNBOUND%'))
    mocker.patch("autosubmit.job.job.Job.update_parameters", return_value=parameters)
    job._init_runtime_parameters()

    config = Mock(spec=AutosubmitConfig)
    config.default_parameters = {}
    config.get_project_dir = Mock(return_value='/project/dir')

    # act
    checked = job.check_script(config, parameters)

    # assert TODO __slots
    # update_parameters_mock.assert_called_with(config, parameters)
    # update_content_mock.assert_called_with(config)
    assert checked is False
