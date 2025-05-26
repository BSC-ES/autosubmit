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

import builtins
import inspect
import os
import pwd
import re
import sys
import tempfile
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from textwrap import dedent
from time import time
from typing import Any

import pytest
from bscearth.utils.date import date2str
from mock import Mock, MagicMock
from mock.mock import patch

from autosubmit.autosubmit import Autosubmit
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistencePkl
from autosubmit.job.job_utils import calendar_chunk_section
from autosubmit.job.job_utils import get_job_package_code, SubJob, SubJobManager
from autosubmit.platforms.platform import Platform
from autosubmit.platforms.psplatform import PsPlatform
from autosubmit.platforms.slurmplatform import SlurmPlatform
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.configcommon import BasicConfig, YAMLParserFactory
from log.log import AutosubmitCritical

"""Tests for the Autosubmit ``Job`` class."""


class TestJob:
    def setup_method(self):
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

        assert platform == returned_platform

    def test_when_the_job_has_only_one_processor_returns_the_serial_platform(self):
        platform = Platform(self.experiment_id, 'parallel-platform', FakeBasicConfig().props())
        platform.serial_platform = 'serial-platform'

        self.job._platform = platform
        self.job.processors = 1

        returned_platform = self.job.platform

        assert 'serial-platform' == returned_platform

    def test_set_platform(self):
        dummy_platform = Platform('whatever', 'rand-name', FakeBasicConfig().props())
        assert dummy_platform != self.job.platform

        self.job.platform = dummy_platform

        assert dummy_platform == self.job.platform

    def test_when_the_job_has_a_queue_returns_that_queue(self):
        dummy_queue = 'whatever'
        self.job._queue = dummy_queue

        returned_queue = self.job.queue

        assert dummy_queue == returned_queue

    def test_when_the_job_has_not_a_queue_and_some_processors_returns_the_queue_of_the_platform(self):
        dummy_queue = 'whatever-parallel'
        dummy_platform = Platform('whatever', 'rand-name', FakeBasicConfig().props())
        dummy_platform.queue = dummy_queue
        self.job.platform = dummy_platform

        assert self.job._queue is None

        returned_queue = self.job.queue

        assert returned_queue is not None
        assert dummy_queue == returned_queue

    def test_when_the_job_has_not_a_queue_and_one_processor_returns_the_queue_of_the_serial_platform(self):
        serial_queue = 'whatever-serial'
        parallel_queue = 'whatever-parallel'

        dummy_serial_platform = Platform('whatever', 'serial', FakeBasicConfig().props())
        dummy_serial_platform.serial_queue = serial_queue

        dummy_platform = Platform('whatever', 'parallel', FakeBasicConfig().props())
        dummy_platform.serial_platform = dummy_serial_platform
        dummy_platform.queue = parallel_queue
        dummy_platform.processors_per_node = "1"

        self.job._platform = dummy_platform
        self.job.processors = '1'

        assert self.job._queue is None

        returned_queue = self.job.queue

        assert returned_queue is not None
        assert serial_queue == returned_queue
        assert parallel_queue != returned_queue

    def test_set_queue(self):
        dummy_queue = 'whatever'
        assert dummy_queue != self.job._queue

        self.job.queue = dummy_queue

        assert dummy_queue == self.job.queue

    def test_that_the_increment_fails_count_only_adds_one(self):
        initial_fail_count = self.job.fail_count
        self.job.inc_fail_count()
        incremented_fail_count = self.job.fail_count

        assert initial_fail_count + 1 == incremented_fail_count

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
                                with open(Path(temp_dir, f'{expid}/proj/project_files/{header_file_name}'),
                                          'w+') as header:
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
                                with open(Path(temp_dir, f'{expid}/proj/project_files/{tailer_file_name}'),
                                          'w+') as tailer:
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

                        config = AutosubmitConfig(expid, basic_config=mocked_basic_config,
                                                  parser_factory=YAMLParserFactory())
                        config.reload(True)

                        # act

                        parameters = config.load_parameters()
                        joblist_persistence = JobListPersistencePkl()

                        job_list_obj = JobList(expid, config, YAMLParserFactory(), joblist_persistence)

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
                                        assert header_content in full_script
                                    if "tailer" in extended_position:
                                        assert tailer_content in full_script
                            else:  # extended_type != script_type
                                if extended_type == "FileNotFound":
                                    with pytest.raises(AutosubmitCritical) as context:
                                        job.check_script(config, parameters)
                                    assert context.value.code == 7014
                                    if extended_position == "header tailer" or extended_position == "header":
                                        assert context.value.message == \
                                               f"Extended header script: failed to fetch [Errno 2] No such file or directory: '{temp_dir}/{expid}/proj/project_files/{header_file_name}' \n"
                                    else:  # extended_position == "tailer":
                                        assert context.value.message == \
                                               f"Extended tailer script: failed to fetch [Errno 2] No such file or directory: '{temp_dir}/{expid}/proj/project_files/{tailer_file_name}' \n"
                                elif extended_type == "Bad1" or extended_type == "Bad2":
                                    # we check if a script without hash bang fails or with a bad executable
                                    with pytest.raises(AutosubmitCritical) as context:
                                        job.check_script(config, parameters)
                                    assert context.value.code == 7011
                                    if extended_position == "header tailer" or extended_position == "header":
                                        assert context.value.message == \
                                               f"Extended header script: couldn't figure out script {header_file_name} type\n"
                                    else:
                                        assert context.value.message == \
                                               f"Extended tailer script: couldn't figure out script {tailer_file_name} type\n"
                                else:  # if extended type is any but the script_type and the malformed scripts
                                    with pytest.raises(AutosubmitCritical) as context:
                                        job.check_script(config, parameters)
                                    assert context.value.code == 7011
                                    # if we have both header and tailer, it will fail at the header first
                                    if extended_position == "header tailer" or extended_position == "header":
                                        assert context.value.message == \
                                               f"Extended header script: script {header_file_name} seems " \
                                               f"{extended_type} but job zzyy_A.cmd isn't\n"
                                    else:  # extended_position == "tailer"
                                        assert context.value.message == \
                                               f"Extended tailer script: script {tailer_file_name} seems " \
                                               f"{extended_type} but job zzyy_A.cmd isn't\n"
                        else:  # extended_position == "neither"
                            # assert it doesn't exist
                            # load the parameters
                            job.check_script(config, parameters)
                            # create the script
                            job.create_script(config)
                            # finally, if we don't have scripts, check if the placeholders have been removed
                            with open(Path(temp_dir, f'{expid}/tmp/zzyy_A.cmd'), 'r') as file:
                                final_script = file.read()
                                assert "%EXTENDED_HEADER%" not in final_script
                                assert "%EXTENDED_TAILER%" not in final_script

    def test_hetjob(self):
        """
        Test job platforms with a platform. Builds job and platform using YAML data, without mocks.
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
                hetjob.write(dedent('''\
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
                # config.get_member_list(run_only=True),
                show_log=True,
                create=True,
            )

            job_list = job_list_obj.get_job_list()
            assert 1 == len(job_list)

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
            assert checked

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
                                with open(Path(temp_dir, f'{expid}/proj/project_files/{header_file_name}'),
                                          'w+') as header:
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
                                with open(Path(temp_dir, f'{expid}/proj/project_files/{tailer_file_name}'),
                                          'w+') as tailer:
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

                        config = AutosubmitConfig(expid, basic_config=mocked_basic_config,
                                                  parser_factory=YAMLParserFactory())
                        config.reload(True)

                        # act

                        parameters = config.load_parameters()
                        joblist_persistence = JobListPersistencePkl()

                        job_list_obj = JobList(expid, config, YAMLParserFactory(), joblist_persistence)

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
                            job.update_parameters(config,
                                                  set_attributes=True)  # TODO quick fix. This sets some attributes and eventually fails, should be fixed in the future

                        if extended_position == "header" or extended_position == "tailer" or extended_position == "header tailer":
                            if extended_type == script_type:
                                # load the parameters
                                job.check_script(config, parameters)
                                # create the script
                                job.create_script(config)
                                with open(Path(temp_dir, f'{expid}/tmp/zzyy_A.cmd'), 'r') as file:
                                    full_script = file.read()
                                    if "header" in extended_position:
                                        assert header_content in full_script
                                    if "tailer" in extended_position:
                                        assert tailer_content in full_script
                            else:  # extended_type != script_type
                                if extended_type == "FileNotFound":
                                    with pytest.raises(AutosubmitCritical) as context:
                                        job.check_script(config, parameters)
                                    assert context.value.code == 7014
                                    if extended_position == "header tailer" or extended_position == "header":
                                        assert context.value.message == \
                                               f"Extended header script: failed to fetch [Errno 2] No such file or directory: '{temp_dir}/{expid}/proj/project_files/{header_file_name}' \n"
                                    else:  # extended_position == "tailer":
                                        assert context.value.message == \
                                               f"Extended tailer script: failed to fetch [Errno 2] No such file or directory: '{temp_dir}/{expid}/proj/project_files/{tailer_file_name}' \n"
                                elif extended_type == "Bad1" or extended_type == "Bad2":
                                    # we check if a script without hash bang fails or with a bad executable
                                    with pytest.raises(AutosubmitCritical) as context:
                                        job.check_script(config, parameters)
                                    assert context.value.code == 7011
                                    if extended_position == "header tailer" or extended_position == "header":
                                        assert context.value.message == \
                                               f"Extended header script: couldn't figure out script {header_file_name} type\n"
                                    else:
                                        assert context.value.message == \
                                               f"Extended tailer script: couldn't figure out script {tailer_file_name} type\n"
                                else:  # if extended type is any but the script_type and the malformed scripts
                                    with pytest.raises(AutosubmitCritical) as context:
                                        job.check_script(config, parameters)
                                    assert context.value.code == 7011
                                    # if we have both header and tailer, it will fail at the header first
                                    if extended_position == "header tailer" or extended_position == "header":
                                        assert context.value.message == \
                                               f"Extended header script: script {header_file_name} seems " \
                                               f"{extended_type} but job zzyy_A.cmd isn't\n"
                                    else:  # extended_position == "tailer"
                                        assert context.value.message == \
                                               f"Extended tailer script: script {tailer_file_name} seems " \
                                               f"{extended_type} but job zzyy_A.cmd isn't\n"
                        else:  # extended_position == "neither"
                            # assert it doesn't exist
                            # load the parameters
                            job.check_script(config, parameters)
                            # create the script
                            job.create_script(config)
                            # finally, if we don't have scripts, check if the placeholders have been removed
                            with open(Path(temp_dir, f'{expid}/tmp/zzyy_A.cmd'), 'r') as file:
                                final_script = file.read()
                                assert "%EXTENDED_HEADER%" not in final_script
                                assert "%EXTENDED_TAILER%" not in final_script

    def test_job_parameters(self):
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
                assert 1 == len(job_list)

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
                assert checked

                # Asserts the configuration value is propagated as-is to the job parameters.
                # Finally, asserts the header created is correct.
                if not reservation:
                    assert 'JOBS.A.RESERVATION' not in parameters
                    template_content, additional_templates = job.update_content(config, parameters)
                    assert not additional_templates

                    assert '#SBATCH --reservation' not in template_content
                else:
                    assert reservation == parameters['JOBS.A.RESERVATION']

                    template_content, additional_templates = job.update_content(config, parameters)
                    assert not additional_templates
                    assert f'#SBATCH --reservation={reservation}' in template_content

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
            assert self.job.total_processors == test['expected']

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
        self.as_conf.get_member_list = Mock(return_value=[])
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
        self.as_conf.platforms_data = {"DUMMY_PLATFORM": {"whatever": "dummy_value", "whatever2": "dummy_value2"}}

        # Act
        parameters = self.job.update_parameters(self.as_conf, set_attributes=True)
        # Assert
        assert 'CURRENT_WHATEVER' in parameters
        assert 'CURRENT_WHATEVER2' in parameters

        assert 'dummy_value' == parameters['CURRENT_WHATEVER']
        assert 'dummy_value2' == parameters['CURRENT_WHATEVER2']
        assert 'd' in parameters
        assert 'd_' in parameters
        assert 'Y' in parameters
        assert 'Y_' in parameters
        assert '%d%' == parameters['d']
        assert '%d_%' == parameters['d_']
        assert '%Y%' == parameters['Y']
        assert '%Y_%' == parameters['Y_']
        # update parameters when date is not none and chunk is none
        self.job.date = datetime(1975, 5, 25, 22, 0, 0, 0, timezone.utc)
        self.job.chunk = None
        parameters = self.job.update_parameters(self.as_conf, set_attributes=True)
        assert 1 == parameters['CHUNK']
        # update parameters when date is not none and chunk is not none
        self.job.date = datetime(1975, 5, 25, 22, 0, 0, 0, timezone.utc)
        self.job.chunk = 1
        self.job.date_format = 'H'
        parameters = self.job.update_parameters(self.as_conf, set_attributes=True)
        assert 1 == parameters['CHUNK']
        assert "TRUE" == parameters['CHUNK_FIRST']
        assert "TRUE" == parameters['CHUNK_LAST']
        assert "1975" == parameters['CHUNK_START_YEAR']
        assert "05" == parameters['CHUNK_START_MONTH']
        assert "25" == parameters['CHUNK_START_DAY']
        assert "22" == parameters['CHUNK_START_HOUR']
        assert "1975" == parameters['CHUNK_END_YEAR']
        assert "05" == parameters['CHUNK_END_MONTH']
        assert "26" == parameters['CHUNK_END_DAY']
        assert "22" == parameters['CHUNK_END_HOUR']
        assert "1975" == parameters['CHUNK_SECOND_TO_LAST_YEAR']

        assert "05" == parameters['CHUNK_SECOND_TO_LAST_MONTH']
        assert "25" == parameters['CHUNK_SECOND_TO_LAST_DAY']
        assert "22" == parameters['CHUNK_SECOND_TO_LAST_HOUR']
        assert '1975052522' == parameters['CHUNK_START_DATE']
        assert '1975052622' == parameters['CHUNK_END_DATE']
        assert '1975052522' == parameters['CHUNK_SECOND_TO_LAST_DATE']
        assert '1975052422' == parameters['DAY_BEFORE']
        assert '1' == parameters['RUN_DAYS']

        self.job.chunk = 2
        parameters = self.job.update_parameters(self.as_conf, set_attributes=True)
        assert 2 == parameters['CHUNK']
        assert "FALSE" == parameters['CHUNK_FIRST']
        assert "FALSE" == parameters['CHUNK_LAST']

    def test_get_from_total_stats(self):
        """
        test of the function get_from_total_stats validating the file generation
        :return:
        """
        for creation_file in [False, True]:
            with tempfile.TemporaryDirectory() as temp_dir:
                mocked_basic_config = FakeBasicConfig
                mocked_basic_config.read = MagicMock()
                mocked_basic_config.LOCAL_ROOT_DIR = str(temp_dir)

                self.job._tmp_path = str(temp_dir)

                log_name = Path(f"{mocked_basic_config.LOCAL_ROOT_DIR}/{self.job.name}_TOTAL_STATS")
                Path(mocked_basic_config.LOCAL_ROOT_DIR).mkdir(parents=True, exist_ok=True)

                if creation_file:
                    with open(log_name, 'w+') as f:
                        f.write(dedent('''\
                            DEFAULT:
                                DATE: 1998
                                EXPID: 199803
                                HPCARCH: 19980324
                            '''))
                        f.flush()

                lst = self.job._get_from_total_stats(1)

            if creation_file:
                assert len(lst) == 3

                fmt = '%Y-%m-%d %H:%M'
                expected = [
                    datetime(1998, 1, 1, 0, 0),
                    datetime(1998, 3, 1, 0, 0),
                    datetime(1998, 3, 24, 0, 0)
                ]

                for left, right in zip(lst, expected):
                    assert left.strftime(fmt) == right.strftime(fmt)
            else:
                assert lst == []
                assert not log_name.exists()

    def test_sdate(self):
        """Test that the property getter for ``sdate`` works as expected."""
        for test in [
            [None, None, ''],
            [datetime(1975, 5, 25, 22, 0, 0, 0, timezone.utc), 'H', '1975052522'],
            [datetime(1975, 5, 25, 22, 30, 0, 0, timezone.utc), 'M', '197505252230'],
            [datetime(1975, 5, 25, 22, 30, 0, 0, timezone.utc), 'S', '19750525223000'],
            [datetime(1975, 5, 25, 22, 30, 0, 0, timezone.utc), None, '19750525']
        ]:
            self.job.date = test[0]
            self.job.date_format = test[1]
            assert test[2] == self.job.sdate

    def test__repr__(self):
        self.job.name = "dummy-name"
        self.job.status = "dummy-status"
        assert "dummy-name STATUS: dummy-status" == self.job.__repr__()

    def test_add_child(self):
        child = Job("child", 1, Status.WAITING, 0)
        self.job.add_children([child])
        assert 1 == len(self.job.children)
        assert child == list(self.job.children)[0]

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
        date = datetime.strptime("20000101", "%Y%m%d")
        chunk = 1
        splits = calendar_chunk_section(self.experiment_data, section, date, chunk)
        assert splits == 24
        splits = calendar_chunk_section(self.experiment_data, "B", date, chunk)
        assert splits == 12
        self.experiment_data['EXPERIMENT']['CHUNKSIZEUNIT'] = 'hour'
        with pytest.raises(AutosubmitCritical):
            calendar_chunk_section(self.experiment_data, "A", date, chunk)

        self.experiment_data['EXPERIMENT']['CHUNKSIZEUNIT'] = 'month'
        splits = calendar_chunk_section(self.experiment_data, "A", date, chunk)
        assert splits == 31
        splits = calendar_chunk_section(self.experiment_data, "B", date, chunk)
        assert splits == 16

        self.experiment_data['EXPERIMENT']['CHUNKSIZEUNIT'] = 'year'
        splits = calendar_chunk_section(self.experiment_data, "A", date, chunk)
        assert splits == 31
        splits = calendar_chunk_section(self.experiment_data, "B", date, chunk)
        assert splits == 16

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
                date_list=[datetime.strptime("20000101", "%Y%m%d")],
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
            assert 24 == len(job_list)

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
            assert job.splits == 12
            assert job.running == 'chunk'

            assert parameters['SPLIT'] == 1
            assert parameters['SPLITSIZE'] == splitsize
            assert parameters['SPLITSIZEUNIT'] == 'hour'
            assert parameters['SPLITSCALENDAR'] == 'standard'
            # assert parameters
            next_start = "00"
            for i, job in enumerate(job_list[0:12]):
                parameters = job.update_parameters(config, set_attributes=True)
                end_hour = str(parameters['SPLIT'] * splitsize).zfill(2)
                if end_hour == "24":
                    end_hour = "00"
                assert parameters['SPLIT'] == i + 1
                assert parameters['SPLITSIZE'] == splitsize
                assert parameters['SPLITSIZEUNIT'] == 'hour'
                assert parameters['SPLIT_START_DATE'] == '20000101'
                assert parameters['SPLIT_START_YEAR'] == '2000'
                assert parameters['SPLIT_START_MONTH'] == '01'
                assert parameters['SPLIT_START_DAY'] == '01'
                assert parameters['SPLIT_START_HOUR'] == next_start
                if parameters['SPLIT'] == 12:
                    assert parameters['SPLIT_END_DATE'] == '20000102'
                    assert parameters['SPLIT_END_DAY'] == '02'
                    assert parameters['SPLIT_END_DATE'] == '20000102'
                    assert parameters['SPLIT_END_DAY'] == '02'
                    assert parameters['SPLIT_END_YEAR'] == '2000'
                    assert parameters['SPLIT_END_MONTH'] == '01'
                    assert parameters['SPLIT_END_HOUR'] == end_hour
                else:
                    assert parameters['SPLIT_END_DATE'] == '20000101'
                    assert parameters['SPLIT_END_DAY'] == '01'
                    assert parameters['SPLIT_END_YEAR'] == '2000'
                    assert parameters['SPLIT_END_MONTH'] == '01'
                    assert parameters['SPLIT_END_HOUR'] == end_hour
                next_start = parameters['SPLIT_END_HOUR']
            next_start = "00"
            for i, job in enumerate(job_list[12:24]):
                parameters = job.update_parameters(config, set_attributes=True)
                end_hour = str(parameters['SPLIT'] * splitsize).zfill(2)
                if end_hour == "24":
                    end_hour = "00"
                assert parameters['SPLIT'] == i + 1
                assert parameters['SPLITSIZE'] == splitsize
                assert parameters['SPLITSIZEUNIT'] == 'hour'
                assert parameters['SPLIT_START_DATE'] == '20000105'
                assert parameters['SPLIT_START_YEAR'] == '2000'
                assert parameters['SPLIT_START_MONTH'] == '01'
                assert parameters['SPLIT_START_DAY'] == '05'
                assert parameters['SPLIT_START_HOUR'] == next_start
                if parameters['SPLIT'] == 12:
                    assert parameters['SPLIT_END_DATE'] == '20000106'
                    assert parameters['SPLIT_END_DAY'] == '06'
                    assert parameters['SPLIT_END_YEAR'] == '2000'
                    assert parameters['SPLIT_END_MONTH'] == '01'
                    assert parameters['SPLIT_END_HOUR'] == end_hour
                else:
                    assert parameters['SPLIT_END_DATE'] == '20000105'
                    assert parameters['SPLIT_END_DAY'] == '05'
                    assert parameters['SPLIT_END_YEAR'] == '2000'
                    assert parameters['SPLIT_END_MONTH'] == '01'
                    assert parameters['SPLIT_END_HOUR'] == end_hour
                next_start = parameters['SPLIT_END_HOUR']


# TODO: remove this and use pytest fixtures.
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


_EXPID = 't001'


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


@pytest.mark.parametrize(
    "file_name,job_name,expid,expected",
    [
        ("testfile.txt", "job1", "exp123", "testfile_job1"),
        ("exp123_testfile.txt", "job2", "exp123", "testfile_job2"),
        ("anotherfile.py", "job3", "exp999", "anotherfile_job3"),
    ]
)
def test_construct_real_additional_file_name(file_name: str, job_name: str, expid: str, expected: str) -> None:
    """
    Test the construct_real_additional_file_name method for various file name patterns.

    :param file_name: The input file name.
    :type file_name: str
    :param job_name: The job name to use.
    :type job_name: str
    :param expid: The experiment id to use.
    :type expid: str
    :param expected: The expected output file name.
    :type expected: str
    """
    job = Job(name=job_name)
    job.expid = expid
    result = job.construct_real_additional_file_name(file_name)
    assert result == expected


def test_create_script(mocker, tmpdir):
    # arrange
    job = Job("job1", "1", Status.READY, 0)
    # arrange
    parameters = dict()
    parameters['NUMPROC'] = 999
    parameters['NUMTHREADS'] = 777
    parameters['NUMTASK'] = 666

    job.name = "job1"
    job._tmp_path = tmpdir.strpath
    job.section = "DUMMY"
    job.additional_files = ['dummy_file1', 'dummy_file2']
    mocker.patch("autosubmit.job.job.Job.update_content", return_value=(
        'some-content: %NUMPROC%, %NUMTHREADS%, %NUMTASK% %% %%',
        ['some-content: %NUMPROC%, %NUMTHREADS%, %NUMTASK% %% %%',
         'some-content: %NUMPROC%, %NUMTHREADS%, %NUMTASK% %% %%']))
    mocker.patch("autosubmit.job.job.Job.update_parameters", return_value=parameters)

    config = Mock(spec=AutosubmitConfig)
    config.default_parameters = {}
    config.dynamic_variables = {}
    config.get_project_dir = Mock(return_value='/project/dir')
    name_without_expid = job.name.replace(f'{job.expid}_', '') if job.expid else job.name
    job.create_script(config)
    # list tmpdir and ensure that each file is created
    assert len(tmpdir.listdir()) == 3  # job script + additional files
    assert tmpdir.join('job1.cmd').check()
    assert tmpdir.join(f'dummy_file1_{name_without_expid}').check()
    assert tmpdir.join(f'dummy_file2_{name_without_expid}').check()
    # assert that the script content is correct
    with open(tmpdir.join('job1.cmd'), 'r') as f:
        content = f.read()
        assert 'some-content: 999, 777, 666' in content


def test_reset_logs(autosubmit_config):
    experiment_data = {
        'AUTOSUBMIT': {
            'WORKFLOW_COMMIT': "dummy-commit",
        },
    }
    as_conf = autosubmit_config("t000", experiment_data)
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


def create_job_and_update_parameters(autosubmit_config, experiment_data, platform_type="ps"):
    as_conf = autosubmit_config("t000", experiment_data)
    as_conf.experiment_data = as_conf.deep_normalize(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.normalize_variables(as_conf.experiment_data, must_exists=True)
    as_conf.experiment_data = as_conf.deep_read_loops(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.substitute_dynamic_variables(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.parse_data_loops(as_conf.experiment_data)
    # Create some jobs
    job = Job('A', '1', 0, 1)
    if platform_type == "ps":
        platform = PsPlatform(expid='t000', name='DUMMY_PLATFORM', config=as_conf.experiment_data)
    else:
        platform = SlurmPlatform(expid='t000', name='DUMMY_PLATFORM', config=as_conf.experiment_data)
    job.section = 'RANDOM-SECTION'
    job.platform = platform
    parameters = job.update_parameters(as_conf, set_attributes=True)
    return job, as_conf, parameters


@pytest.mark.parametrize('experiment_data, expected_data', [(
        {
            'JOBS': {
                'RANDOM-SECTION': {
                    'FILE': "test.sh",
                    'PLATFORM': 'DUMMY_PLATFORM',
                    'TEST': "%other%",
                },
            },
            'PLATFORMS': {
                'dummy_platform': {
                    'type': 'ps',
                    'whatever': 'dummy_value',
                    'whatever2': 'dummy_value2',
                    'CUSTOM_DIRECTIVES': ['$SBATCH directive1', '$SBATCH directive2'],
                },
            },
            'OTHER': "%CURRENT_WHATEVER%/%CURRENT_WHATEVER2%",
            'ROOTDIR': 'dummy_rootdir',
            'LOCAL_TMP_DIR': 'dummy_tmpdir',
            'LOCAL_ROOT_DIR': 'dummy_rootdir',
        },
        {
            'CURRENT_FILE': "test.sh",
            'CURRENT_PLATFORM': 'DUMMY_PLATFORM',
            'CURRENT_WHATEVER': 'dummy_value',
            'CURRENT_WHATEVER2': 'dummy_value2',
            'CURRENT_TEST': 'dummy_value/dummy_value2',

        }
)])
def test_update_parameters_current_variables(autosubmit_config, experiment_data, expected_data):
    _, _, parameters = create_job_and_update_parameters(autosubmit_config, experiment_data)
    for key, value in expected_data.items():
        assert parameters[key] == value


@pytest.mark.parametrize('test_with_file, file_is_empty, last_line_empty', [
    (False, False, False),
    (True, True, False),
    (True, False, False),
    (True, False, True)
], ids=["no file", "file is empty", "file is correct", "file last line is empty"])
def test_recover_last_ready_date(tmpdir, test_with_file, file_is_empty, last_line_empty):
    job = Job('dummy', '1', 0, 1)
    job._tmp_path = Path(tmpdir)
    stat_file = job._tmp_path.joinpath(f'{job.name}_TOTAL_STATS')
    ready_time = datetime.now() + timedelta(minutes=5)
    ready_date = int(ready_time.strftime("%Y%m%d%H%M%S"))
    expected_date = None
    if test_with_file:
        if file_is_empty:
            stat_file.touch()
            expected_date = datetime.fromtimestamp(stat_file.stat().st_mtime).strftime('%Y%m%d%H%M%S')
        else:
            if last_line_empty:
                with stat_file.open('w') as f:
                    f.write(" ")
                expected_date = datetime.fromtimestamp(stat_file.stat().st_mtime).strftime('%Y%m%d%H%M%S')
            else:
                with stat_file.open('w') as f:
                    f.write(f"{ready_date} {ready_date} {ready_date} COMPLETED")
                expected_date = str(ready_date)
    job.ready_date = None
    job.recover_last_ready_date()
    assert job.ready_date == expected_date


@pytest.mark.parametrize('test_with_logfiles, file_timestamp_greater_than_ready_date', [
    (False, False),
    (True, True),
    (True, False),
], ids=["no file", "log timestamp >= ready_date", "log timestamp < ready_date"])
def test_recover_last_log_name(tmpdir, test_with_logfiles, file_timestamp_greater_than_ready_date):
    job = Job('dummy', '1', 0, 1)
    job._log_path = Path(tmpdir)
    expected_local_logs = (f"{job.name}.out.0", f"{job.name}.err.0")
    if test_with_logfiles:
        if file_timestamp_greater_than_ready_date:
            ready_time = datetime.now() - timedelta(minutes=5)
            job.ready_date = str(ready_time.strftime("%Y%m%d%H%M%S"))
            log_name = job._log_path.joinpath(f'{job.name}_{job.ready_date}')
            expected_update_log = True
            expected_local_logs = (log_name.with_suffix('.out').name, log_name.with_suffix('.err').name)
        else:
            expected_update_log = False
            ready_time = datetime.now() + timedelta(minutes=5)
            job.ready_date = str(ready_time.strftime("%Y%m%d%H%M%S"))
            log_name = job._log_path.joinpath(f'{job.name}_{job.ready_date}')
        log_name.with_suffix('.out').touch()
        log_name.with_suffix('.err').touch()
    else:
        expected_update_log = False

    job.updated_log = False
    job.recover_last_log_name()
    assert job.updated_log == expected_update_log
    assert job.local_logs[0] == str(expected_local_logs[0])
    assert job.local_logs[1] == str(expected_local_logs[1])


@pytest.mark.parametrize('experiment_data, attributes_to_check', [(
        {
            'JOBS': {
                'RANDOM-SECTION': {
                    'FILE': "test.sh",
                    'PLATFORM': 'DUMMY_PLATFORM',
                    'NOTIFY_ON': 'COMPLETED',
                },
            },
            'PLATFORMS': {
                'dummy_platform': {
                    'type': 'ps',
                },
            },
            'ROOTDIR': 'dummy_rootdir',
            'LOCAL_TMP_DIR': 'dummy_tmpdir',
            'LOCAL_ROOT_DIR': 'dummy_rootdir',
        },
        {'notify_on': ['COMPLETED']}
)])
def test_update_parameters_attributes(autosubmit_config, experiment_data, attributes_to_check):
    job, _, _ = create_job_and_update_parameters(autosubmit_config, experiment_data)
    for attr in attributes_to_check:
        assert hasattr(job, attr)
        assert getattr(job, attr) == attributes_to_check[attr]


@pytest.mark.parametrize('custom_directives, test_type, result_by_lines', [
    ("test_str a", "platform", ["test_str a"]),
    (['test_list', 'test_list2'], "platform", ['test_list', 'test_list2']),
    (['test_list', 'test_list2'], "job", ['test_list', 'test_list2']),
    ("test_str", "job", ["test_str"]),
    (['test_list', 'test_list2'], "both", ['test_list', 'test_list2']),
    ("test_str", "both", ["test_str"]),
    (['test_list', 'test_list2'], "current_directive", ['test_list', 'test_list2']),
    ("['test_str_list', 'test_str_list2']", "job", ['test_str_list', 'test_str_list2']),
], ids=["Test str - platform", "test_list - platform", "test_list - job", "test_str - job", "test_list - both",
        "test_str - both", "test_list - job - current_directive", "test_str_list - current_directive"])
def test_custom_directives(tmpdir, custom_directives, test_type, result_by_lines, mocker, autosubmit_config):
    file_stat = os.stat(f"{tmpdir.strpath}")
    file_owner_id = file_stat.st_uid
    tmpdir.owner = pwd.getpwuid(file_owner_id).pw_name
    tmpdir_path = Path(tmpdir.strpath)
    project = "whatever"
    user = tmpdir.owner
    scratch_dir = f"{tmpdir.strpath}/scratch"
    full_path = f"{scratch_dir}/{project}/{user}"
    experiment_data = {
        'JOBS': {
            'RANDOM-SECTION': {
                'SCRIPT': "echo 'Hello World!'",
                'PLATFORM': 'DUMMY_PLATFORM',
            },
        },
        'PLATFORMS': {
            'dummy_platform': {
                "type": "slurm",
                "host": "127.0.0.1",
                "user": f"{user}",
                "project": f"{project}",
                "scratch_dir": f"{scratch_dir}",
                "QUEUE": "gp_debug",
                "ADD_PROJECT_TO_HOST": False,
                "MAX_WALLCLOCK": "48:00",
                "TEMP_DIR": "",
                "MAX_PROCESSORS": 99999,
                "PROCESSORS_PER_NODE": 123,
                "DISABLE_RECOVERY_THREADS": True
            },
        },
        'ROOTDIR': f"{full_path}",
        'LOCAL_TMP_DIR': f"{full_path}",
        'LOCAL_ROOT_DIR': f"{full_path}",
        'LOCAL_ASLOG_DIR': f"{full_path}",
    }
    tmpdir_path.joinpath(f"{scratch_dir}/{project}/{user}").mkdir(parents=True)

    if test_type == "platform":
        experiment_data['PLATFORMS']['dummy_platform']['CUSTOM_DIRECTIVES'] = custom_directives
    elif test_type == "job":
        experiment_data['JOBS']['RANDOM-SECTION']['CUSTOM_DIRECTIVES'] = custom_directives
    elif test_type == "both":
        experiment_data['PLATFORMS']['dummy_platform']['CUSTOM_DIRECTIVES'] = custom_directives
        experiment_data['JOBS']['RANDOM-SECTION']['CUSTOM_DIRECTIVES'] = custom_directives
    elif test_type == "current_directive":
        experiment_data['PLATFORMS']['dummy_platform']['APP_CUSTOM_DIRECTIVES'] = custom_directives
        experiment_data['JOBS']['RANDOM-SECTION']['CUSTOM_DIRECTIVES'] = "%CURRENT_APP_CUSTOM_DIRECTIVES%"
    job, as_conf, parameters = create_job_and_update_parameters(autosubmit_config, experiment_data, "slurm")
    mocker.patch('autosubmitconfigparser.config.configcommon.AutosubmitConfig.reload')
    template_content, _ = job.update_content(as_conf, parameters)
    for directive in result_by_lines:
        pattern = r'^\s*' + re.escape(directive) + r'\s*$'  # Match Start line, match directive, match end line
        assert re.search(pattern, template_content, re.MULTILINE) is not None


@pytest.mark.parametrize('experiment_data', [(
        {
            'JOBS': {
                'RANDOM-SECTION': {
                    'FILE': "test.sh",
                    'PLATFORM': 'DUMMY_PLATFORM',
                    'TEST': "rng",
                },
            },
            'PLATFORMS': {
                'dummy_platform': {
                    'type': 'ps',
                    'whatever': 'dummy_value',
                    'whatever2': 'dummy_value2',
                    'CUSTOM_DIRECTIVES': ['$SBATCH directive1', '$SBATCH directive2'],
                },
            },
            'ROOTDIR': "asd",
            'LOCAL_TMP_DIR': "asd",
            'LOCAL_ROOT_DIR': "asd",
            'LOCAL_ASLOG_DIR': "asd",
        }
)], ids=["Simple job"])
def test_no_start_time(autosubmit_config, experiment_data):
    job, as_conf, parameters = create_job_and_update_parameters(autosubmit_config, experiment_data)
    del job.start_time
    as_conf.force_load = False
    as_conf.data_changed = False
    job.update_parameters(as_conf, set_attributes=True)
    assert isinstance(job.start_time, datetime)


def test_get_job_package_code(autosubmit_config):
    autosubmit_config('dummy', {})
    experiment_id = 'dummy'
    job = Job(experiment_id, '1', 0, 1)

    with patch("autosubmit.job.job_utils.JobPackagePersistence") as mock_persistence:
        mock_persistence.return_value.load.return_value = [
            ['dummy', '0005_job_packages', 'dummy']
        ]
        code = get_job_package_code(job.expid, job.name)

        assert code == 5


def test_sub_job_instantiation(tmp_path, autosubmit_config):
    job = SubJob("dummy", package=None, queue=0, run=0, total=0, status="UNKNOWN")

    assert job.name == "dummy"
    assert job.package is None
    assert job.queue == 0
    assert job.run == 0
    assert job.total == 0
    assert job.status == "UNKNOWN"


@pytest.mark.parametrize("current_structure",
                         [
                             ({
                                 'dummy2':
                                     {'dummy', 'dummy1', 'dummy4'},
                                 'dummy3':
                                     'dummy'
                             }),
                             ({}),
                         ],
                         ids=["Current structure of the Job Manager with multiple values",
                              "Current structure of the Job Manager without values"]
                         )
def test_sub_job_manager(current_structure):
    """
    tester of the function _sub_job_manager
    """
    jobs = {
        SubJob("dummy", package="test2", queue=0, run=1, total=30, status="UNKNOWN"),
        SubJob("dummy", package=["test4", "test1", "test2", "test3"], queue=1,
               run=2, total=10, status="UNKNOWN"),
        SubJob("dummy2", package="test2", queue=2, run=3, total=100, status="UNKNOWN"),
        SubJob("dummy", package="test3", queue=3, run=4, total=1000, status="UNKNOWN"),
    }

    job_to_package = {
        'dummy test'
    }

    package_to_job = {
        'test':
            {'dummy', 'dummy2'},
        'test2':
            {'dummy', 'dummy2'},
        'test3':
            {'dummy', 'dummy2'}
    }

    job_manager = SubJobManager(jobs, job_to_package, package_to_job, current_structure)
    job_manager.process_index()
    job_manager.process_times()

    print(type(job_manager.get_subjoblist()))

    assert job_manager is not None and type(job_manager) is SubJobManager
    assert job_manager.get_subjoblist() is not None and type(job_manager.get_subjoblist()) is set
    assert job_manager.subjobindex is not None and type(job_manager.subjobindex) is dict
    assert job_manager.subjobfixes is not None and type(job_manager.subjobfixes) is dict
    assert (job_manager.get_collection_of_fixes_applied() is not None
            and type(job_manager.get_collection_of_fixes_applied()) is dict)


def test_update_parameters_reset_logs(autosubmit_config, tmpdir):
    # TODO This experiment_data (aside from WORKFLOW_COMMIT and maybe JOBS) could be a good candidate for a fixture in the conf_test. "basic functional configuration"
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data={
            'AUTOSUBMIT': {'WORKFLOW_COMMIT': 'dummy'},
            'PLATFORMS': {'DUMMY_P': {'TYPE': 'ps'}},
            'JOBS': {'DUMMY_S': {'FILE': 'dummy.sh', 'PLATFORM': 'DUMMY_P'}},
            'DEFAULT': {'HPCARCH': 'DUMMY_P'},
        }
    )
    job = Job('DUMMY', '1', 0, 1)
    job.section = 'DUMMY_S'
    job.log_recovered = True
    job.packed_during_building = True
    job.workflow_commit = "incorrect"
    job.update_parameters(as_conf, set_attributes=True, reset_logs=True)
    assert job.workflow_commit == "dummy"


# NOTE: These tests were migrated from ``test/integration/test_job.py``.

def _create_relationship(parent, child):
    parent.children.add(child)
    child.parents.add(parent)


@pytest.fixture
def integration_jobs():
    """The name of this function has "integration" because it was in the folder of integration tests."""
    jobs = list()
    jobs.append(Job('whatever', 0, Status.UNKNOWN, 0))
    jobs.append(Job('whatever', 1, Status.UNKNOWN, 0))
    jobs.append(Job('whatever', 2, Status.UNKNOWN, 0))
    jobs.append(Job('whatever', 3, Status.UNKNOWN, 0))
    jobs.append(Job('whatever', 4, Status.UNKNOWN, 0))

    _create_relationship(jobs[0], jobs[1])
    _create_relationship(jobs[0], jobs[2])
    _create_relationship(jobs[1], jobs[3])
    _create_relationship(jobs[1], jobs[4])
    _create_relationship(jobs[2], jobs[3])
    _create_relationship(jobs[2], jobs[4])
    return jobs


def test_is_ancestor_works_well(integration_jobs):
    check_ancestors_array(integration_jobs[0], [False, False, False, False, False], integration_jobs)
    check_ancestors_array(integration_jobs[1], [False, False, False, False, False], integration_jobs)
    check_ancestors_array(integration_jobs[2], [False, False, False, False, False], integration_jobs)
    check_ancestors_array(integration_jobs[3], [True, False, False, False, False], integration_jobs)
    check_ancestors_array(integration_jobs[4], [True, False, False, False, False], integration_jobs)


def test_is_parent_works_well(integration_jobs):
    _check_parents_array(integration_jobs[0], [False, False, False, False, False], integration_jobs)
    _check_parents_array(integration_jobs[1], [True, False, False, False, False], integration_jobs)
    _check_parents_array(integration_jobs[2], [True, False, False, False, False], integration_jobs)
    _check_parents_array(integration_jobs[3], [False, True, True, False, False], integration_jobs)
    _check_parents_array(integration_jobs[4], [False, True, True, False, False], integration_jobs)


def test_remove_redundant_parents_works_well(integration_jobs):
    # Adding redundant relationships
    _create_relationship(integration_jobs[0], integration_jobs[3])
    _create_relationship(integration_jobs[0], integration_jobs[4])
    # Checking there are redundant parents
    assert len(integration_jobs[3].parents) == 3
    assert len(integration_jobs[4].parents) == 3


def check_ancestors_array(job, assertions, jobs):
    for assertion, jobs_job in zip(assertions, jobs):
        assert assertion == job.is_ancestor(jobs_job)


def _check_parents_array(job, assertions, jobs):
    for assertion, jobs_job in zip(assertions, jobs):
        assert assertion == job.is_parent(jobs_job)


@pytest.mark.parametrize(
    "file_exists, index_timestamp, fail_count, expected",
    [
        (True, 0, None, 19704923),
        (True, 1, None, 19704924),
        (True, 0, 0, 19704923),
        (True, 0, 1, 29704923),
        (True, 1, 0, 19704924),
        (True, 1, 1, 29704924),
        (False, 0, None, 0),
        (False, 1, None, 0),
        (False, 0, 0, 0),
        (False, 0, 1, 0),
        (False, 1, 0, 0),
        (False, 1, 1, 0),
    ],
    ids=[
        "File exists, index_timestamp=0",
        "File exists, index_timestamp=1",
        "File exists, index_timestamp=0, fail_count=0",
        "File exists, index_timestamp=0, fail_count=1",
        "File exists, index_timestamp=1, fail_count=0",
        "File exists, index_timestamp=1, fail_count=1",
        "File does not exist, index_timestamp=0",
        "File does not exist, index_timestamp=1",
        "File does not exist, index_timestamp=0, fail_count=0",
        "File does not exist, index_timestamp=0, fail_count=1",
        "File does not exist, index_timestamp=1, fail_count=0",
        "File does not exist, index_timestamp=1, fail_count=1",
    ],
)
def test_get_from_stat(tmpdir, file_exists, index_timestamp, fail_count, expected):
    job = Job("dummy", 1, Status.WAITING, 0)
    assert job.stat_file == f"{job.name}_STAT_"
    job._tmp_path = Path(tmpdir)
    job._tmp_path.mkdir(parents=True, exist_ok=True)

    # Generating the timestamp file
    if file_exists:
        with open(job._tmp_path.joinpath(f"{job.stat_file}0"), "w") as stat_file:
            stat_file.write("19704923\n19704924\n")
        with open(job._tmp_path.joinpath(f"{job.stat_file}1"), "w") as stat_file:
            stat_file.write("29704923\n29704924\n")

    if fail_count is None:
        result = job._get_from_stat(index_timestamp)
    else:
        result = job._get_from_stat(index_timestamp, fail_count)

    assert result == expected


@pytest.mark.parametrize(
    'total_stats_exists',
    [
        True,
        False
    ]
)
def test_write_submit_time_ignore_exp_history(total_stats_exists: bool, autosubmit_config, local, mocker):
    """Test that the job writes the submit time correctly.

    It ignores what happens to the experiment history object."""
    mocker.patch('autosubmit.job.job.ExperimentHistory')

    as_conf = autosubmit_config(_EXPID, experiment_data={})
    tmp_path = Path(as_conf.basic_config.LOCAL_ROOT_DIR, _EXPID, as_conf.basic_config.LOCAL_TMP_DIR)

    job = Job(f'{_EXPID}_dummy', 1, Status.WAITING, 0)
    job.submit_time_timestamp = date2str(datetime.now(), 'S')
    job.platform = local

    total_stats = Path(tmp_path, f'{job.name}_TOTAL_STATS')
    if total_stats_exists:
        total_stats.touch()
        total_stats.write_text('First line')

    job.write_submit_time()

    # It will exist regardless of the argument ``total_stats_exists``, as ``write_submit_time()``
    # must have created it.
    assert total_stats.exists()

    # When the file already exists, it will append a new line. Otherwise,
    # a new file is created with a single line.
    expected_lines = 2 if total_stats_exists else 1
    assert len(total_stats.read_text().split('\n')) == expected_lines


@pytest.mark.parametrize(
    'completed,existing_lines,count',
    [
        (True, 'a\nb\n', -1),
        (True, None, -1),
        (False, 'a\n', -1),
        (False, None, 100)
    ],
    ids=[
        'job completed, two existing lines, no count',
        'job completed, empty file, no count',
        'job failed, one existing line, no count',
        'job failed, empty file, count is 100'
    ]
)
def test_write_end_time_ignore_exp_history(completed: bool, existing_lines: str, local, count: int,
                                           autosubmit_config, mocker):
    """Test that the job writes the end time correctly.

    It ignores what happens to the experiment history object."""
    mocker.patch('autosubmit.job.job.ExperimentHistory')

    as_conf = autosubmit_config(_EXPID, experiment_data={})
    tmp_path = Path(as_conf.basic_config.LOCAL_ROOT_DIR, _EXPID, as_conf.basic_config.LOCAL_TMP_DIR)

    status = Status.COMPLETED if True else Status.WAITING
    job = Job(f'{_EXPID}_dummy', 1, status, 0)
    job.finish_time_timestamp = time()
    job.platform = local

    total_stats = Path(tmp_path, f'{job.name}_TOTAL_STATS')
    if existing_lines:
        total_stats.touch()
        total_stats.write_text(existing_lines)

    job.write_end_time(completed=completed, count=count)

    # It will exist regardless of the argument ``total_stats_exists``, as ``write_submit_time()``
    # must have created it.
    assert total_stats.exists()

    # When the file already exists, it will append new content. It must never
    # delete the existing lines, so this assertion just verifies the content
    # written previously (if any) was not removed.
    existing_lines = len(existing_lines.split('\n')) - 1 if existing_lines else 0
    expected_lines = existing_lines + 1
    assert len(total_stats.read_text().split('\n')) == expected_lines
