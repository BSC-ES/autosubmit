from unittest import TestCase

from autosubmit.helpers.parameters import (
    autosubmit_parameter,
    autosubmit_parameters,
    PARAMETERS
)


class TestParameters(TestCase):
    """Tests for the ``helpers.parameters`` module."""

    def test_autosubmit_decorator(self):
        """Test the ``autosubmit_decorator``."""

        parameter_name = 'JOBNAME'
        parameter_group = 'PLATFORM'

        class Job:
            @property
            @autosubmit_parameter(name=parameter_name, group=parameter_group)
            def name(self):
                """This parameter is the job name."""
                return 'FOO'

        job = Job()

        self.assertEqual('FOO', job.name)
        self.assertTrue(len(PARAMETERS) > 0)
        # Defaults to the module name if not provided! So the class name
        # ``Job`` becomes ``JOB``.
        self.assertTrue(parameter_group in PARAMETERS)
        self.assertTrue(parameter_name in PARAMETERS[parameter_group])
        self.assertEqual('This parameter is the job name.', PARAMETERS[parameter_group][parameter_name])


    def test_autosubmit_decorator_using_array(self):
        """Test the ``autosubmit_decorator``."""

        parameter_names = ['JOBNAME', 'JOB____NAME']
        parameter_group = 'PLATFORM'

        class Job:
            @property
            @autosubmit_parameter(name=parameter_names, group=parameter_group)
            def name(self):
                """This parameter is the job name."""
                return 'FOO'

        job = Job()

        self.assertEqual('FOO', job.name)
        self.assertTrue(len(PARAMETERS) > 0)
        # Defaults to the module name if not provided! So the class name
        # ``Job`` becomes ``JOB``.
        self.assertTrue(parameter_group in PARAMETERS)
        for parameter_name in parameter_names:
            self.assertTrue(parameter_name in PARAMETERS[parameter_group])
            self.assertEqual('This parameter is the job name.', PARAMETERS[parameter_group][parameter_name])


    def test_autosubmit_decorator_no_group(self):
        """Test the ``autosubmit_decorator`` when ``group`` is not provided."""

        parameter_name = 'JOBNAME'

        class Job:
            @property
            @autosubmit_parameter(name=parameter_name)
            def name(self):
                """This parameter is the job name."""
                return 'FOO'

        job = Job()

        self.assertEqual('FOO', job.name)
        self.assertTrue(len(PARAMETERS) > 0)
        # Defaults to the module name if not provided! So the class name
        # ``Job`` becomes ``JOB``.
        self.assertTrue(Job.__name__.upper() in PARAMETERS)
        self.assertTrue(parameter_name in PARAMETERS[Job.__name__.upper()])
        self.assertEqual('This parameter is the job name.', PARAMETERS[Job.__name__.upper()][parameter_name])


    def test_autosubmit_class_decorator(self):
        """Test the ``autosubmit_decorator`` when ``group`` is not provided."""

        @autosubmit_parameters(parameters={
            'job': {
                'JOBNAME': 'The value!'
            }
        })
        class Job:
            @property
            def name(self):
                """This parameter is the job name."""
                return 'FOO'

        job = Job()

        self.assertEqual('FOO', job.name)
        self.assertTrue(len(PARAMETERS) > 0)
        self.assertTrue('JOB' in PARAMETERS)
        self.assertTrue('JOBNAME' in PARAMETERS['JOB'])
        self.assertEqual('The value!', PARAMETERS['JOB']['JOBNAME'])


