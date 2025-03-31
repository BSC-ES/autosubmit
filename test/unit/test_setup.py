import os
from unittest import TestCase


class TestSetup(TestCase):
    def test_setup_check_works(self):
        exit_code = run_setup_check()
        self.assertEquals(0, exit_code)


def run_setup_check():
    return os.system('python2.7 ' + get_directory_of_this_file() + '/../../setup.py check')


def get_directory_of_this_file():
    return os.path.dirname(os.path.realpath(__file__))
