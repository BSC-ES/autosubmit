import os


#class TestSetup(TestCase):
    #def test_setup_check_works(self):
    #    exit_code = run_setup_check()
    #    self.assertEqual(0, exit_code)


def run_setup_check():
    return os.system('python ' + get_directory_of_this_file() + '/../../setup.py check')


def get_directory_of_this_file():
    return os.path.dirname(os.path.realpath(__file__))
