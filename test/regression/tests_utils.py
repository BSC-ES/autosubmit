import os
import subprocess

from autosubmit.experiment.experiment_common import base36decode, base36encode
from .tests_commands import create_database_cmd

BIN_PATH = '../../bin'


def next_experiment_id(current_id):
    return base36encode(base36decode(current_id) + 1)


def check_cmd(command, path=BIN_PATH, verbose='AS_TEST_VERBOSE' in os.environ):
    try:
        output = subprocess.check_output(os.path.join(path, command), shell=True, stderr=subprocess.STDOUT)
        if verbose:
            print(output)

        if 'CRITICAL' in output or 'ERROR' in output:
            return False

        return True

    except subprocess.CalledProcessError as e:
        if verbose:
            print(e.output)
        return False


def copy_experiment_conf_files(db_path, src_path, experiment_id):
    check_cmd(get_copy_cmd(db_path, src_path, 'autosubmit', experiment_id), '')
    check_cmd(get_copy_cmd(db_path, src_path, 'expdef', experiment_id), '')
    check_cmd(get_copy_cmd(db_path, src_path, 'jobs', experiment_id), '')
    if os.path.exists(get_conf_file_path(src_path, 'platforms')):
        check_cmd(get_copy_cmd(db_path, src_path, 'platforms', experiment_id), '')
    else:
        check_cmd(get_default_copy_cmd(db_path, 'platforms', experiment_id), '')
    check_cmd(get_copy_cmd(db_path, src_path, 'proj', experiment_id), '')
    check_cmd(get_replace_exp_id(experiment_id) + os.path.join(db_path, experiment_id, 'conf', '*'), '', )
    check_cmd(get_replace_project_path(src_path) + os.path.join(db_path, experiment_id, 'conf', '*'), '', )


def clean_database(db_path):
    check_cmd('rm -rf ' + os.path.join(db_path, '*'), '', 'AS_TEST_VERBOSE' in os.environ)


def create_database():
    check_cmd(create_database_cmd())


def get_replace_exp_id(experiment_id):
    return "sed -i -- 's/EXPID-HERE/" + experiment_id + "/g' "


def get_replace_project_path(src_path):
    current_path = os.getcwd().replace('/', '\/')
    src_path = src_path.replace('/', '\/')
    return "sed -i -- 's/PROJECT-PATH-HERE/" + current_path + '\/' + src_path + '\/' + 'src' + "/g' "


def get_copy_cmd(db_path, src_path, filename, experiment_id):
    return 'cp ' + get_conf_file_path(src_path, filename) + ' ' + \
           get_conf_file_path(os.path.join(db_path, experiment_id), filename + '_' + experiment_id)


def get_default_copy_cmd(db_path, filename, experiment_id):
    return 'cp ' + os.path.join('default_conf', filename + '.conf') + ' ' + \
           get_conf_file_path(os.path.join(db_path, experiment_id), filename + '_' + experiment_id)


def get_conf_file_path(base_path, filename):
    return os.path.join(base_path, 'conf', filename + '.conf')
