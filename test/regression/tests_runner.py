from .tests_log import Log
from .tests_utils import check_cmd, next_experiment_id, copy_experiment_conf_files, create_database, clean_database
from .tests_commands import (
    generate_experiment_cmd,
    create_experiment_cmd,
    check_experiment_cmd,
    monitor_experiment_cmd,
    refresh_experiment_cmd,
    run_experiment_cmd,
    stats_experiment_cmd,
    recovery_experiment_cmd,
)
from threading import Thread
from time import sleep
import argparse
try:
    # noinspection PyCompatibility
    from configparser import SafeConfigParser
except ImportError:
    # noinspection PyCompatibility
    from configparser import SafeConfigParser

# Configuration file where the regression tests are defined with INI style
tests_parser_file = 'tests.conf'

# Path where the temporal files (db, experiments, etc) fill be saved
db_path = './db'

# Initial experiment id
initial_experiment_id = 'a000'


def run_test_case(experiment_id, name, hpc_arch, description, src_path, retrials=2):
    if not check_cmd(generate_experiment_cmd(hpc_arch, description)):
        Log.error('Error while generating the experiment {0}({1})', name, experiment_id)
        return False
    copy_experiment_conf_files(db_path, src_path, experiment_id)
    sleep(5)  # Avoiding synchronization problems while copying

    run_ok = False
    for attempt in range(retrials):
        if not check_cmd(create_experiment_cmd(experiment_id)):
            Log.warning('Error while creating the experiment {0}({1})', name, experiment_id)
            continue

        if not check_cmd(check_experiment_cmd(experiment_id)):
            Log.warning('Error while checking the experiment {0}({1})', name, experiment_id)
            continue

        if not check_cmd(monitor_experiment_cmd(experiment_id)):
            Log.warning('Error while monitoring the experiment {0}({1})', name, experiment_id)
            continue

        if not check_cmd(refresh_experiment_cmd(experiment_id)):
            Log.warning('Error while refreshing the experiment {0}({1})', name, experiment_id)
            continue

        if not check_cmd(run_experiment_cmd(experiment_id)):
            Log.warning('Error while running the experiment {0}({1})', name, experiment_id)
            continue

        if not check_cmd(monitor_experiment_cmd(experiment_id)):
            Log.warning('Error while monitoring the experiment {0}({1})', name, experiment_id)
            continue

        if not check_cmd(stats_experiment_cmd(experiment_id)):
            Log.warning('Error while getting stats of the experiment {0}({1})', name, experiment_id)
            continue

        if not check_cmd(recovery_experiment_cmd(experiment_id)):
            Log.warning('Error while recovering the experiment {0}({1})', name, experiment_id)
            continue

        run_ok = True
        break
    if run_ok:
        Log.result('[OK] Test {0}({1}) has been passed successfully', name, experiment_id)
    else:
        Log.error('[KO] Test {0}({1}) has not been passed successfully', name, experiment_id)


def run(current_experiment_id, only_list=None, exclude_list=None, max_threads=5):
    # Local variables for testing
    test_threads = []

    # Building tests parser
    tests_parser = SafeConfigParser()
    tests_parser.optionxform = str
    tests_parser.read(tests_parser_file)

    # Resetting the database
    clean_database(db_path)
    create_database()

    # Main loop to run all the tests
    for section in tests_parser.sections():
        # Skipping filtered experiments
        if only_list is not None and section not in only_list:
            Log.warning('Test {0} has been skipped', section)
            continue

        if exclude_list is not None and section in exclude_list:
            Log.warning('Test {0} has been skipped', section)
            continue

        # Prevents too many concurrent threads
        if len(test_threads) >= max_threads:
            test_threads.pop(0).join()

        # Getting test settings
        description, hpc_arch, src_path = get_test_settings(section, tests_parser)

        # Running the test as a new thread
        test_threads.append(create_test_thread(current_experiment_id, section, description, hpc_arch, src_path))

        # Updating current experiment id
        current_experiment_id = next_experiment_id(current_experiment_id)

        # Avoiding synchronization problems
        sleep(10)

    # Loop to wait the end of all the running tests
    for test_thread in test_threads:
        test_thread.join()


def create_test_thread(current_experiment_id, name, description, hpc_arch, src_path):
    thr = Thread(target=run_test_case, args=(current_experiment_id, name, hpc_arch, description, src_path), kwargs={})
    thr.start()
    return thr


def get_test_settings(section, tests_parser):
    hpc_arch = tests_parser.get(section, 'HPCARCH')
    description = tests_parser.get(section, 'DESCRIPTION')
    src_path = tests_parser.get(section, 'SRC_PATH')
    return description, hpc_arch, src_path


def create_args_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", type=str,
                        help="List of experiments to be run, test names separated by white spaces")
    parser.add_argument("--exclude", type=str,
                        help="List of experiments to be avoided, test names separated by white spaces")
    return parser


if __name__ == "__main__":
    args_parser = create_args_parser()

    only = None if args_parser.parse_args().only is None else args_parser.parse_args().only.split()
    exclude = None if args_parser.parse_args().exclude is None else args_parser.parse_args().exclude.split()

    run(initial_experiment_id, only, exclude)
