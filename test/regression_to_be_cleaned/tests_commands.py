import os


def generate_experiment_cmd(hpc, description):
    return 'autosubmit expid -H ' + hpc + ' -d ' + description


def create_experiment_cmd(experiment_id):
    return 'autosubmit -lf EVERYTHING -lc EVERYTHING create {0} --hide'.format(experiment_id)


def run_experiment_cmd(experiment_id):
    return 'autosubmit -lf EVERYTHING -lc EVERYTHING run {0}'.format(experiment_id)


def monitor_experiment_cmd(experiment_id):
    return 'autosubmit -lf EVERYTHING -lc EVERYTHING monitor {0} --hide'.format(experiment_id)


def refresh_experiment_cmd(experiment_id):
    return 'autosubmit -lf EVERYTHING -lc EVERYTHING refresh {0}'.format(experiment_id)


def recovery_experiment_cmd(experiment_id):
    return 'autosubmit -lf EVERYTHING -lc EVERYTHING recovery {0} --all --hide -s'.format(experiment_id)


def check_experiment_cmd(experiment_id):
    return 'autosubmit -lf EVERYTHING -lc EVERYTHING check {0}'.format(experiment_id)


def stats_experiment_cmd(experiment_id):
    return 'autosubmit -lf EVERYTHING -lc EVERYTHING stats {0} --hide'.format(experiment_id)


def create_database_cmd():
    return 'autosubmit install'
