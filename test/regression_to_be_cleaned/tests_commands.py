

def generate_experiment_cmd(hpc, description):
    return f'autosubmit expid -H {hpc} -d {description}'


def create_experiment_cmd(experiment_id):
    return f'autosubmit -lf EVERYTHING -lc EVERYTHING create {experiment_id} --hide'


def run_experiment_cmd(experiment_id):
    return f'autosubmit -lf EVERYTHING -lc EVERYTHING run {experiment_id}'


def monitor_experiment_cmd(experiment_id):
    return f'autosubmit -lf EVERYTHING -lc EVERYTHING monitor {experiment_id} --hide'


def refresh_experiment_cmd(experiment_id):
    return f'autosubmit -lf EVERYTHING -lc EVERYTHING refresh {experiment_id}'


def recovery_experiment_cmd(experiment_id):
    return f'autosubmit -lf EVERYTHING -lc EVERYTHING recovery {experiment_id} --all --hide -s'


def check_experiment_cmd(experiment_id):
    return f'autosubmit -lf EVERYTHING -lc EVERYTHING check {experiment_id}'


def stats_experiment_cmd(experiment_id):
    return f'autosubmit -lf EVERYTHING -lc EVERYTHING stats {experiment_id} --hide'


def create_database_cmd():
    return 'autosubmit install'
