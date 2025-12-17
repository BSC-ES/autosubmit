import pytest
import os
from ruamel.yaml import YAML

@pytest.fixture()
def jobs_data():
    return {"JOBS": {
            "LOCAL_SETUP": {
                "FILE": "LOCAL_SETUP.sh",
                "PLATFORM": "LOCAL",
                "RUNNING": "once",
                "DEPENDENCIES": {},
                "ADDITIONAL_FILES": [],
            },
            "REMOTE_SETUP": {
                "FILE": "REMOTE_SETUP.sh",
                "PLATFORM": "LOCAL",
                "DEPENDENCIES": {"LOCAL_SETUP": {}},
                "WALLCLOCK": "00:05",
                "RUNNING": "once",
                "ADDITIONAL_FILES": [],
            },
            "INI": {
                "FILE": "INI.sh",
                "PLATFORM": "LOCAL",
                "DEPENDENCIES": {"REMOTE_SETUP": {}},
                "RUNNING": "member",
                "WALLCLOCK": "00:05",
                "ADDITIONAL_FILES": [],
            },
            "SIM": {
                "FILE": "SIM.sh",
                "PLATFORM": "LOCAL",
                "DEPENDENCIES": {"INI": {}, "SIM-1": {}},
                "RUNNING": "chunk",
                "WALLCLOCK": "00:05",
                "ADDITIONAL_FILES": [],
            },
            "POST": {
                "FILE": "POST.sh",
                "PLATFORM": "LOCAL",
                "DEPENDENCIES": {"SIM": {}},
                "RUNNING": "once",
                "WALLCLOCK": "00:05",
                "ADDITIONAL_FILES": [],
            },
            "CLEAN": {
                "FILE": "CLEAN.sh",
                "PLATFORM": "LOCAL",
                "DEPENDENCIES": {"POST": {}},
                "RUNNING": "once",
                "WALLCLOCK": "00:05",
                "ADDITIONAL_FILES": [],
            },
            "TRANSFER": {
                "FILE": "TRANSFER.sh",
                "PLATFORM": "LOCAL",
                "DEPENDENCIES": {"CLEAN": {}},
                "RUNNING": "member",
                "ADDITIONAL_FILES": [],
            },
        }}


@pytest.fixture()
def expdef_data():
    expid = "dummy-id"
    return {
        "DEFAULT": {"EXPID": f"{expid}", "HPCARCH": "LOCAL"},
        "EXPERIMENT": {
            "DATELIST": "20000101",
            "MEMBERS": "fc0",
            "CHUNKSIZEUNIT": "month",
            "CHUNKSIZE": 4,
            "NUMCHUNKS": 2,
            "CHUNKINI": "",
            "CALENDAR": "standard",
        },
        "PROJECT": {"PROJECT_TYPE": "none", "PROJECT_DESTINATION": ""},
        "GIT": {
            "PROJECT_ORIGIN": "",
            "PROJECT_BRANCH": "",
            "PROJECT_COMMIT": "",
            "PROJECT_SUBMODULES": "",
            "FETCH_SINGLE_BRANCH": True,
        },
        "SVN": {"PROJECT_URL": "", "PROJECT_REVISION": ""},
        "LOCAL": {"PROJECT_PATH": ""},
        "PROJECT_FILES": {
            "FILE_PROJECT_CONF": "",
            "FILE_JOBS_CONF": "",
            "JOB_SCRIPTS_TYPE": "",
        },
        "RERUN": {"RERUN": False, "RERUN_JOBLIST": ""}
    }

@pytest.fixture()
def platforms_data():
    return {"PLATFORMS": {
            "MARENOSTRUM4": {
                "TYPE": "slurm",
                "HOST": "mn1.bsc.es",
                "PROJECT": "bsc32",
                "USER": None,
                "QUEUE": "debug",
                "SCRATCH_DIR": "/gpfs/scratch",
                "ADD_PROJECT_TO_HOST": False,
                "MAX_WALLCLOCK": "48:00",
                "TEMP_DIR": "",
            },
            "MARENOSTRUM_ARCHIVE": {
                "TYPE": "ps",
                "HOST": "dt02.bsc.es",
                "PROJECT": "bsc32",
                "USER": None,
                "SCRATCH_DIR": "/gpfs/scratch",
                "ADD_PROJECT_TO_HOST": False,
                "TEST_SUITE": False,
            },
            "TRANSFER_NODE": {
                "TYPE": "ps",
                "HOST": "dt01.bsc.es",
                "PROJECT": "bsc32",
                "USER": None,
                "ADD_PROJECT_TO_HOST": False,
                "SCRATCH_DIR": "/gpfs/scratch",
            },
            "TRANSFER_NODE_BSCEARTH000": {
                "TYPE": "ps",
                "HOST": "bscearth000",
                "USER": None,
                "PROJECT": "Earth",
                "ADD_PROJECT_TO_HOST": False,
                "QUEUE": "serial",
                "SCRATCH_DIR": "/esarchive/scratch",
            },
            "BSCEARTH000": {
                "TYPE": "ps",
                "HOST": "bscearth000",
                "USER": None,
                "PROJECT": "Earth",
                "ADD_PROJECT_TO_HOST": False,
                "QUEUE": "serial",
                "SCRATCH_DIR": "/esarchive/scratch",
            },
            "NORD3": {
                "TYPE": "SLURM",
                "HOST": "nord1.bsc.es",
                "PROJECT": "bsc32",
                "USER": None,
                "QUEUE": "debug",
                "SCRATCH_DIR": "/gpfs/scratch",
                "MAX_WALLCLOCK": "48:00",
            },
            "ECMWF-XC40": {
                "TYPE": "ecaccess",
                "VERSION": "pbs",
                "HOST": "cca",
                "USER": None,
                "PROJECT": "spesiccf",
                "ADD_PROJECT_TO_HOST": False,
                "SCRATCH_DIR": "/scratch/ms",
                "QUEUE": "np",
                "SERIAL_QUEUE": "ns",
                "MAX_WALLCLOCK": "48:00",
            },
        }}

def test_aiida_generator2(mocker, autosubmit_config, autosubmit, expdef_data, jobs_data, platforms_data, tmp_path):
    from autosubmit.config.basicconfig import BasicConfig
    from autosubmit.generators import Engine 

    expid = expdef_data['DEFAULT']['EXPID']
    as_conf = autosubmit_config(expid, experiment_data=expdef_data)
    mocker.patch('autosubmit.autosubmit.BasicConfig', as_conf.basic_config)
    read_files_mock = mocker.patch('autosubmit.autosubmit.read_files', return_value=None)

    config_path = tmp_path / expid / 'conf'
    config_path.mkdir(parents=True, exist_ok=True)
    BasicConfig.LOCAL_ROOT_DIR = tmp_path
    yaml = YAML()
    jobs_file_path = config_path / f"jobs_{expid}.yaml"
    expdef_file_path = config_path / f"expdef_{expid}.yaml"
    platforms_file_path = config_path / f"platforms_{expid}.yaml"

    with open(jobs_file_path, "w") as f:
        yaml.dump(jobs_data, f)
    with open(expdef_file_path, "w") as f:
        yaml.dump(expdef_data, f)
    with open(platforms_file_path, "w") as f:
        yaml.dump(platforms_data, f)
    read_files_mock.return_value = tmp_path

    # Generate job list data
    from autosubmit.job.job_list import JobList
    orig_generate = JobList.generate
    def mockgenerate(self, *args, **kwargs):
        kwargs['create'] = True
        orig_generate(self, *args, **kwargs)
    mocker.patch('autosubmit.job.job_list.JobList.generate', mockgenerate)
    autosubmit.load_job_list(
        as_conf.expid, as_conf, monitor=False
    )

    # Test generator
    import argparse
    generate_folder = tmp_path / "generate"
    generate_folder.mkdir()
    args = argparse.Namespace(output_dir=str(generate_folder.absolute()))
    autosubmit.generate_workflow(expid, Engine('aiida'), args)

    generated_paths_in_experiment_folder = set([
        f'{as_conf.expid}_LOCAL_SETUP.cmd',
        f'{as_conf.expid}_REMOTE_SETUP.cmd', # date from experiment data
        f'{as_conf.expid}_20000101_fc0_INI.cmd',
        f'{as_conf.expid}_20000101_fc0_TRANSFER.cmd', # date from experiment data
        f'{as_conf.expid}_20000101_fc0_1_SIM.cmd', # date from experiment data
        f'{as_conf.expid}_20000101_fc0_2_SIM.cmd', # date from experiment data
        f'{as_conf.expid}_POST.cmd',
        f'{as_conf.expid}_CLEAN.cmd',
        'local',
        "README.md",
        "submit_aiida_workflow.py",
    ])
    assert set(os.listdir(generate_folder / as_conf.expid)) == generated_paths_in_experiment_folder

    generated_paths_in_local_folder = set([
        "bash@local-setup.yml",
        "local-setup.yml"
    ])
    assert set(os.listdir(generate_folder / as_conf.expid / "local")) == generated_paths_in_local_folder
