from autosubmit.generators.aiida import Generator as AiidaGenerator
import pytest
import os


@pytest.fixture
def as_conf(autosubmit_config):
    expid = "dummy-id"
    experiment_data = {
        "CONFIG": {
            "AUTOSUBMIT_VERSION": "4.1.9",
            "MAXWAITINGJOBS": 20,
            "TOTALJOBS": 20,
            "SAFETYSLEEPTIME": 10,
            "RETRIALS": 0,
            "RELOAD_WHILE_RUNNING": False,
        },
        "MAIL": {"NOTIFICATIONS": False, "TO": None},
        "STORAGE": {"TYPE": "pkl", "COPY_REMOTE_LOGS": True},
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
        "RERUN": {"RERUN": False, "RERUN_JOBLIST": ""},
        "JOBS": {
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
        },
        "PLATFORMS": {
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
        },
        "LOCAL_TMP_DIR": "/dummy/local/temp/dir"
    }

    as_conf = autosubmit_config(expid, experiment_data=experiment_data)
    return as_conf

@pytest.fixture
def job_list(as_conf, monkeypatch):
    from autosubmit.job.job_list import JobList
    orig_generate = JobList.generate
    def mockgenerate(self, *args, **kwargs):
        kwargs['create'] = True
        orig_generate(self, *args, **kwargs)

    monkeypatch.setattr(JobList, 'generate', mockgenerate)
    from autosubmit.autosubmit import Autosubmit

    job_list = Autosubmit.load_job_list(
        as_conf.expid, as_conf, notransitive=False, monitor=False
    )
    jobs = job_list.get_all()
    for job in jobs:
        job._platform = jobs[0].platform # for jobs[0] the local platform is correctly set
        job.het = {}
    return job_list

def test_aiida_generator(as_conf, job_list, tmp_path):
    AiidaGenerator.generate(job_list, as_conf, str(tmp_path))
    
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
    assert set(os.listdir(tmp_path / as_conf.expid)) == generated_paths_in_experiment_folder

    generated_paths_in_local_folder = set([
        "bash@local-setup.yml",
        "local-setup.yml"
    ])
    assert set(os.listdir(tmp_path / as_conf.expid / "local")) == generated_paths_in_local_folder
