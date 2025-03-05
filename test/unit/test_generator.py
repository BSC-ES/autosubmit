from autosubmit.generators.aiida import Generator as AiidaGenerator
import pytest
import os


@pytest.fixture
def as_conf(autosubmit_config):
    expid = "dummy-id"
    # TODO I added PLATFORM to JOBS but they are still remote
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
    }

    as_conf = autosubmit_config(expid, experiment_data=experiment_data)
    return as_conf


class FakeBasicConfig:
    def __init__(self):
        pass

    def props(self):
        pr = {}
        for name in dir(self):
            value = getattr(self, name)
            if (
                not name.startswith("__")
                and not inspect.ismethod(value)
                and not inspect.isfunction(value)
            ):
                pr[name] = value
        return pr

    DB_DIR = "/dummy/db/dir"
    DB_FILE = "/dummy/db/file"
    DB_PATH = "/dummy/db/path"
    LOCAL_ROOT_DIR = "/dummy/local/root/dir"
    LOCAL_TMP_DIR = "/dummy/local/temp/dir"
    LOCAL_PROJ_DIR = "/dummy/local/proj/dir"
    DEFAULT_PLATFORMS_CONF = ""
    DEFAULT_JOBS_CONF = ""
    STRUCTURES_DIR = "/dummy/structure/dir"


@pytest.fixture
def job_list(as_conf):
    from autosubmit.autosubmit import Autosubmit

    job_list = Autosubmit.load_job_list(
        as_conf.expid, as_conf, notransitive=False, monitor=False
    )
    #  TODO(hack-conf) How can I enforce more cleanly that everything is local?
    #            It is in the config but still does not work.
    #            Or should I handle this case when a job does not specify a platform?
    jobs = job_list.get_all()
    for job in jobs:
        job._platform = jobs[0].platform # for jobs[0] the local platform is correctly set
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
