from textwrap import dedent
from pathlib import Path
from autosubmit.autosubmit import Autosubmit
from test.unit.conftest import autosubmit_config
from autosubmitconfigparser.config.basicconfig import BasicConfig

import re

def test_as_conf_default_values(tmp_path, autosubmit_config):

    autosubmit_config('a000', {})

    ini_file = Path(f'{BasicConfig.LOCAL_ROOT_DIR}/a000/conf')
    ini_file.mkdir(parents=True, exist_ok=True)
    ini_file = ini_file / 'jobs_a000.yml'

    with open(ini_file, 'w+') as f:
        f.write(dedent('''\
                DEFAULT:
                    AUTOSUBMIT_VERSION:
                    EXPID: "a001"
                    HPCARCH: "MN5"
        '''))
        f.flush()

    Autosubmit().as_conf_default_values('a000')

    with open(ini_file) as f:
        content = f.read()
        hpc = re.search('HPCARCH: \"MN5\"', content, re.MULTILINE)
        expid = re.search('EXPID: \"a000\"', content, re.MULTILINE)

        assert hpc
        assert expid
