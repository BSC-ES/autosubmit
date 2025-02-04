from pathlib import Path
from textwrap import dedent

from autosubmitconfigparser.config.basicconfig import BasicConfig

from autosubmit.autosubmit import Autosubmit


def test_copy_as_config(tmp_path, autosubmit_config):
    autosubmit_config('a000', {})

    ini_file = Path(f'{BasicConfig.LOCAL_ROOT_DIR}/a000/conf')
    new_file = Path(f'{BasicConfig.LOCAL_ROOT_DIR}/a001/conf')
    ini_file.mkdir(parents=True, exist_ok=True)
    new_file.mkdir(parents=True, exist_ok=True)
    ini_file = ini_file / 'jobs_a000.conf'
    new_file = new_file / 'jobs_a001.yml'

    print(f'BasicConfig: {BasicConfig.LOCAL_ROOT_DIR}')
    with open(ini_file, 'w+') as f:
        f.write(dedent('''\
                [LOCAL_SETUP]
                FILE = LOCAL_SETUP.sh
                PLATFORM = LOCAL
                '''))
        f.flush()

    Autosubmit().copy_as_config('a001','a000')

    new_yaml_file = Path(new_file.parent, new_file.stem).with_suffix('.yml')

    assert new_yaml_file.exists()
    assert new_yaml_file.stat().st_size > 0

    new_yaml_file = Path(new_file.parent, new_file.stem).with_suffix('.conf_AS_v3_backup')

    assert new_yaml_file.exists()
    assert new_yaml_file.stat().st_size > 0
