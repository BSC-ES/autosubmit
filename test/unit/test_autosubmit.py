from pathlib import Path
from textwrap import dedent
from typing import Callable, Dict


from autosubmit.autosubmit import Autosubmit
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from test.unit.conftest import autosubmit_config


def test_as_conf_default_values(autosubmit_config: Callable[[str,Dict], BasicConfig]) -> None:

    expid = 'a000'
    arch = 'MN5'
    autosubmit_config(expid, {})

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

    Autosubmit().as_conf_default_values(exp_id=expid)

    factory = YAMLParserFactory()
    parser = factory.create_parser()

    with open(ini_file) as f:
        content = f.read()
        yaml_contents = parser.load(content)

    assert yaml_contents['DEFAULT']['EXPID'] == expid
    assert yaml_contents['DEFAULT']['HPCARCH'] == arch
