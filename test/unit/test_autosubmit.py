from pathlib import Path
from textwrap import dedent
from typing import Callable, Dict


from autosubmit.autosubmit import Autosubmit
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from test.unit.conftest import autosubmit_config


def test_as_conf_default_values(autosubmit_config: Callable[[str,Dict], BasicConfig]) -> None:

    expid = "a000"
    arch = "mn5"
    hpc = "mn4"
    autosubmit_config(expid, {})

    ini_file = Path(f'{BasicConfig.LOCAL_ROOT_DIR}/a000/conf')
    ini_file.mkdir(parents=True, exist_ok=True)
    ini_file = ini_file / 'jobs_a000.yml'

    with open(ini_file, 'w+') as f:
        f.write(dedent(f'''\
                DEFAULT:
                    AUTOSUBMIT_VERSION:
                    EXPID: "a001"
                    HPCARCH: {arch}
        '''))
        f.flush()

    Autosubmit().as_conf_default_values(expid, hpc)

    factory = YAMLParserFactory()
    parser = factory.create_parser()

    with open(ini_file) as f:
        content = f.read()
        yaml_contents = parser.load(content)

    print(f'yaml_contents: {yaml_contents}')
    assert yaml_contents['DEFAULT']['EXPID'] == expid
    assert yaml_contents['DEFAULT']['HPCARCH'] == arch or yaml_contents['DEFAULT']['HPCARCH'] == hpc
