# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

import os
from pathlib import Path

import pytest
from ruamel.yaml import YAML


@pytest.mark.parametrize(
    "data,owner",
    [
        ({
             "DEFAULT": {
                 "HPCARCH": "LOCAL",
             },
         }, True),
        ({
             "DEFAULT": {
                 "HPCARCH": "LOCAL",
             },
         }, False)
    ],
    ids=["local_true", "local_false"]
)
def test_save(autosubmit_config, tmpdir, mocker, data: dict, owner: str):
    if owner:
        os.environ["USER"] = Path(tmpdir).owner()
    else:
        os.environ["USER"] = 'whatever'
    as_conf = autosubmit_config(expid='t000', experiment_data=data, include_basic_config=False)
    as_conf.load_common_parameters(as_conf.experiment_data)
    as_conf.save()

    data['ROOTDIR'] = str(as_conf.basic_config.LOCAL_ROOT_DIR / as_conf.expid)

    if not owner:
        assert not Path(as_conf.metadata_folder).exists()
    else:
        assert Path(as_conf.metadata_folder).exists()
        assert (Path(as_conf.metadata_folder) / 'experiment_data.yml').exists()

        # check contents
        with open(Path(as_conf.metadata_folder) / 'experiment_data.yml', 'r') as f:
            yaml = YAML(typ="safe")
            loaded = yaml.load(f)
            assert data['DEFAULT']['HPCARCH'] == loaded['DEFAULT']['HPCARCH']
            assert data['ROOTDIR'] == loaded['ROOTDIR']

        # Test .bak generated.
        as_conf.save()
        assert (Path(as_conf.metadata_folder) / 'experiment_data.yml.bak').exists()
        # force fail save
        mocker.patch("builtins.open", side_effect=Exception("Forced exception"))
        mocker.patch("shutil.copyfile", return_value=True)
        as_conf.save()
        assert Path(as_conf.metadata_folder) / 'experiment_data.yml' not in Path(as_conf.metadata_folder).iterdir()
        assert as_conf.data_changed is True
        assert as_conf.last_experiment_data == {}
