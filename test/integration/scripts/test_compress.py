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

"""Test for the autosubmit compresslogs command and --compresslogs option"""

from pathlib import Path

# from autosubmit.autosubmit import Autosubmit
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.scripts.autosubmit import main

_EXPID = "t111"


def test_autosubmit_compresslogs(autosubmit_exp, mocker):
    exp = autosubmit_exp(_EXPID, experiment_data={})

    # autosubmit: Autosubmit = exp.autosubmit
    as_conf: AutosubmitConfig = exp.as_conf

    mocker.patch("sys.argv", ["autosubmit", "compresslogs", _EXPID])

    assert 0 == main()

    aslogs_folder = Path(as_conf.basic_config.LOCAL_ROOT_DIR).joinpath(
        _EXPID, "tmp", "ASLOGS"
    )
    aslogs_files = list(aslogs_folder.iterdir())

    assert len(aslogs_files) > 0 and any(f.name.endswith(".xz") for f in aslogs_files)


def test_autosubmit_compresslogs_option(autosubmit_exp, mocker):
    exp = autosubmit_exp(_EXPID, experiment_data={})

    # autosubmit: Autosubmit = exp.autosubmit
    as_conf: AutosubmitConfig = exp.as_conf

    mocker.patch("sys.argv", ["autosubmit", "--compresslogs", "create", "-np", _EXPID])

    assert 0 == main()

    aslogs_folder = Path(as_conf.basic_config.LOCAL_ROOT_DIR).joinpath(
        _EXPID, "tmp", "ASLOGS"
    )
    aslogs_files = list(aslogs_folder.iterdir())

    assert (
        len(aslogs_files) > 0
        and any(f.name.endswith("_create.log.xz") for f in aslogs_files)
        and any(f.name.endswith("_create_err.log.xz") for f in aslogs_files)
    )
