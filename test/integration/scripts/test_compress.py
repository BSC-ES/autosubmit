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

import pytest

# from autosubmit.autosubmit import Autosubmit
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.scripts.autosubmit import main

_EXPID = "t111"


@pytest.mark.parametrize(
    "dry_run, compress_type",
    [(True, "gzip"), (False, "gzip"), (True, "xz"), (False, "xz")],
)
def test_autosubmit_compresslogs(
    autosubmit_exp, mocker, dry_run: bool, compress_type: str
):
    exp = autosubmit_exp(_EXPID, experiment_data={})

    # autosubmit: Autosubmit = exp.autosubmit
    as_conf: AutosubmitConfig = exp.as_conf

    cmd = ["autosubmit", "compresslogs"]
    if dry_run:
        cmd.append("--dry-run")
    cmd.append(f"--compress-type={compress_type}")
    cmd.append(_EXPID)

    mocker.patch("sys.argv", cmd)

    assert 0 == main()

    aslogs_folder = Path(as_conf.basic_config.LOCAL_ROOT_DIR).joinpath(
        _EXPID, "tmp", "ASLOGS"
    )
    aslogs_files = list(aslogs_folder.iterdir())

    extension = "gz" if compress_type == "gzip" else "xz"
    if dry_run:
        assert len(aslogs_files) > 0 and not any(
            f.name.endswith(f".{extension}") for f in aslogs_files
        )
    else:
        assert len(aslogs_files) > 0 and any(
            f.name.endswith(f".{extension}") for f in aslogs_files
        )

@pytest.mark.parametrize("compression_type", ["gzip", "xz"])
def test_autosubmit_compresslogs_option(autosubmit_exp, mocker, compression_type: str):
    exp = autosubmit_exp(_EXPID, experiment_data={})

    # autosubmit: Autosubmit = exp.autosubmit
    as_conf: AutosubmitConfig = exp.as_conf

    cmd = [
        "autosubmit",
        "--compresslogs",
        f"--compress-type={compression_type}",
        "create",
        "-np",
        _EXPID,
    ]

    mocker.patch("sys.argv", cmd)

    assert 0 == main()

    aslogs_folder = Path(as_conf.basic_config.LOCAL_ROOT_DIR).joinpath(
        _EXPID, "tmp", "ASLOGS"
    )
    aslogs_files = list(aslogs_folder.iterdir())

    extension = "gz" if compression_type == "gzip" else "xz"
    assert (
        len(aslogs_files) > 0
        and any(f.name.endswith(f"_create.log.{extension}") for f in aslogs_files)
        and any(f.name.endswith(f"_create_err.log.{extension}") for f in aslogs_files)
    )
