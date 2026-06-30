# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

import pytest


@pytest.mark.parametrize("noclean,uncompress", [
    (True, True),
    (True, False),
    (False, False),
])
def test_archive_and_unarchive(as_exp, mocker, noclean, uncompress):
    as_exp.autosubmit.create(
        as_exp.expid,
        noplot=True,
        hide=True,
    )

    assert as_exp.autosubmit.archive(as_exp.expid, noclean, uncompress)
    assert as_exp.autosubmit.unarchive(as_exp.expid, uncompress)


def test_archive_noncreated_experiment(as_exp):
    assert as_exp.autosubmit.archive(as_exp.expid)


def test_unarchive_nonarchived_experiment(as_exp):
    assert not as_exp.autosubmit.unarchive(as_exp.expid)
