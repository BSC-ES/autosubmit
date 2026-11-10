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

"""Unit tests for ``autosubmit.job.job`` module."""

import pickle
from typing import TYPE_CHECKING

from autosubmit.job.job import Job

if TYPE_CHECKING:
    from _pytest.monkeypatch import MonkeyPatch


def test_job_serialisation_ignores_non_persistent_attributes(
    monkeypatch: "MonkeyPatch",
) -> None:
    """Serialising a Job must not access non-persistent attributes.

    A production experiment in DestinE had ``RecursionError`` entries in
    the log. In Autosubmit 4.1.x, ``Job.__getstate__`` attempted to
    serialise every slot except a small set of explicitly excluded attributes.
    This could cause a ``RecursionError`` when accessing a problematic
    attribute.

    Here, we reproduce the problem by replacing the non-persistent
    ``rerun_only`` slot with a descriptor that recursively accesses itself.

    In 4.1.x, ``__getstate__`` accesses ``rerun_only`` and therefore raises
    ``RecursionError``. In 4.2.0, ``__getstate__`` only accesses attributes
    listed in ``PERSISTENT_ATTRIBUTES``, so ``rerun_only`` is not accessed
    and serialisation succeeds.

    This test was manually run on 4.1.16.1, where it failed to run, and also
    on 4.2.0 (commit 54058a7d2fe5e43731f124b096deaed677e5ada8) successfully.
    """
    job = Job(name="sim_parent_20200101_001_1", job_id=1, status=3)

    class RecursiveAttr:
        def __get__(self, instance, owner):
            return instance.rerun_only

    monkeypatch.setattr(Job, "rerun_only", RecursiveAttr())

    pickle.dumps(job)
