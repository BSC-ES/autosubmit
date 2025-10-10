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

"""Platform headers."""

from typing import Any, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from autosubmit.job.job import Job


class PlatformHeader(Protocol):
    """TODO: Replace this protocol by a proper class design for headers."""

    SERIAL: str

    PARALLEL: str

    def get_queue_directive(self, job: 'Job', parameters: dict[str, Any]):
        pass  # pragma: no cover

    def calculate_het_header(self, job: 'Job', parameters: dict[str, Any]):
        pass  # pragma: no cover
