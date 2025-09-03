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
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/

from typing import Protocol


class TemplateSnippet(Protocol):
    """Autosubmit template.

    Implementations must provide the header, body, and tailer
    functions.

    The triple value is used by platforms when creating the final
    script, which is a combination of header, body, and a tailer/footer.

    NOTE: This type, protocol, is implemented for modules like ``a.job.template.bash``,
          ref: https://peps.python.org/pep-0544/#modules-as-implementations-of-protocols
    """

    def as_header(self, platform_header: str, executable: str) -> str:
        """The Autosubmit header.

        :param platform_header: The platform header. A platform such as Slurm, for instance,
            may offer extra headers like ``SBATCH`` directives.
        :param executable: The executable. Used to create the Bash shebang, for instance.
        """

    def as_body(self, body: str) -> str:
        """The script body.

        :param body: The script body. In a shell template, for instance, it would be the
            complete Bash script that the user configured the job to run with. For Bash,
            continuing the example, we would return the execution in a subshell, so that
            we can run commands that contain ``exit 1`` or that fail, but still continue
            and update the ``_STAT_`` file. Note, that on failure the tailer is still not
            executed, even with the subshell. The same applies to the other languages.
        """

    def as_tailer(self) -> str:
        """The Autosubmit tailer.

        The tailer plays crucial role as it must create the ``_COMPLETED``
        file.

        Tailer code is only executed when the job script (header + body up
        to this point) did not fail.

        Otherwise, Autosubmit misses the ``_COMPLETED`` file and marks the
        job as ``FAILED``.
        """
