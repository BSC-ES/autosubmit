#!/usr/bin/env python3

# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

from autosubmit.log.log import Log

from .platform_monitor import PlatformMonitor
from .slurm_monitor_item import SlurmMonitorItem

HEADER_DIFF_TOLERANCE = 0.01


class SlurmMonitor(PlatformMonitor):
    """ Manages Slurm commands interpretation. """

    def __init__(self, platform_output):
        super().__init__(platform_output)
        self._identify_input_rows()

    @property
    def steps_energy(self):
        return sum([step.energy for step in self.input_items if step.is_step])

    @property
    def steps_cpu_time_raw(self):
        return sum([step.CPUTimeRAW for step in self.input_items if step.is_step])

    @property
    def total_energy(self):
        return max(self.header.energy, self.steps_energy + self.extern.energy)

    @property
    def total_cpu_time_raw(self):
        return max(self.header.CPUTimeRAW, self.steps_cpu_time_raw + self.extern.CPUTimeRAW)

    @property
    def step_count(self):
        return len([step for step in self.input_items if step.is_step])

    @property
    def steps(self):
        return [item for item in self.input_items if item.is_step]

    @property
    def header(self):
        # test
        headers = [header for header in self.input_items if header.is_header]
        if len(headers) > 0:
            return headers[0]
        else:
            return None

    @property
    def batch(self):
        # test
        batch = [batch for batch in self.input_items if batch.is_batch]
        if len(batch) > 0:
            return batch[0]
        else:
            return None

    @property
    def extern(self):
        # test
        extern = [extern for extern in self.input_items if extern.is_extern]
        if len(extern) > 0:
            return extern[0]
        else:
            return None

    def _identify_input_rows(self):
        lines = self.input.split("\n")
        self.input_items = [SlurmMonitorItem.from_line(line) for line in lines]

    def steps_plus_extern_approximate_header(self, attribute: str) -> bool:
        """Check whether steps plus extern approximately match the header value.

        :param attribute: The attribute to compare (e.g., "energy" or "CPUTimeRAW").
        :type attribute: str

        :return: True if the sum of steps and extern values is within HEADER_DIFF_TOLERANCE (%) 
        of the header value, False otherwise.
        :rtype: bool
        """
        if not self.header:
            Log.warning(f"Header information is missing. Cannot perform comparison for {attribute}.")
            return False
        elif not self.extern:
            Log.warning(f"Extern information is missing. Cannot perform comparison for {attribute}.")
            return False

        steps_value = sum(getattr(step, attribute) for step in self.input_items if step.is_step)
        extern_value = getattr(self.extern, attribute)
        header_value = getattr(self.header, attribute)
        return abs(steps_value + extern_value - header_value) <= HEADER_DIFF_TOLERANCE * header_value
