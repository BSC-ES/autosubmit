#!/usr/bin/env python3

# Copyright 2015-2020 Earth Sciences Department, BSC-CNS
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

from .platform_monitor import PlatformMonitor
from .slurm_monitor_item import SlurmMonitorItem

class SlurmMonitor(PlatformMonitor):
  """ Manages Slurm commands interpretation. """
  def __init__(self, platform_output):
      super(SlurmMonitor, self).__init__(platform_output)
      self._identify_input_rows()

  @property
  def steps_energy(self):
    return sum([step.energy for step in self.input_items if step.is_step])
    
  @property
  def total_energy(self):
    return max(self.header.energy, self.steps_energy + self.extern.energy)

  @property
  def step_count(self):
    return len([step for step in self.input_items if step.is_step])

  def _identify_input_rows(self):
      lines = self.input.split("\n")
      self.input_items = [SlurmMonitorItem.from_line(line) for line in lines]        
  
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
    #test
    batch = [batch for batch in self.input_items if batch.is_batch]
    if len(batch) > 0:
        return batch[0]
    else:
        return None

  @property
  def extern(self):
    #test
    extern = [extern for extern in self.input_items if extern.is_extern]
    if len(extern) > 0:
        return extern[0]
    else:
        return None

  def steps_plus_extern_approximate_header_energy(self):
    return abs(self.steps_energy + self.extern.energy - self.header.energy) <= 0.01*self.header.energy


