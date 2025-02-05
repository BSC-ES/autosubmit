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

from . import platform_utils as utils

class SlurmMonitorItem:
  def __init__(self, name, status, ncpus, nnodes, submit, start, finish, energy="0", MaxRSS=0.0, AveRSS=0.0):
    self.name = str(name)
    self.status = str(status)
    self.ncpus = int(ncpus)
    self.nnodes = int(nnodes)    
    self.submit = utils.try_parse_time_to_timestamp(submit)
    self.start = utils.try_parse_time_to_timestamp(start)
    self.finish = utils.try_parse_time_to_timestamp(finish)
    self.energy_str = energy
    self.energy = utils.parse_output_number(energy)
    self.MaxRSS = utils.parse_output_number(MaxRSS)
    self.AveRSS = utils.parse_output_number(AveRSS)
  
  @property
  def is_header(self):
    return not self.is_detail

  @property
  def is_detail(self):
    if self.name.find(".") >= 0:
      return True
    return False
    
  @property
  def is_extern(self):
    if self.name.find(".ext") >= 0:
      return True
    return False
  
  @property
  def is_batch(self):
    if self.name.find(".bat") >= 0:
      return True
    return False
  
  @property
  def step_number(self):
    if self.is_step is True:
      point_loc = self.name.find(".")
      return int(self.name[point_loc+1:])
    return -1
  
  @property
  def is_step(self):
    if self.name.find(".") >= 0 and self.is_batch is False and self.is_extern is False:
      return True
    return False
    
  @classmethod
  def from_line(cls, line):
    line = line.strip().split()
    if len(line) < 2:
      raise Exception("Slurm parser found a line too short {0}".format(line))
    new_item = cls(line[0],
                  line[1],
                  str(line[2]) if len(line) > 2 else 0,
                  str(line[3]) if len(line) > 3 else 0,
                  str(line[4]) if len(line) > 4 else 0,
                  str(line[5]) if len(line) > 5 else 0,
                  str(line[6]) if len(line) > 6 else 0,
                  str(line[7]) if len(line) > 7 else 0,
                  str(line[8]) if len(line) > 8 else 0,
                  str(line[9]) if len(line) > 9 else 0)
    return new_item

  def get_as_dict(self):    
    return {"ncpus": self.ncpus,
            "nnodes": self.nnodes,
            "submit": self.submit,
            "start": self.start,
            "finish": self.finish,
            "energy": self.energy,
            "MaxRSS": self.MaxRSS,
            "AveRSS": self.AveRSS}
  
  def __str__(self):
    return "Name {0}, Status {1}, NCpus {2}, NNodes {3}, Submit {4}, Start {5}, Finish {6}, Energy {7}, MaxRSS {8}, AveRSS {9} [Energy Str {10}]".format(self.name, self.status, self.ncpus, self.nnodes, self.submit, self.start, self.finish, self.energy, self.MaxRSS, self.AveRSS, self.energy_str, self.is_batch)