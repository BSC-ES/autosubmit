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

from abc import ABCMeta, abstractmethod
import autosubmit.history.database_managers.database_models as Models
import traceback
from .internal_logging import Logging
from .database_managers.database_manager import DEFAULT_HISTORICAL_LOGS_DIR

class PlatformInformationHandler:
  def __init__(self, strategy):
    self._strategy = strategy
  
  @property
  def strategy(self):
    return self._strategy
  
  @strategy.setter
  def strategy(self, strategy):
    self._strategy = strategy
  
  def execute_distribution(self, job_data_dc, job_data_dcs_in_wrapper, slurm_monitor):
    return self._strategy.apply_distribution(job_data_dc, job_data_dcs_in_wrapper, slurm_monitor)
  

class Strategy(metaclass=ABCMeta):
  """ Strategy Interface """

  def __init__(self, historiclog_dir_path=DEFAULT_HISTORICAL_LOGS_DIR):
    self.historiclog_dir_path = historiclog_dir_path

  @abstractmethod
  def apply_distribution(self, job_data_dc, job_data_dcs_in_wrapper, slurm_monitor):
    pass

  def set_job_data_dc_as_processed(self, job_data_dc, original_ssh_output):
    job_data_dc.platform_output = original_ssh_output
    job_data_dc.rowstatus = Models.RowStatus.PROCESSED
    return job_data_dc
  
  def set_job_data_dc_as_process_failed(self, job_data_dc, original_ssh_output):
    job_data_dc.platform_output = original_ssh_output
    job_data_dc.rowstatus = Models.RowStatus.FAULTY
    return job_data_dc
  
  def get_calculated_weights_of_jobs_in_wrapper(self, job_data_dcs_in_wrapper):
    """ Based on computational weight: running time in seconds * number of cpus. """
    total_weight = sum(job.computational_weight for job in job_data_dcs_in_wrapper)
    if total_weight == 0:
      total_weight = 1
    return {job.job_name: round(job.computational_weight/total_weight, 4) for job in job_data_dcs_in_wrapper}


class SingleAssociationStrategy(Strategy):

  def __init__(self, historiclog_dir_path=DEFAULT_HISTORICAL_LOGS_DIR):
    super(SingleAssociationStrategy, self).__init__(historiclog_dir_path=historiclog_dir_path)

  def apply_distribution(self, job_data_dc, job_data_dcs_in_wrapper, slurm_monitor):
    try:
      if len(job_data_dcs_in_wrapper) > 0:
        return []
      # job_data_dc.submit = slurm_monitor.header.submit
      # job_data_dc.start = slurm_monitor.header.start
      # job_data_dc.finish = slurm_monitor.header.finish
      job_data_dc.ncpus = slurm_monitor.header.ncpus
      job_data_dc.nnodes = slurm_monitor.header.nnodes
      job_data_dc.energy = slurm_monitor.header.energy
      job_data_dc.MaxRSS = max(slurm_monitor.header.MaxRSS, slurm_monitor.batch.MaxRSS if slurm_monitor.batch else 0, slurm_monitor.extern.MaxRSS if slurm_monitor.extern else 0) # TODO: Improve this rule
      job_data_dc.AveRSS = max(slurm_monitor.header.AveRSS, slurm_monitor.batch.AveRSS if slurm_monitor.batch else 0, slurm_monitor.extern.AveRSS if slurm_monitor.extern else 0)        
      job_data_dc = self.set_job_data_dc_as_processed(job_data_dc, slurm_monitor.original_input)
      return [job_data_dc]
    except Exception as exp:
      Logging("strategies", self.historiclog_dir_path).log("SingleAssociationStrategy failed for {0}. Using ssh_output: {1}. Exception message: {2}".format(job_data_dc.job_name, slurm_monitor.original_input, str(exp)), 
                              traceback.format_exc())
      job_data_dc = self.set_job_data_dc_as_process_failed(job_data_dc, slurm_monitor.original_input)
      return [job_data_dc]

class StraightWrapperAssociationStrategy(Strategy):

  def __init__(self, historiclog_dir_path=DEFAULT_HISTORICAL_LOGS_DIR):
    super(StraightWrapperAssociationStrategy, self).__init__(historiclog_dir_path=historiclog_dir_path)

  def apply_distribution(self, job_data_dc, job_data_dcs_in_wrapper, slurm_monitor):
    """ """
    try:
      if len(job_data_dcs_in_wrapper) != slurm_monitor.step_count:
        return []
      result = []
      computational_weights = self.get_calculated_weights_of_jobs_in_wrapper(job_data_dcs_in_wrapper)      
      for job_dc, step in zip(job_data_dcs_in_wrapper, slurm_monitor.steps):        
        job_dc.energy = step.energy + computational_weights.get(job_dc.job_name, 0) * slurm_monitor.extern.energy
        job_dc.AveRSS = step.AveRSS
        job_dc.MaxRSS = step.MaxRSS        
        job_dc.platform_output = ""
        if job_dc.job_name == job_data_dc.job_name:
          job_data_dc.energy = job_dc.energy
          job_data_dc.AveRSS = job_dc.AveRSS
          job_data_dc.MaxRSS = job_dc.MaxRSS        
        result.append(job_dc)
      job_data_dc = self.set_job_data_dc_as_processed(job_data_dc, slurm_monitor.original_input)
      result.append(job_data_dc)
      return result
    except Exception as exp:
      Logging("strategies", self.historiclog_dir_path).log("StraightWrapperAssociationStrategy failed for {0}. Using ssh_output: {1}. Exception message: {2}".format(job_data_dc.job_name, 
                                    slurm_monitor.original_input, 
                                    str(exp)), 
                              traceback.format_exc())
      job_data_dc = self.set_job_data_dc_as_process_failed(job_data_dc, slurm_monitor.original_input)
      return [job_data_dc]

class GeneralizedWrapperDistributionStrategy(Strategy):

  def __init__(self, historiclog_dir_path=DEFAULT_HISTORICAL_LOGS_DIR):
    super(GeneralizedWrapperDistributionStrategy, self).__init__(historiclog_dir_path=historiclog_dir_path)

  def apply_distribution(self, job_data_dc, job_data_dcs_in_wrapper, slurm_monitor):
    try:
      result = []
      computational_weights = self.get_calculated_weights_of_jobs_in_wrapper(job_data_dcs_in_wrapper)
      for job_dc in job_data_dcs_in_wrapper:
        job_dc.energy = round(computational_weights.get(job_dc.job_name, 0) * slurm_monitor.total_energy,2)
        job_dc.platform_output = "" 
        if job_dc.job_name == job_data_dc.job_name:
          job_data_dc.energy = job_dc.energy      
        result.append(job_dc)
      job_data_dc = self.set_job_data_dc_as_processed(job_data_dc, slurm_monitor.original_input)
      result.append(job_data_dc)
      return result
    except Exception as exp:
      Logging("strategies", self.historiclog_dir_path).log("GeneralizedWrapperDistributionStrategy failed for {0}. Using ssh_output: {1}. Exception message: {2}".format(job_data_dc.job_name, slurm_monitor.original_input, str(exp)), 
                              traceback.format_exc())
      job_data_dc = self.set_job_data_dc_as_process_failed(job_data_dc, slurm_monitor.original_input)
      return [job_data_dc]

class TwoDimWrapperDistributionStrategy(Strategy):

  def __init__(self, historiclog_dir_path=DEFAULT_HISTORICAL_LOGS_DIR):
    super(TwoDimWrapperDistributionStrategy, self).__init__(historiclog_dir_path=historiclog_dir_path)
    self.jobs_per_level = None

  def apply_distribution(self, job_data_dc, job_data_dcs_in_wrapper, slurm_monitor):
    try:
      result = []        
      self.jobs_per_level = self.get_jobs_per_level(job_data_dcs_in_wrapper)
      if len(self.jobs_per_level) != slurm_monitor.step_count:
        return []
      comp_weight_per_level = self.get_comp_weight_per_level(self.jobs_per_level)
      level_energy = []
      for i, step in enumerate(slurm_monitor.steps):
        level_energy.append(step.energy + comp_weight_per_level[i] * slurm_monitor.extern.energy)
      for i, jobs in enumerate(self.jobs_per_level):
        weights = self.get_comp_weight_per_group_of_job_dcs(jobs)
        for j, job_dc in enumerate(jobs):
          job_dc.energy = round(level_energy[i] * weights[j], 2)
          if job_dc.job_name == job_data_dc.job_name:
            job_data_dc.energy = job_dc.energy    
          result.append(job_dc)   
      job_data_dc = self.set_job_data_dc_as_processed(job_data_dc, slurm_monitor.original_input)
      result.append(job_data_dc)
      return result
    except Exception as exp:
      Logging("strategies", self.historiclog_dir_path).log("TwoDimWrapperDistributionStrategy failed for {0}. Using ssh_output: {1}. Exception message: {2}".format(job_data_dc.job_name, slurm_monitor.original_input, str(exp)), 
                              traceback.format_exc())
      job_data_dc = self.set_job_data_dc_as_process_failed(job_data_dc, slurm_monitor.original_input)
      return [job_data_dc]

  def get_jobs_per_level(self, job_data_dcs_in_wrapper):
    """ List of Lists, index of list is the level. """
    job_name_to_object = {job.job_name: job for job in job_data_dcs_in_wrapper}         
    levels = []    
    roots_dcs = self._get_roots(job_data_dcs_in_wrapper)    
    levels.append(roots_dcs)
    next_level = self.get_level(roots_dcs, job_name_to_object)      
    while len(next_level) > 0:      
      levels.append([job for job in next_level])      
      next_level = self.get_level(next_level, job_name_to_object)            
    return levels
  
  def _get_roots(self, job_data_dcs_in_wrapper):
    children_names = self._get_all_children(job_data_dcs_in_wrapper)
    return [job for job in job_data_dcs_in_wrapper if job.job_name not in children_names]

  def _get_all_children(self, job_data_dcs_in_wrapper):
    result = []
    for job_dc in job_data_dcs_in_wrapper:
      result.extend(job_dc.children_list)
    return result
  
  def get_comp_weight_per_group_of_job_dcs(self, jobs):
    total = sum(job.computational_weight for job in jobs)
    return [round(job.computational_weight/total, 4) for job in jobs]

  def get_comp_weight_per_level(self, jobs_per_level):
    level_weight = []
    total_weight = 0
    for jobs in jobs_per_level:
      computational_weight = sum(job.computational_weight for job in jobs)
      total_weight += computational_weight
      level_weight.append(computational_weight)    
    return [round(weight/total_weight, 4) for weight in level_weight]

  def get_level(self, previous_level_dcs, job_name_to_object):
    children_names = []        
    for job_dc in previous_level_dcs:
      children_names.extend(job_dc.children_list)        
    level_dcs = [job_name_to_object[job_name] for job_name in children_names if job_name in job_name_to_object]    
    return level_dcs

    