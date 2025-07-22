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

import pytest 
from autosubmit.job.job import Job

def test_creation(naive_job: Job):
    """
    Test to verify that a Job instance is correctly created.
    """
    assert isinstance(naive_job, Job)
    
    assert naive_job.name == "test_job_001"
    assert naive_job.status == "COMPLETED"
    assert naive_job._section == "SIM"
    
    assert naive_job.start_time_timestamp > 0
    assert naive_job.finish_time_timestamp > 0
    
    assert naive_job._chunk == "12"
    
    assert hasattr(naive_job, 'parameters')
    assert 'EXPERIMENT' in naive_job.parameters
    assert naive_job.parameters['EXPERIMENT']['CHUNKSIZEUNIT'] == "month"
    assert naive_job.parameters['EXPERIMENT']['CHUNKSIZE'] == "12"
    

