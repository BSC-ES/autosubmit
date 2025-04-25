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

"""Tests for the straight wrapper association strategy."""

from datetime import datetime
from time import time

from autosubmit.history.data_classes.job_data import JobData
from autosubmit.history.database_managers.database_models import RowType, RowStatus
from autosubmit.history.platform_monitor.slurm_monitor import SlurmMonitor
from autosubmit.history.strategies import SingleAssociationStrategy
from autosubmit.job.job_common import Status


def _create_job_data_dc(_id, name):
    return JobData(
        _id=_id,
        counter=_id,
        job_name=name,
        submit=str(int(time())),
        status=Status,
        rowtype=RowType.NORMAL,
        ncpus=1,
        wallclock='00:30',
        qos='gp_debug',
        date=datetime.now(),
        member='fc0',
        section='SIM',
        chunk='2',
        platform='MARENOSTRUM5',
        job_id='test',
        children=[],
        run_id='1',
        workflow_commit='')


def test_straight_wrapper_distribution_with_wrappers(tmp_path):
    """When wrappers are used, the distribution is empty."""
    output = '''15994954  COMPLETED        448        2 2025-02-24T16:11:33 2025-02-24T16:11:42 2025-02-24T16:21:30        883.55K                                                     
    15994954.batch  COMPLETED        224        1 2025-02-24T16:11:42 2025-02-24T16:11:42 2025-02-24T16:21:30        497.36K                    18111K                    18111K 
    15994954.extern  COMPLETED        448        2 2025-02-24T16:11:42 2025-02-24T16:11:42 2025-02-24T16:21:30        883.55K                      427K                      421K 
       15994954.0  COMPLETED        224        1 2025-02-24T16:11:47 2025-02-24T16:11:47 2025-02-24T16:11:52              0                     3486K                     3486K 
       15994954.1  COMPLETED        448        2 2025-02-24T16:12:17 2025-02-24T16:12:17 2025-02-24T16:21:22        844.90K                 29740154K              27008625.50K 
    '''
    slurm_monitor = SlurmMonitor(output)

    job_data_dc = _create_job_data_dc(15994954, 'a28v_19900101_fc0_2_SIM')

    job_data_dcs_in_wrapper = [
        _create_job_data_dc(15994954, 'a28v_19900101_fc0_2_SIM'),
        _create_job_data_dc(15994954, 'a28v_19900101_fc0_2_SIM')
    ]

    strategy = SingleAssociationStrategy(str(tmp_path))

    assert job_data_dc.rowstatus == RowStatus.INITIAL

    processed_jobs = strategy.apply_distribution(job_data_dc, job_data_dcs_in_wrapper, slurm_monitor)

    assert not processed_jobs


def test_straight_wrapper_distribution(tmp_path):
    output = '''15994954  COMPLETED        448        2 2025-02-24T16:11:33 2025-02-24T16:11:42 2025-02-24T16:21:30        883.55K                                                     
    15994954.batch  COMPLETED        224        1 2025-02-24T16:11:42 2025-02-24T16:11:42 2025-02-24T16:21:30        497.36K                    18111K                    18111K 
    15994954.extern  COMPLETED        448        2 2025-02-24T16:11:42 2025-02-24T16:11:42 2025-02-24T16:21:30        883.55K                      427K                      421K 
       15994954.0  COMPLETED        224        1 2025-02-24T16:11:47 2025-02-24T16:11:47 2025-02-24T16:11:52              0                     3486K                     3486K 
       15994954.1  COMPLETED        448        2 2025-02-24T16:12:17 2025-02-24T16:12:17 2025-02-24T16:21:22        844.90K                 29740154K              27008625.50K 
    '''
    slurm_monitor = SlurmMonitor(output)

    job_data_dc = _create_job_data_dc(15994954, 'a28v_19900101_fc0_2_SIM')

    job_data_dcs_in_wrapper = []

    strategy = SingleAssociationStrategy(str(tmp_path))

    assert job_data_dc.rowstatus == RowStatus.INITIAL

    processed_jobs = strategy.apply_distribution(job_data_dc, job_data_dcs_in_wrapper, slurm_monitor)

    # only the original job data is processed
    assert len(processed_jobs) == 1
    assert job_data_dc.rowstatus == RowStatus.PROCESSED

    # the header energy is used
    assert job_data_dc.energy == slurm_monitor.header.energy
