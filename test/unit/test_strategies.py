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

from collections import namedtuple

from autosubmit.history.data_classes.job_data import JobData
from autosubmit.history.platform_monitor.slurm_monitor import SlurmMonitor
from autosubmit.history.strategies import PlatformInformationHandler, TwoDimWrapperDistributionStrategy

job_dc = namedtuple("Job", ["job_name", "date", "member", "status_str", "children", "children_list"])


class Test2DWrapperDistributionStrategy:
    def setup_method(self):
        self.strategy = TwoDimWrapperDistributionStrategy()
        self.job_data_dcs_in_wrapper = [
            JobData(0, job_name="a29z_20000101_fc2_1_POSTR", status="COMPLETED", submit=10, start=100, finish=200,
                    ncpus=100, energy=0, children="a29z_20000101_fc1_1_CLEAN, a29z_20000101_fc3_1_POST"),
            JobData(0, job_name="a29z_20000101_fc1_1_CLEAN", status="COMPLETED", submit=10, start=100, finish=200,
                    ncpus=100, energy=0, children="a29z_20000101_fc2_1_CLEAN"),
            JobData(0, job_name="a29z_20000101_fc3_1_POST", status="COMPLETED", submit=10, start=100, finish=200,
                    ncpus=100, energy=0, children="a29z_20000101_fc0_3_SIM"),
            JobData(0, job_name="a29z_20000101_fc2_1_CLEAN", status="COMPLETED", submit=10, start=100, finish=200,
                    ncpus=100, energy=0, children=""),
            JobData(0, job_name="a29z_20000101_fc0_3_SIM", status="COMPLETED", submit=10, start=100, finish=200,
                    ncpus=100, energy=0, children=""),
            JobData(0, job_name="a29z_20000101_fc1_2_POSTR1", status="COMPLETED", submit=10, start=100, finish=200,
                    ncpus=100, energy=0, children="a29z_20000101_fc1_5_POST2"),
            JobData(0, job_name="a29z_20000101_fc1_5_POST2", status="COMPLETED", submit=10, start=100, finish=200,
                    ncpus=100, energy=0, children="a29z_20000101_fc1_4_POST3"),
            JobData(0, job_name="a29z_20000101_fc1_4_POST3", status="COMPLETED", submit=10, start=100, finish=200,
                    ncpus=100, energy=0, children="a29z_20000101_fc2_5_CLEAN4"),
            JobData(0, job_name="a29z_20000101_fc2_5_CLEAN4", status="COMPLETED", submit=10, start=100, finish=200,
                    ncpus=100, energy=0, children="a29z_20000101_fc0_1_POST5"),
            JobData(0, job_name="a29z_20000101_fc0_1_POST5", status="COMPLETED", submit=10, start=100, finish=200,
                    ncpus=100, energy=0, children=""),
        ]

    def test_get_all_children(self):
        children = self.strategy._get_all_children(self.job_data_dcs_in_wrapper)
        assert len(children) == 8

    def test_get_roots(self):
        roots = self.strategy._get_roots(self.job_data_dcs_in_wrapper)
        assert len(roots) == 2

    def test_get_level(self):
        roots = self.strategy._get_roots(self.job_data_dcs_in_wrapper)
        job_name_to_children_names = {job.job_name: job.children_list for job in self.job_data_dcs_in_wrapper}
        next_level = self.strategy.get_level(roots, job_name_to_children_names)
        assert len(next_level) == 3

    def test_get_jobs_per_level(self):
        levels = self.strategy.get_jobs_per_level(self.job_data_dcs_in_wrapper)
        for level in levels:
            print([job.job_name for job in level])
        assert len(levels) == 5
        assert "a29z_20000101_fc0_1_POST5" in [job.job_name for job in levels[4]]

    def test_energy_distribution(self):
        ssh_output = '''                 17857525  COMPLETED         10        1 2021-10-13T15:51:16 2021-10-13T15:51:17 2021-10-13T15:52:47         2.62K                                                     
           17857525.batch  COMPLETED         10        1 2021-10-13T15:51:17 2021-10-13T15:51:17 2021-10-13T15:52:47          1.88K                     6264K                     6264K 
          17857525.extern  COMPLETED         10        1 2021-10-13T15:51:17 2021-10-13T15:51:17 2021-10-13T15:52:47          1.66K                      473K                       68K 
               17857525.0  COMPLETED         10        1 2021-10-13T15:51:21 2021-10-13T15:51:21 2021-10-13T15:51:22            186                      352K                   312.30K 
               17857525.1  COMPLETED         10        1 2021-10-13T15:51:23 2021-10-13T15:51:23 2021-10-13T15:51:24            186                      420K                   306.70K 
               17857525.2  COMPLETED         10        1 2021-10-13T15:51:24 2021-10-13T15:51:24 2021-10-13T15:51:27            188                      352K                   325.80K 
               17857525.3  COMPLETED         10        1 2021-10-13T15:51:28 2021-10-13T15:51:28 2021-10-13T15:51:29            192                      352K                   341.90K
               17857525.4  COMPLETED         10        1 2021-10-13T15:51:28 2021-10-13T15:51:28 2021-10-13T15:51:29            210                      352K                   341.90K                
    '''
        slurm_monitor = SlurmMonitor(ssh_output)
        info_handler = PlatformInformationHandler(TwoDimWrapperDistributionStrategy())
        job_dcs = info_handler.execute_distribution(self.job_data_dcs_in_wrapper[0], self.job_data_dcs_in_wrapper,
                                                    slurm_monitor)
        for job in job_dcs:
            print(("{0} -> {1} and {2} : ncpus {3} running {4}".format(job.job_name, job.energy, job.rowstatus,
                                                                       job.ncpus, job.running_time)))
        for level in info_handler.strategy.jobs_per_level:
            print([job.job_name for job in level])
        total_in_jobs = sum(job.energy for job in job_dcs[:-1])  # ignore last
        assert abs(total_in_jobs - slurm_monitor.total_energy) <= 10
        assert abs(job_dcs[0].energy - 259) < 1
        assert abs(job_dcs[1].energy - 259) < 1
        assert abs(job_dcs[2].energy - 228) < 1
        assert abs(job_dcs[3].energy - 228) < 1
        assert abs(job_dcs[4].energy - 228) < 1
        assert abs(job_dcs[5].energy - 228.67) < 1
        assert abs(job_dcs[6].energy - 228.67) < 1
        assert abs(job_dcs[7].energy - 228.67) < 1
        assert abs(job_dcs[8].energy - 358) < 1
        assert abs(job_dcs[9].energy - 376) < 1
