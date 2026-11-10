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

"""Tests for the Slurm monitor."""

from autosubmit.history.platform_monitor.slurm_monitor import SlurmMonitor


def test_slurm_monitor_energy_values():
    """Test that for a job with batch + extern + steps, energy is calculated correctly."""
    output = '''15994954  COMPLETED        448        2 2025-02-24T16:11:33 2025-02-24T16:11:42 2025-02-24T16:21:30        883.55K                                                     
15994954.batch  COMPLETED        224        1 2025-02-24T16:11:42 2025-02-24T16:11:42 2025-02-24T16:21:30        497.36K                    18111K                    18111K 
15994954.extern  COMPLETED        448        2 2025-02-24T16:11:42 2025-02-24T16:11:42 2025-02-24T16:21:30        883.55K                      427K                      421K 
   15994954.0  COMPLETED        224        1 2025-02-24T16:11:47 2025-02-24T16:11:47 2025-02-24T16:11:52              0                     3486K                     3486K 
   15994954.1  COMPLETED        448        2 2025-02-24T16:12:17 2025-02-24T16:12:17 2025-02-24T16:21:22        844.90K                 29740154K              27008625.50K 
'''
    slurm_monitor = SlurmMonitor(output)

    header_energy = 883550.0
    batch_energy = 497360.0
    extern_energy = 883550.0
    first_step_energy = 0
    second_step_energy = 844900.0
    steps_energy = first_step_energy + second_step_energy
    total_energy = header_energy + first_step_energy + second_step_energy

    assert slurm_monitor.batch is not None
    assert slurm_monitor.batch.energy == batch_energy

    assert slurm_monitor.extern is not None
    assert slurm_monitor.extern.energy == extern_energy

    assert slurm_monitor.step_count == 2

    assert slurm_monitor.steps[0].step_number == 0
    assert slurm_monitor.steps[0].energy == first_step_energy

    assert slurm_monitor.steps[1].step_number == 1
    assert slurm_monitor.steps[1].energy == second_step_energy

    assert slurm_monitor.steps_energy == steps_energy

    assert slurm_monitor.total_energy == total_energy

    assert slurm_monitor.header.energy == header_energy
