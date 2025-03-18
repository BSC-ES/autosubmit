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

from autosubmit.history.platform_monitor import platform_utils as utils
from autosubmit.history.platform_monitor.slurm_monitor import SlurmMonitor


def test_reader_on_simple_wrapper_example_1():
    ssh_output = utils.read_example("wrapper1.txt")
    slurm_monitor = SlurmMonitor(ssh_output)
    # Header
    assert slurm_monitor.input_items[0].is_batch is False
    assert slurm_monitor.input_items[0].is_detail is False
    assert slurm_monitor.input_items[0].is_extern is False
    assert slurm_monitor.input_items[0].is_header is True
    assert slurm_monitor.input_items[0].is_detail is False
    # Batch
    assert slurm_monitor.input_items[1].is_batch is True
    assert slurm_monitor.input_items[1].is_detail is True
    assert slurm_monitor.input_items[1].is_extern is False
    assert slurm_monitor.input_items[1].is_header is False
    assert slurm_monitor.input_items[1].is_detail is True
    # Extern
    assert slurm_monitor.input_items[2].is_batch is False
    assert slurm_monitor.input_items[2].is_detail is True
    assert slurm_monitor.input_items[2].is_extern is True
    assert slurm_monitor.input_items[2].is_header is False
    assert slurm_monitor.input_items[2].is_detail is True
    header = slurm_monitor.header
    batch = slurm_monitor.batch
    extern = slurm_monitor.extern
    assert header is not None
    assert batch is not None
    assert extern is not None
    # print("{0} {1} <- {2}".format(batch.name, batch.energy, batch.energy_str))
    # print("{0} {1} <- {2}".format(extern.name, extern.energy, extern.energy_str))
    # print("{0} {1} <- {2}".format(header.name, header.energy, header.energy_str))
    assert slurm_monitor.steps_plus_extern_approximate_header_energy()


def test_reader_on_simple_wrapper_example_2():
    ssh_output = utils.read_example("wrapper2.txt")  # not real
    slurm_monitor = SlurmMonitor(ssh_output)
    # Header
    assert slurm_monitor.input_items[0].is_batch is False
    assert slurm_monitor.input_items[0].is_detail is False
    assert slurm_monitor.input_items[0].is_step is False
    assert slurm_monitor.input_items[0].is_extern is False
    assert slurm_monitor.input_items[0].is_header is True
    # Batch
    assert slurm_monitor.input_items[1].is_batch is True
    assert slurm_monitor.input_items[1].is_detail is True
    assert slurm_monitor.input_items[1].is_step is False
    assert slurm_monitor.input_items[1].is_extern is False
    assert slurm_monitor.input_items[1].is_header is False
    # Step 0
    assert slurm_monitor.input_items[2].is_batch is False
    assert slurm_monitor.input_items[2].is_detail is True
    assert slurm_monitor.input_items[2].is_step is True
    assert slurm_monitor.input_items[2].is_extern is False
    assert slurm_monitor.input_items[2].is_header is False
    assert slurm_monitor.input_items[2].step_number >= 0


def test_reader_on_big_wrapper():
    ssh_output = utils.read_example("wrapper_big.txt")
    slurm_monitor = SlurmMonitor(ssh_output)
    assert slurm_monitor.step_count == 30
    header = slurm_monitor.header
    batch = slurm_monitor.batch
    extern = slurm_monitor.extern
    assert header is not None
    assert batch is not None
    assert extern is not None
    assert slurm_monitor.steps_plus_extern_approximate_header_energy()
