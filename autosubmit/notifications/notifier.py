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


class Notifier:
    def __init__(self):
        pass

    @staticmethod
    def notify_status_change(implementation, exp_id, job_name="", prev_status="", status="", notify_to=""):
        implementation.notify_status_change(exp_id, job_name, prev_status, status, notify_to)

    @staticmethod
    def notify_experiment_status(implementation, exp_id, notify_to="",platform=""):
        implementation.notify_experiment_status(exp_id,notify_to,platform)