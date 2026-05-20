# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

"""Dispatch CPMIP threshold-violation notifications for completed jobs.

A ``CPMIPNotifier`` orchestrates three concerns:

* **Capture**: collect the runtime metadata (start/end timestamps, simulated
  years, processing elements, configured thresholds) from a job while its
  attributes are still populated. This must happen *before*
  :meth:`JobList.update_log_status` runs because the latter wipes runtime
  attributes when it recovers logs.
* **Evaluate**: build a :class:`CPMIPMetricsData` and ask
  :class:`CPMIPMetrics` whether any threshold is violated.
* **Notify**: when there are violations, hand them to the standard mail
  notifier for delivery.
"""

from dataclasses import dataclass
from typing import Optional

from bscearth.utils.date import chunk_end_date, chunk_start_date, subs_dates

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.job.job_common import Status
from autosubmit.job.job_utils import is_leap_year
from autosubmit.log.log import Log
from autosubmit.metrics.cpmip_metrics import CPMIPMetrics, CPMIPMetricsData
from autosubmit.notifications.mail_notifier import MailNotifier
from autosubmit.notifications.notifier import Notifier
from autosubmit.statistics.jobs_stat import _calculate_processing_elements


@dataclass(frozen=True)
class CPMIPEvaluation:
    """Snapshot of the inputs needed to evaluate CPMIP thresholds for a job."""

    data: CPMIPMetricsData
    thresholds: dict


class CPMIPNotifier:
    """Build CPMIP metric inputs from a job and notify on threshold violations."""

    @staticmethod
    def capture(job, as_conf) -> Optional[CPMIPEvaluation]:
        """Snapshot the data required to evaluate CPMIP thresholds.

        Call this before ``JobList.update_log_status`` clears the job's runtime
        attributes. Returns ``None`` when the job has no CPMIP thresholds
        configured or when its stat file is missing/unreadable; the caller can
        safely use the result as a truthy gate.
        """
        thresholds = job.cpmip_thresholds
        if not thresholds:
            return None

        start_time = job.check_start_time(job.fail_count)
        end_time = job.check_end_time(job.fail_count)
        if start_time <= 0 or end_time <= 0:
            return None

        data = CPMIPMetricsData(
            start_time=start_time,
            end_time=end_time,
            run_years=_simulated_years(job, as_conf),
            ncpus=_estimate_cpus(job),
        )
        return CPMIPEvaluation(data=data, thresholds=thresholds)

    @staticmethod
    def notify(as_conf, expid: str, job, evaluation: CPMIPEvaluation) -> None:
        """Send a mail notification if any captured threshold is violated.

        No-op when notifications are disabled in the experiment configuration
        or when the job did not complete successfully.
        """
        if as_conf.get_notifications() != "true":
            return
        if job.status != Status.COMPLETED:
            return

        violations = CPMIPMetrics.evaluate(evaluation.data, evaluation.thresholds)
        if not violations:
            return

        Notifier.notify_cpmip_threshold_violations(
            MailNotifier(BasicConfig),
            expid,
            job.name,
            violations,
            as_conf.experiment_data["MAIL"]["TO"],
        )


def _estimate_cpus(job) -> Optional[int]:
    """Compute the number of CPUs used by the job, mirroring statistics logic."""
    try:
        return _calculate_processing_elements(
            job.nodes,
            job.processors,
            job.tasks,
            job.processors_per_node,
            job.exclusive,
        )
    except (TypeError, ValueError) as error:
        Log.warning(f"Unable to compute CPMIP processing elements: {error}")
        return None


def _simulated_years(job, as_conf) -> Optional[float]:
    """Return chunk duration in simulated years, honouring the experiment calendar.

    Uses the same calendar primitives the Job class relies on so the result
    matches what the workflow would compute for ``RUN_DAYS``.
    """
    if job.date is None or job.chunk is None or not job.chunk_size:
        return None

    calendar = str(
        as_conf.experiment_data.get("EXPERIMENT", {}).get("CALENDAR", "standard")
    ).lower()
    chunk_unit = str(job.chunk_size_unit or "day").lower()

    try:
        chunk_length = int(job.chunk_size)
        chunk = int(job.chunk)
        start = chunk_start_date(job.date, chunk, chunk_length, chunk_unit, calendar)
        end = chunk_end_date(start, chunk_length, chunk_unit, calendar)
        run_days = float(subs_dates(start, end, calendar))
    except (TypeError, ValueError) as error:
        Log.warning(f"Unable to compute CPMIP simulated years: {error}")
        return None

    # Pick the year length that matches the calendar/year actually being simulated:
    # noleap calendars are always 365, otherwise leap years contribute 366 days.
    days_in_year = 366.0 if calendar != "noleap" and is_leap_year(start.year) else 365.0
    return run_days / days_in_year
