#!/usr/bin/env python3

# Copyright 2017-2020 Earth Sciences Department, BSC-CNS

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

""" Diagram generator File """

import itertools
import math
import traceback
from collections import defaultdict
from dataclasses import dataclass, field

import matplotlib as mtp
import matplotlib.pyplot as plt
from matplotlib import gridspec
import matplotlib.patches as mpatches
from matplotlib.backends.backend_pdf import PdfPages

from autosubmit.statistics.jobs_stat import JobStat
from autosubmit.statistics.statistics import Statistics
from autosubmit.job.job import Job

from log.log import Log
from datetime import datetime, timedelta
from typing import List, Dict, Union, Any
from typing_extensions import LiteralString


mtp.use('Agg')
Log.get_logger("Autosubmit")

# Autosubmit stats constants
RATIO = 4
MAX_JOBS_PER_PLOT = 12.0
MAX_NUM_PLOTS = 40

# Summary config constants
MARGIN_DISTANCE = 3

# Table constants
TABLE_WIDTH = 15
TABLE_ROW_HEIGHT = 0.5


@dataclass
class JobAggData:
    """A data class with job data aggregated by values."""
    section: Dict[str, str] = field(default_factory=dict)
    count: int = 0
    queue_sum: timedelta = timedelta()
    avg_queue: timedelta = timedelta()
    run_sum: timedelta = timedelta()
    avg_run: timedelta = timedelta()

    @staticmethod
    def headers() -> List[str]:
        """
            Header function
            :param
            :return: List[str]
        """
        spaced = [k.split('_') for k in JobAggData.__annotations__.keys()]
        return [
            ' '.join([key.capitalize() for key in keys]) for keys in spaced
        ]

    def values(self) -> List[Any]:
        """
            Values function
            :param
            :return: List[Any]
        """
        return list(self.__dict__.values())

    @staticmethod
    def number_of_columns() -> int:
        """
            Num of columns function
            :param
            :return: int
        """
        return len(JobAggData.__annotations__)


@dataclass
class JobData:
    """A data class with job data."""
    job_name: str = ""
    queue_time: timedelta = timedelta()
    run_time: timedelta = timedelta()
    status: str = ""

    @staticmethod
    def headers() -> List[str]:
        spaced = [k.split('_') for k in JobData.__annotations__.keys()]
        return [
            ' '.join([key.capitalize() for key in keys]) for keys in spaced
        ]

    def values(self) -> List[Any]:
        return list(self.__dict__.values())

    @staticmethod
    def number_of_columns() -> int:
        return len(JobData.__annotations__)


def _seq(start, end, step):
    """From: https://pynative.com/python-range-for-float-numbers/"""
    sample_count = int(abs(end - start) / step)
    return itertools.islice(itertools.count(start, step), sample_count)


def populate_statistics(
        jobs_list: List[Job],
        period_ini: datetime,
        period_fi: datetime,
        queue_time_fixes: Dict[str, int]
) -> Statistics:
    try:
        return (
            Statistics(jobs_list, period_ini, period_fi, queue_time_fixes).
            calculate_statistics().
            calculate_summary().
            make_old_format().
            build_failed_jobs()
        )
    except Exception as exp:
        Log.warning(str(exp))


def create_stats_report(
        expid: str, jobs_list: List[Job], general_stats: List, output_file: str,
        section_summary: bool, jobs_summary: bool, hide: bool, period_ini: datetime = None,
        period_fi: datetime = None, queue_fix_times: Dict[str, int] = None
) -> bool:
    """Function to create the stats report.

    Produces one or more PDF files, depending on the parameters.

    Also produces CSV files with the data from each PDF.
    """
    # Close all figures first... just in case.
    plt.close('all')

    exp_stats = populate_statistics(jobs_list, period_ini, period_fi, queue_fix_times)
    plot = create_bar_diagram(expid, exp_stats, jobs_list, general_stats)
    create_csv_stats(exp_stats, jobs_list, output_file)

    if plot:
        if section_summary:
            jobs_data = _aggregate_jobs_by_section(jobs_list, exp_stats.jobs_stat)
            job_section_output_file = output_file.replace("statistics", "section_summary")
            headers = JobAggData.headers()
            _create_table(
                jobs_data,
                headers,
                doc_title=f"SECTION SUMMARY - {expid}",
                table_title="Aggregated by Job Section"
            )
            _create_csv(
                jobs_data,
                job_section_output_file,
                headers
            )
            Log.result(f'Section Summary created')
        if jobs_summary:
            jobs_data = _get_job_list_data(jobs_list, exp_stats.jobs_stat)
            jobs_output_file = output_file.replace("statistics", "jobs_summary")
            headers = JobData.headers()
            _create_table(
                jobs_data,
                headers,
                doc_title=f"JOBS SUMMARY - {expid}",
                table_title="Job List"
            )
            _create_csv(
                jobs_data,
                jobs_output_file,
                headers
            )
            Log.result(f'Jobs Summary created')

        with PdfPages(output_file) as pdf:
            for figure_number in plt.get_fignums():
                plt.figure(figure_number)
                pdf.savefig()
                plt.close()

            d = pdf.infodict()
            d['expid'] = expid

        Log.result(f'Stats created at {output_file}')

    return hide


def create_bar_diagram(expid: str, exp_stats: Statistics, jobs_list: List[Job],
                       general_stats: List[str]) -> bool:
    """create_bar_diagram Function

    :param
    :expid: str with the id of a experiment
    :exp_stats: Statistics of the jobs of the experiment
    :jobs_list: List[Job] of jobs in the experiment
    :general_stats: List[str] of status of the jobs

    :return: bool
    """
    # Error prevention
    normal_plots_count = 0
    failed_jobs_plots_count = 0
    try:
        normal_plots_count = int(math.ceil(len(exp_stats.jobs_stat) / MAX_JOBS_PER_PLOT))
        failed_jobs_plots_count = int(math.ceil(len(exp_stats.failed_jobs) / MAX_JOBS_PER_PLOT))
    except Exception as exp:
        Log.warning(str(exp))

    # Plotting
    total_plots_count = normal_plots_count + failed_jobs_plots_count
    width = 0.16
    # Creating stats figure + sanity check
    err_message = ("The results are too large to be shown, try narrowing your query.\nUse a filter like -ft where you "
                   "supply a list of job types, e.g. INI, SIM or use the flag -fp where you supply an integer that "
                   "represents the number of hours into the past that should be queried:\nSuppose it is noon, if you "
                   "supply -fp 5 the query will consider changes starting from 7:00 am. If you really wish to query the "
                   "whole experiment, refer to Autosubmit GUI.")
    if total_plots_count > MAX_NUM_PLOTS:
        Log.info(err_message)
        return False
    fig = plt.figure(figsize=(RATIO * 4, 3 * RATIO * total_plots_count))
    fig.suptitle(f'STATS - {expid}', fontsize=24, fontweight='bold')

    # Variables initialization
    ax = []
    rects = [None] * 5
    grid_spec = gridspec.GridSpec(RATIO * total_plots_count + 2, 1)
    i_plot = 0
    for plot in range(1, normal_plots_count + 1):
        try:
            # Calculating jobs inside the given plot
            l1 = int((plot - 1) * MAX_JOBS_PER_PLOT)
            l2 = min(int(plot * MAX_JOBS_PER_PLOT), len(exp_stats.jobs_stat))
            if l2 - l1 <= 0:
                continue
            ind = range(l2 - l1)
            ind_width = [x + width for x in ind]
            ind_width_3 = [x + width * 3 for x in ind]
            ind_width_4 = [x + width * 4 for x in ind]

            # Building plot axis
            ax.append(fig.add_subplot(grid_spec[RATIO * plot - RATIO + 2:RATIO * plot + 1]))
            ax[plot - 1].set_ylabel('hours')
            ax[plot - 1].set_xticks(ind_width)
            ax[plot - 1].set_xticklabels(
                [job.name for job in jobs_list[l1:l2]],
                rotation='vertical')
            ax[plot - 1].set_title(expid, fontsize=20)
            upper_limit = round(1.10 * exp_stats.max_time, 4)
            step = round(upper_limit / 10, 4)
            y_ticks = [round(x, 4) for x in _seq(0, upper_limit + step, step)]
            ax[plot - 1].set_yticks(y_ticks)
            ax[plot - 1].set_ylim(0, float(1.10 * exp_stats.max_time))

            # Building reacts
            rects[0] = ax[plot - 1].bar(ind, exp_stats.queued[l1:l2], width, color='lightpink')
            rects[1] = ax[plot - 1].bar(ind_width, exp_stats.run[l1:l2], width, color='green')
            rects[2] = ax[plot - 1].bar(ind_width_3, exp_stats.fail_queued[l1:l2], width, color='lightsalmon')
            rects[3] = ax[plot - 1].bar(ind_width_4, exp_stats.fail_run[l1:l2], width, color='salmon')
            rects[4] = ax[plot - 1].plot([0., width * 6 * MAX_JOBS_PER_PLOT],
                                         [exp_stats.threshold, exp_stats.threshold], "k--", label='wallclock sim')
            # Building legend
            i_plot = plot
        except Exception as exp:
            print((traceback.format_exc()))
            print(exp)

    job_names_in_failed = [name for name in exp_stats.failed_jobs_dict]
    failed_jobs_rects = [None]
    for j_plot in range(1, failed_jobs_plots_count + 1):
        try:
            l1 = int((j_plot - 1) * MAX_JOBS_PER_PLOT)
            l2 = min(int(j_plot * MAX_JOBS_PER_PLOT), len(job_names_in_failed))
            if l2 - l1 <= 0:
                continue
            ind = range(l2 - l1)
            ind_width = [x + width for x in ind]
            ind_width_2 = [x + width * 2 for x in ind]
            plot = i_plot + j_plot
            ax.append(fig.add_subplot(grid_spec[RATIO * plot - RATIO + 2:RATIO * plot + 1]))
            ax[plot - 1].set_ylabel('# failed attempts')
            ax[plot - 1].set_xticks(ind_width)
            ax[plot - 1].set_xticklabels([name for name in job_names_in_failed[l1:l2]], rotation='vertical')
            ax[plot - 1].set_title(expid, fontsize=20)
            ax[plot - 1].set_ylim(0, float(1.10 * exp_stats.max_fail))
            ax[plot - 1].set_yticks(range(0, exp_stats.max_fail + 2))
            failed_jobs_rects[0] = ax[plot - 1].bar(ind_width_2, [exp_stats.failed_jobs_dict[name] for name in
                                                                  job_names_in_failed[l1:l2]], width, color='red')
        except Exception as exp:
            print((traceback.format_exc()))
            print(exp)

    # Building legends subplot
    legends_plot = fig.add_subplot(grid_spec[0, 0])
    legends_plot.set_frame_on(False)
    legends_plot.axes.get_xaxis().set_visible(False)
    legends_plot.axes.get_yaxis().set_visible(False)

    try:
        # Building legends
        build_legends(legends_plot, rects, exp_stats, general_stats)
    except Exception as exp:
        print(exp)
        print((traceback.format_exc()))
    return True


def create_csv_stats(exp_stats: Statistics, jobs_list: List[Job],
                     output_file: Union[str, LiteralString, bytes]) -> None:
    """create_csv_stats Function

    :param
    :exp_stats: Statistics of the jobs of the experiment
    :jobs_list: List[Job] of jobs in the experiment
    :output_file: Union[str, LiteralString, bytes] Path to the file (str)

    :return: None
    """
    job_names = [job.name for job in exp_stats.jobs_stat]
    start_times = exp_stats.start_times
    end_times = exp_stats.end_times
    queuing_times = exp_stats.queued
    running_times = exp_stats.run

    output_file = output_file.replace('pdf', 'csv')
    with open(output_file, 'w') as file:
        file.write(
            "Job,Started,Ended,Queuing time (hours),Running time (hours)\n")
        for i in range(len([job for job in jobs_list if job.get_last_retrials()])):
            file.write("{0},{1},{2},{3},{4}\n".format(
                job_names[i], start_times[i], end_times[i], queuing_times[i], running_times[i]))


def build_legends(plot: Any, rects: list[list[str]], experiment_stats: Statistics,
                  general_stats: list[tuple[str, str]]) -> int:
    """build_legends Function

    :param plot: Subplot arrangement part of a figure
    :type plot: Any
    :param rects: A string that contains the legends for the plot
    :type rects: list[list[str]]
    :param experiment_stats: Statistics of the jobs of the experiment
    :type experiment_stats: Statistics
    :param general_stats: list that contains the status of the jobs
    :type general_stats: list[tuple[str, str]]
    :returns: Return the length of the legends built
    :rtype: int
    """
    # Main legend with colourful rectangles
    legend_rects = [[rect[0] for rect in rects]]

    legend_titles = [
        ['Queued (h)', 'Run (h)', 'Fail Queued (h)', 'Fail Run (h)', 'Max wallclock (h)']
    ]
    legend_locs = ["upper right"]
    legend_handlelengths = [None]

    # General stats legends, if exists
    if len(general_stats) > 0:
        legend_rects.append(get_whites_array(len(general_stats)))
        legend_titles.append([str(key) + ': ' + str(value) for key, value in general_stats])
        legend_locs.append("upper center")
        legend_handlelengths.append(0)

    # Total stats legend
    stats_summary_as_list = experiment_stats.summary_list
    legend_rects.append(get_whites_array(len(stats_summary_as_list)))
    legend_titles.append(stats_summary_as_list)
    legend_locs.append("upper left")
    legend_handlelengths.append(0)

    # Creating the legends
    legends = create_legends(plot, legend_rects, legend_titles, legend_locs, legend_handlelengths)
    for legend in legends:
        plt.gca().add_artist(legend)
    return len(legends)


def create_legends(plot, rects, titles, locs, handlelengths):
    legends = []
    for i in range(len(rects)):
        legends.append(create_legend(
            plot, rects[i], titles[i], locs[i], handlelengths[i]))
    return legends


def create_legend(plot, rects, titles, loc, handlelength=None):
    return plot.legend(rects, titles, loc=loc, handlelength=handlelength)


def get_whites_array(length):
    white = mpatches.Rectangle((0, 0), 0, 0, alpha=0.0)
    return [white for _ in range(length)]


def _group_by_section(jobs_list: List[Job], jobs_stats: List[JobStat]) -> Dict[str, List[JobStat]]:
    """Return a dictionary with the jobs grouped by section."""
    grouped_jobs_by_section = defaultdict(list)
    for job_stats in jobs_stats:
        for job in jobs_list:
            if job.name == job_stats.name:
                grouped_jobs_by_section[job.section].append(job_stats)
    return grouped_jobs_by_section


def _format_times(td: timedelta) -> str:
    """Return a human-readable string with the number of day(s), and the time in the delta."""
    days = td.days
    hours, remainder = divmod(td.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if days == 0:
        return f"{hours:02}:{minutes:02}:{seconds:02}"
    if days == 1:
        return f"{days} day - {hours:02}:{minutes:02}:{seconds:02}"
    return f"{days} days - {hours:02}:{minutes:02}:{seconds:02}"


def _filter_by_status(jobs_list: List[Job]) -> List[Job]:
    """Filter jobs by status."""
    ret = []
    for job in jobs_list:
        if job.status_str == "COMPLETED" or job.status_str == "RUNNING":
            ret.append(job)
    return ret


def _get_status(jobs_list: List[Job], job_name: str) -> str:
    """Return the status of the job."""
    for job in jobs_list:
        if job.name == job_name:
            return job.status_str


def _aggregate_jobs_by_section(jobs_list: List['Job'], jobs_stats: List['JobStat']) -> List[JobAggData]:
    """Aggregate jobs by section."""
    filtered_job_list = _filter_by_status(jobs_list)
    grouped_by_section = _group_by_section(filtered_job_list, jobs_stats)

    # Order jobs by section
    section_order = [section for section in grouped_by_section]
    for job in filtered_job_list:
        section = job.status_str
        if section not in section_order:
            section_order.append(section)

    # Calculate values
    section_values = [section for section in section_order if section in grouped_by_section]
    jobs_values = [grouped_by_section[section] for section in section_values]
    count_values = [len(values) for values in jobs_values]
    time_delta = timedelta()
    total_queue_time_values = [
        sum([job_stat.completed_queue_time + job_stat.failed_queue_time
             for job_stat in values], time_delta)
        for values in jobs_values]

    # Calculate total queue time
    total_run_time_values = []
    for section in jobs_values:
        sum_run_time = timedelta()
        for job_stat in section:
            if _get_status(jobs_list, job_stat.name) == "RUNNING":
                sum_run_time += datetime.now() - job_stat.start_time
            else:
                sum_run_time += job_stat.completed_run_time + job_stat.failed_run_time
        total_run_time_values.append(sum_run_time)
    avg_queue_time_values = [total_queue_time / count if count != 0 else timedelta()
                             for total_queue_time, count in zip(total_queue_time_values, count_values)]
    avg_run_time_values = [total_run_time / count if count != 0 else timedelta()
                           for total_run_time, count in zip(total_run_time_values, count_values)]

    # Format times
    total_queue_time_values = [_format_times(td) for td in total_queue_time_values]
    avg_queue_time_values = [_format_times(td) for td in avg_queue_time_values]
    total_run_time_values = [_format_times(td) for td in total_run_time_values]
    avg_run_time_values = [_format_times(td) for td in avg_run_time_values]

    data = [JobAggData(section, count, total_queue, avg_queue, total_run, avg_run)
            for section, count, total_queue, avg_queue, total_run, avg_run in
            zip(section_values, count_values, total_queue_time_values, avg_queue_time_values, total_run_time_values, avg_run_time_values)]

    # Order return data by section
    ret = []
    for job in jobs_list:
        for job_data in data:
            if job_data.section == job.section and job_data not in ret:
                ret.append(job_data)
                break
    return ret


def _get_job_list_data(jobs_list: List[Job], jobs_stats: List[JobStat]) -> List[JobData]:
    """Return a list of jobs data."""
    filtered_job_list = sorted(_filter_by_status(jobs_list), key=lambda x: x.name)

    jobs_stats_list = []
    for job in filtered_job_list:
        for job_stats in jobs_stats:
            if job_stats.name == job.name:
                jobs_stats_list.append(job_stats)
                break
    # Order initial jobs by name
    jobs_stats_list = sorted(jobs_stats_list, key=lambda x: x.name)

    # Calculate values
    job_names = [job_aux.name for job_aux in filtered_job_list]
    queue_values = [_format_times(job_stat.completed_queue_time + job_stat.failed_queue_time) for job_stat in jobs_stats_list]
    # Calculate run time
    run_values = []
    for job_stats in jobs_stats_list:
        if _get_status(jobs_list, job_stats.name) == "RUNNING":
            job_running_time = _format_times(datetime.now() - job_stats.start_time)
        else:
            job_running_time = _format_times(job_stats.completed_run_time + job_stats.failed_run_time)
        run_values.append(job_running_time)
    status = [_get_status(jobs_list, job_stat.name) for job_stat in jobs_stats_list]
    data = [JobData(job_name, queue_time, run_time, status)
            for job_name, queue_time, run_time, status in
            zip(job_names, queue_values, run_values, status)]

    # Order return data by job name
    res = []
    for job in jobs_list:
        for job_data in data:
            if job_data.job_name == job.name:
                res.append(job_data)
                break
    return res


def _create_table(
        jobs_data: List[Union[JobData, JobAggData]],
        headers: List[str],
        doc_title: str,
        table_title: str
) -> None:
    """
    Creates a pdf with the table of the jobs aggregated by section or the jobs list.

    :param jobs_data: jobs data for the table
    :type jobs_data: List[Union[JobData, JobAggData]]
    :param headers: Table headers.
    :type headers: List[str]
    :param doc_title: The PDF document title.
    :type doc_title: str
    :param table_title: The PDF table title.
    :type table_title: str
    :return: None
    """
    data = [job_data.values() for job_data in jobs_data]

    table_height = len(jobs_data) * TABLE_ROW_HEIGHT + 2

    fig = plt.figure(figsize=(TABLE_WIDTH, table_height))

    # Grid spec for the document
    grid_spec = gridspec.GridSpec(3, 1, height_ratios=[0.5, 0.1, len(jobs_data) * TABLE_ROW_HEIGHT])

    # Document title
    ax_title = fig.add_subplot(grid_spec[0])
    ax_title.text(0.5, 0.5, doc_title, fontsize=14, fontweight='bold', va='center', ha='center')
    ax_title.axis('off')

    # Table title
    ax_text = fig.add_subplot(grid_spec[1])
    ax_text.text(0.1, 0.3, table_title, fontsize=14, fontweight='bold', va='center', ha='left')
    ax_text.axis('off')

    # Table
    ax_table = fig.add_subplot(grid_spec[2])
    left_margin, right_margin = 0.1, 0.1
    table = ax_table.table(
        cellText=data,
        colLabels=headers,
        cellLoc='left',
        loc='center',
        bbox=[left_margin, 0, 1 - left_margin - right_margin, 1],
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)

    cell_dict = table.get_celld()

    for i in range(len(headers)):
        cell = cell_dict[(0, i)]
        cell.set_text_props(ha='left', va='center', weight='bold', color='w')
        cell.set_facecolor('#404040')
        cell.set_linewidth(0.1)
        cell.set_edgecolor('#404040')
        if i == 0:
            cell.set_width(0.3)  # first column width 30%
        else:
            cell.set_width(0.14)

    for i in range(1, len(jobs_data) + 1):  # skip header
        for j in range(len(headers)):
            cell = cell_dict[(i, j)]
            cell.set_linewidth(0.1)
            cell.set_edgecolor('#d9d9d9')
            if j == 0:
                cell.set_width(0.3)  # first column width 30%
            else:
                cell.set_width(0.14)
            if i % 2 == 0:
                cell.set_facecolor('#f0f0f0')
            else:
                cell.set_facecolor('#ffffff')

    ax_table.axis('off')

    # Save table
    plt.tight_layout()
    plt.subplots_adjust(left=0, right=1, top=0.7, bottom=0.1)


def _create_csv(
        jobs_data: List[Union[JobData, JobAggData]],
        output_file: str,
        headers: List[str]
) -> None:
    """
    Creates a CSV file with the table of the jobs aggregated by section or the jobs list.

    :param jobs_data: jobs data for the table
    :type jobs_data: List[Union[JobData, JobAggData]]
    :param output_file: output file
    :type output_file: str
    :param headers: Table headers.
    :type headers: List[str]
    :return: None
    """
    output_file_aux = output_file.replace('.pdf', '.csv')

    with open(output_file_aux, 'w') as file:
        file.write(",".join(headers) + "\n")
        for job_data in jobs_data:
            file.write(",".join([str(value) for value in job_data.values()]) + "\n")
