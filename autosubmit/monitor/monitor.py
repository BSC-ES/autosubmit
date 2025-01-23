#!/usr/bin/env python3

# Copyright 2015-2020Earth Sciences Department, BSC-CNS

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
import datetime
import os
import time
import sys
from os import path
from os import chdir
from os import listdir
from os import remove

import py3dotplus as pydotplus
import copy

import subprocess
import autosubmit.history.utils as HUtils
import autosubmit.helpers.utils as HelperUtils

from autosubmit.job.job_common import Status
from autosubmit.job.job import Job
from autosubmit.helpers.utils import NaturalSort
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.configcommon import AutosubmitConfig

from autosubmit.monitor.diagram import create_stats_report
from log.log import Log, AutosubmitCritical
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory

from typing import Dict, List

GENERAL_STATS_OPTION_MAX_LENGTH = 1000


def _display_file_xdg(a_file: str) -> None:
    """Displays the PDF for the user.

    Tries to use the X Desktop Group tool ``xdg-open``. If that fails,
    it fallbacks to ``mimeopen``. If this latter fails too, then it
    propagates the possible ``subprocess.CalledProcessError`` exception,
    or another exception or error raised.

    :param a_file: A file to be displayed.
    :type a_file: str
    :return: Nothing.
    :rtype: None
    :raises subprocess.CalledProcessError: raised by ``subprocess.check_output`` of
        either ``xdg-open`` or ``mimeopen``.
    """
    try:
        subprocess.check_output(["xdg-open", a_file])
    except subprocess.CalledProcessError:
        subprocess.check_output(["mimeopen", a_file])


def _display_file(a_file: str):
    """Display a file for the user.

    The file is displayed using the user preferred application.
    This is achieved first checking if the user is on Linux or
    not. If not, we try to use ``open``.

    If ``open`` fails, then we try the same approach used on
    Linux (maybe it is not macOS nor windows?).

    But, if the user is already on Linux, then we simply call
    ``xdg-open``. And if ``xdg-open`` fails, we still fallback
    to ``mimeopen``.
    """
    if sys.platform != "linux":
        try:
            subprocess.check_output(["open", a_file])
        except subprocess.CalledProcessError:
            _display_file_xdg(a_file)
    else:
        _display_file_xdg(a_file)


class Monitor:
    """Class to handle monitoring of Jobs at HPC."""
    _table = dict([(Status.UNKNOWN, 'white'), (Status.WAITING, 'gray'), (Status.READY, 'lightblue'), (Status.PREPARED, 'skyblue'),
         (Status.SUBMITTED, 'cyan'), (Status.HELD, 'salmon'), (Status.QUEUING, 'pink'), (Status.RUNNING, 'green'), (Status.COMPLETED, 'yellow'), (Status.FAILED, 'red'), (Status.DELAYED, 'lightcyan'),
         (Status.SUSPENDED, 'orange'), (Status.SKIPPED, 'lightyellow')])

    def __init__(self):
        self.nodes_plotted = None

    @staticmethod
    def color_status(status):
        """
        Return color associated to given status

        :param status: status
        :type status: Status
        :return: color
        :rtype: str
        """
        if status == Status.WAITING:
            return Monitor._table[Status.WAITING]
        elif status == Status.READY:
            return Monitor._table[Status.READY]
        elif status == Status.PREPARED:
            return Monitor._table[Status.PREPARED]
        elif status == Status.SUBMITTED:
            return Monitor._table[Status.SUBMITTED]
        elif status == Status.HELD:
            return Monitor._table[Status.HELD]
        elif status == Status.QUEUING:
            return Monitor._table[Status.QUEUING]
        elif status == Status.RUNNING:
            return Monitor._table[Status.RUNNING]
        elif status == Status.COMPLETED:
            return Monitor._table[Status.COMPLETED]
        elif status == Status.SKIPPED:
            return Monitor._table[Status.SKIPPED]
        elif status == Status.FAILED:
            return Monitor._table[Status.FAILED]
        elif status == Status.SUSPENDED:
            return Monitor._table[Status.SUSPENDED]
        elif status == Status.DELAYED:
            return Monitor._table[Status.SUSPENDED]
        else:
            return Monitor._table[Status.UNKNOWN]

    def create_tree_list(self, expid, joblist, packages, groups, hide_groups=False):
        """
        Create graph from joblist

        :param hide_groups:
        :param groups:
        :param packages:
        :param expid: experiment's identifier
        :type expid: str
        :param joblist: joblist to plot
        :type joblist: JobList
        :return: created graph
        :rtype: pydotplus.Dot
        """
        Log.debug('Creating workflow graph...')
        graph = pydotplus.Dot(graph_type='digraph')

        Log.debug('Creating legend...')
        legend = pydotplus.Subgraph(
            graph_name='Legend', label='Legend', rank="source")
        legend.add_node(pydotplus.Node(name='UNKNOWN', shape='box', style="",
                                       fillcolor=self._table[Status.UNKNOWN]))

        legend.add_node(pydotplus.Node(name='WAITING', shape='box', style="filled",
                                       fillcolor=self._table[Status.WAITING]))
        legend.add_node(pydotplus.Node(name='DELAYED', shape='box', style="filled",
                                       fillcolor=self._table[Status.DELAYED]))
        legend.add_node(pydotplus.Node(name='READY', shape='box', style="filled",
                                       fillcolor=self._table[Status.READY]))
        legend.add_node(pydotplus.Node(name='PREPARED', shape='box', style="filled",
                                       fillcolor=self._table[Status.PREPARED]))

        legend.add_node(pydotplus.Node(name='SUBMITTED', shape='box', style="filled",
                                       fillcolor=self._table[Status.SUBMITTED]))
        legend.add_node(pydotplus.Node(name='HELD', shape='box', style="filled",
                                       fillcolor=self._table[Status.HELD]))
        legend.add_node(pydotplus.Node(name='QUEUING', shape='box', style="filled",
                                       fillcolor=self._table[Status.QUEUING]))
        legend.add_node(pydotplus.Node(name='RUNNING', shape='box', style="filled",
                                       fillcolor=self._table[Status.RUNNING]))
        legend.add_node(pydotplus.Node(name='SKIPPED', shape='box', style="filled",
                                       fillcolor=self._table[Status.SKIPPED]))
        legend.add_node(pydotplus.Node(name='COMPLETED', shape='box', style="filled",
                                       fillcolor=self._table[Status.COMPLETED]))
        legend.add_node(pydotplus.Node(name='FAILED', shape='box', style="filled",
                                       fillcolor=self._table[Status.FAILED]))

        legend.add_node(pydotplus.Node(name='SUSPENDED', shape='box', style="filled",
                                       fillcolor=self._table[Status.SUSPENDED]))

        graph.add_subgraph(legend)

        exp = pydotplus.Subgraph(graph_name='Experiment', label=expid)
        self.nodes_plotted = set()
        Log.debug('Creating job graph...')

        jobs_packages_dict = dict()
        if packages is not None and len(str(packages)) > 0:
            for (exp_id, package_name, job_name, wallclock) in packages:
                jobs_packages_dict[job_name] = package_name

        packages_subgraphs_dict = dict()

        for job in joblist:
            if job.has_parents():
                continue

            if not groups or job.name not in groups['jobs'] or (job.name in groups['jobs'] and len(groups['jobs'][job.name]) == 1):
                node_job = pydotplus.Node(job.name, shape='box', style="filled",
                                          fillcolor=self.color_status(job.status))

                if groups and job.name in groups['jobs']:
                    group = groups['jobs'][job.name][0]
                    node_job.obj_dict['name'] = group
                    node_job.obj_dict['attributes']['fillcolor'] = self.color_status(
                        groups['status'][group])
                    node_job.obj_dict['attributes']['shape'] = 'box3d'

                exp.add_node(node_job)
                self._add_children(job, exp, node_job, groups, hide_groups)

        if groups:
            if not hide_groups:
                for job, group in groups['jobs'].items():
                    if len(group) > 1:
                        group_name = 'cluster_' + '_'.join(group)
                        if group_name not in graph.obj_dict['subgraphs']:
                            subgraph = pydotplus.graphviz.Cluster(
                                graph_name='_'.join(group))
                            subgraph.obj_dict['attributes']['color'] = 'invis'
                        else:
                            subgraph = graph.get_subgraph(group_name)[0]

                        previous_node = exp.get_node(group[0])[0]
                        if len(subgraph.get_node(group[0])) == 0:
                            subgraph.add_node(previous_node)

                        for i in range(1, len(group)):
                            node = exp.get_node(group[i])[0]
                            if len(subgraph.get_node(group[i])) == 0:
                                subgraph.add_node(node)

                            edge = subgraph.get_edge(
                                node.obj_dict['name'], previous_node.obj_dict['name'])
                            if len(edge) == 0:
                                edge = pydotplus.Edge(previous_node, node)
                                edge.obj_dict['attributes']['dir'] = 'none'
                                # constraint false allows the horizontal alignment
                                edge.obj_dict['attributes']['constraint'] = 'false'
                                edge.obj_dict['attributes']['penwidth'] = 4
                                subgraph.add_edge(edge)

                            previous_node = node
                        if group_name not in graph.obj_dict['subgraphs']:
                            graph.add_subgraph(subgraph)
            else:
                for edge in copy.deepcopy(exp.obj_dict['edges']):
                    if edge[0].replace('"', '') in groups['status']:
                        del exp.obj_dict['edges'][edge]

            graph.set_strict(True)

        graph.add_subgraph(exp)
        #Wrapper visualization
        for node in exp.get_nodes():
            name = node.obj_dict['name']
            if name in jobs_packages_dict:
                package = jobs_packages_dict[name]
                if package not in packages_subgraphs_dict:
                    packages_subgraphs_dict[package] = pydotplus.graphviz.Cluster(
                        graph_name=package)
                    packages_subgraphs_dict[package].obj_dict['attributes']['color'] = 'black'
                    packages_subgraphs_dict[package].obj_dict['attributes']['style'] = 'dashed'
                packages_subgraphs_dict[package].add_node(node)

        for package, cluster in packages_subgraphs_dict.items():
            graph.add_subgraph(cluster)

        Log.debug('Graph definition finalized')
        return graph

    def _check_final_status(self, job, child):
        # order of self._table
        # the dictionary is composed by:
        label = None
        if len(child.edge_info) > 0:
            if job.name in child.edge_info.get("FAILED",{}):
                color = self._table.get(Status.FAILED,None)
                label = child.edge_info["FAILED"].get(job.name,0)[1]
            elif job.name in child.edge_info.get("RUNNING",{}):
                color =  self._table.get(Status.RUNNING,None)
                label = child.edge_info["RUNNING"].get(job.name,0)[1]
            elif job.name in child.edge_info.get("QUEUING",{}):
                color =  self._table.get(Status.QUEUING,None)
            elif job.name in child.edge_info.get("HELD",{}):
                color =  self._table.get(Status.HELD,None)
            elif job.name in child.edge_info.get("DELAYED",{}):
                color =  self._table.get(Status.DELAYED,None)
            elif job.name in child.edge_info.get("UNKNOWN",{}):
                color =  self._table.get(Status.UNKNOWN,None)
            elif job.name in child.edge_info.get("SUSPENDED",{}):
                color =  self._table.get(Status.SUSPENDED,None)
            elif job.name in child.edge_info.get("SKIPPED",{}):
                color =  self._table.get(Status.SKIPPED,None)
            elif job.name in child.edge_info.get("WAITING",{}):
                color =  self._table.get(Status.WAITING,None)
            elif job.name in child.edge_info.get("READY",{}):
                color =  self._table.get(Status.READY,None)
            elif job.name in child.edge_info.get("SUBMITTED",{}):
                color =  self._table.get(Status.SUBMITTED,None)
            else:
                return None, None
            if label and label == 0:
                label = None
            return color,label
        else:
            return None, None

    def _add_children(self, job, exp, node_job, groups, hide_groups):
        if job in self.nodes_plotted:
            return
        self.nodes_plotted.add(job)
        if job.has_children() != 0:
            for child in sorted(job.children, key=lambda k: NaturalSort(k.name)):
                node_child, skip = self._check_node_exists(
                    exp, child, groups, hide_groups)
                color, label = self._check_final_status(job, child)
                if len(node_child) == 0 and not skip:
                    node_child = self._create_node(child, groups, hide_groups)
                    if node_child:
                        exp.add_node(node_child)
                        if color:
                            # label = None doesn't disable label, instead it sets it to nothing and complain about invalid syntax
                            if label:
                                exp.add_edge(pydotplus.Edge(node_job, node_child, style="dashed", color=color, label=label))
                            else:
                                exp.add_edge(pydotplus.Edge(node_job, node_child,style="dashed",color=color))
                        else:
                            exp.add_edge(pydotplus.Edge(node_job, node_child))
                    else:
                        skip = True
                elif not skip:
                    node_child = node_child[0]
                    if color:
                        # label = None doesn't disable label, instead it sets it to nothing and complain about invalid syntax
                        if label:
                            exp.add_edge(pydotplus.Edge(node_job, node_child, style="dashed", color=color, label=label))
                        else:
                            exp.add_edge(pydotplus.Edge(node_job, node_child, style="dashed", color=color))
                    else:
                        exp.add_edge(pydotplus.Edge(node_job, node_child))
                    skip = True
                if not skip:
                    self._add_children(
                        child, exp, node_child, groups, hide_groups)

    def _check_node_exists(self, exp, job, groups, hide_groups):
        skip = False
        if groups and job.name in groups['jobs']:
            group = groups['jobs'][job.name][0]
            node = exp.get_node(group)
            if len(groups['jobs'][job.name]) > 1 or hide_groups:
                skip = True
        else:
            node = exp.get_node(job.name)

        return node, skip

    def _create_node(self, job, groups, hide_groups):
        node = None

        if groups and job.name in groups['jobs'] and len(groups['jobs'][job.name]) == 1:
            if not hide_groups:
                group = groups['jobs'][job.name][0]
                node = pydotplus.Node(group, shape='box3d', style="filled",
                                      fillcolor=self.color_status(groups['status'][group]))
                node.set_name(group.replace('"', ''))

        elif not groups or job.name not in groups['jobs']:
            node = pydotplus.Node(job.name, shape='box', style="filled",
                                  fillcolor=self.color_status(job.status))
        return node

    def generate_output(self, expid, joblist, path, output_format="pdf", packages=None, show=False, groups=dict(), hide_groups=False, job_list_object=None):
        """
        Plots graph for joblist and stores it in a file

        :param hide_groups:
        :param groups:
        :param packages:
        :param path:
        :param expid: experiment's identifier
        :type expid: str
        :param joblist: list of jobs to plot
        :type joblist: List of Job objects
        :param output_format: file format for plot
        :type output_format: str (png, pdf, ps)
        :param show: if true, will open the new plot with the default viewer
        :type show: bool
        :param job_list_object: Object that has the main txt generation method
        :type job_list_object: JobList object
        """
        message = ""
        try:
            Log.info('Plotting...')
            now = time.localtime()
            output_date = time.strftime("%Y%m%d_%H%M", now)
            output_file = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "plot", expid + "_" + output_date + "." +
                                       output_format)

            graph = self.create_tree_list(
                expid, joblist, packages, groups, hide_groups)

            Log.debug("Saving workflow plot at '{0}'", output_file)
            if output_format == "png":
                # noinspection PyUnresolvedReferences
                graph.write_png(output_file)
            elif output_format == "pdf":
                # noinspection PyUnresolvedReferences
                graph.write_pdf(output_file)
            elif output_format == "ps":
                # noinspection PyUnresolvedReferences
                graph.write_ps(output_file)
            elif output_format == "svg":
                # noinspection PyUnresolvedReferences
                graph.write_svg(output_file)
            elif output_format == "txt":
                # JobList object is needed, also it acts as a flag.
                if job_list_object is not None:
                    self.generate_output_txt(
                        expid, joblist, path, job_list_object=job_list_object)
            else:
                raise AutosubmitCritical(
                    'Format {0} not supported'.format(output_format), 7069)
            if output_format != "txt":
                Log.result('Plot created at {0}', output_file)

            # If the txt has been generated, don't make it again.
            if output_format != "txt":
                self.generate_output_txt(expid, joblist, path, "default")

            if show and output_format != "txt":
                try:
                    if sys.platform != "linux":
                        try:
                            subprocess.check_output(["open", output_file])
                        except Exception as e:
                            try:
                                subprocess.check_output(["xdg-open", output_file])
                            except Exception as e:
                                subprocess.check_output(["mimeopen", output_file])
                    else:
                        try:
                            subprocess.check_output(["xdg-open", output_file])
                        except Exception as e:
                            subprocess.check_output(["mimeopen", output_file])

                except subprocess.CalledProcessError:
                    Log.printlog('File {0} could not be opened, only the txt option will show'.format(output_file), 7068)
        except AutosubmitCritical:
            raise
        except BaseException as e:
            try:
                message= str(e)
                message += "\n"+str(e)
                if "GraphViz" in message:
                    message= "Graphviz is not installed. Autosubmit need this system package in order to plot the workflow."
            except Exception as e:
                pass

            Log.printlog("{0}\nSpecified output doesn't have an available viewer installed or graphviz is not installed. The output was only written in txt".format(message),7014)

    def generate_output_txt(self, expid, joblist, path, classictxt=False, job_list_object=None):
        """
        Function that generates a representation of the jobs in a txt file
        :param classictxt:
        :param path:
        :param expid: experiment's identifier
        :type expid: str
        :param joblist: experiment's list of jobs
        :type joblist: list
        :param job_list_object: Object that has the main txt generation method
        :type job_list_object: JobList object
        """
        Log.info('Writing status txt...')

        now = time.localtime()
        output_date = time.strftime("%Y%m%d_%H%M", now)
        file_path = os.path.join(
            BasicConfig.LOCAL_ROOT_DIR, expid, "status", expid + "_" + output_date + ".txt")

        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))

        output_file = open(file_path, 'w+')
        if classictxt:
            for job in joblist:
                log_out = ""
                log_err = ""
                if job.status in [Status.FAILED, Status.COMPLETED]:
                    if type(job.local_logs) is not tuple:
                        job.local_logs = ("","")
                    log_out = path + "/" + job.local_logs[0]
                    log_err = path + "/" + job.local_logs[1]

                output = job.name + " " + \
                    Status().VALUE_TO_KEY[job.status] + \
                    " " + log_out + " " + log_err + "\n"
                output_file.write(output)
        else:
            # Replaced call to function for a call to the function of the object that
            # was previously implemented, nocolor is set to True because we don't want
            # strange ANSI codes in our plain text file
            if job_list_object is not None:
                output_file.write(job_list_object.print_with_status(statusChange=None, nocolor=True, existingList=joblist))
            else:
                output_file.write(
                    "Writing jobs, they're grouped by [FC and DATE] \n")
                self.write_output_txt_recursive(
                    joblist[0], output_file, "", file_path)
            output_file.close()
        Log.result('Status txt created at {0}', output_file)

    def write_output_txt_recursive(self, job, output_file, level, path):
        # log_out = ""
        # log_err = ""
        # + " " + log_out + " " + log_err + "\n"
        output = level + job.name + " " + \
            Status().VALUE_TO_KEY[job.status] + "\n"
        output_file.write(output)
        if job.has_children() > 0:
            for child in job.children:
                self.write_output_txt_recursive(
                    child, output_file, "_" + level, path)

    def generate_output_stats(self, expid, joblist, output_format="pdf", section_summary=False, jobs_summary=False, hide=False, period_ini=None, period_fi=None,
                              show=False, queue_time_fixes=None):
        # type: (str, List[Job], str, bool, bool, bool, datetime.datetime, datetime.datetime, bool, Dict[str, int]) -> None
        """
        Plots stats for joblist and stores it in a file

        :param queue_time_fixes:
        :param expid: experiment's identifier
        :type expid: str
        :param joblist: joblist to plot
        :type joblist: JobList
        :param output_format: file format for plot
        :type output_format: str (png, pdf, ps)
        :param section_summary: if true, will plot a summary of the experiment
        :type section_summary: bool
        :param jobs_summary: if true, will plot a list of jobs summary
        :type jobs_summary: bool
        :param hide: if true, will hide the plot
        :type hide: bool
        :param period_ini: initial datetime of filtered period
        :type period_ini: datetime
        :param period_fi: final datetime of filtered period
        :type period_fi: datetime
        :param show: if true, will open the new plot(s) with the default viewer
        :type show: bool
        """
        Log.info('Creating stats file')
        is_owner, is_eadmin, _ = HelperUtils.check_experiment_ownership(expid, BasicConfig, raise_error=False, logger=Log)
        now = time.localtime()
        output_date = time.strftime("%Y%m%d_%H%M%S", now)
        output_filename = "{}_statistics_{}.{}".format(expid, output_date, output_format)
        output_complete_path_stats = os.path.join(BasicConfig.DEFAULT_OUTPUT_DIR, output_filename)
        is_default_path = True
        if is_owner or is_eadmin:
            HUtils.create_path_if_not_exists_group_permission(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "stats"))
            output_complete_path_stats = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "stats", output_filename)
            is_default_path = False
        else:
            if os.path.exists(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "stats")) and os.access(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "stats"), os.W_OK):
                output_complete_path_stats = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "stats", output_filename)
                is_default_path = False
            elif os.path.exists(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, BasicConfig.LOCAL_TMP_DIR)) and os.access(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, BasicConfig.LOCAL_TMP_DIR), os.W_OK):
                output_complete_path_stats = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, BasicConfig.LOCAL_TMP_DIR,
                                                          output_filename)
                is_default_path = False
        if is_default_path:
            Log.info("You don't have enough permissions to the experiment's ({}) folder. The output file will be created in the default location: {}".format(expid, BasicConfig.DEFAULT_OUTPUT_DIR))
            HUtils.create_path_if_not_exists_group_permission(BasicConfig.DEFAULT_OUTPUT_DIR)

        show = create_stats_report(
            expid, joblist, self.get_general_stats(expid), str(output_complete_path_stats),
            section_summary, jobs_summary, hide, period_ini, period_fi, queue_time_fixes
        )
        if show:
            try:
                _display_file(str(output_complete_path_stats))
            except subprocess.CalledProcessError:
                Log.printlog(
                    'File {0} could not be opened, only the txt option will show'.format(output_complete_path_stats),
                    7068)

    @staticmethod
    def clean_plot(expid):
        """
        Function to clean space on BasicConfig.LOCAL_ROOT_DIR/plot directory.
        Removes all plots except last two.

        :param expid: experiment's identifier
        :type expid: str
        """
        search_dir = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "plot")
        chdir(search_dir)
        files = list(filter(path.isfile, listdir(search_dir)))
        files = [path.join(search_dir, f)
                 for f in files if 'statistics' not in f]
        files.sort(key=lambda x: path.getmtime(x))
        remain = files[-2:]
        filelist = [f for f in files if f not in remain]
        for f in filelist:
            remove(f)
        Log.result("Plots cleaned!\nLast two plots remaining there.\n")

    @staticmethod
    def clean_stats(expid):
        """
        Function to clean space on BasicConfig.LOCAL_ROOT_DIR/plot directory.
        Removes all stats' plots except last two.

        :param expid: experiment's identifier
        :type expid: str
        """
        search_dir = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "plot")
        chdir(search_dir)
        files = list(filter(path.isfile, listdir(search_dir)))
        files = [path.join(search_dir, f) for f in files if 'statistics' in f]
        files.sort(key=lambda x: path.getmtime(x))
        remain = files[-1:]
        filelist = [f for f in files if f not in remain]
        for f in filelist:
            remove(f)
        Log.result("Stats cleaned!\nLast stats' plot remaining there.\n")

    @staticmethod
    def get_general_stats(expid):
        # type: (str) -> List
        """
        Returns all the options in the sections of the %expid%_GENERAL_STATS. Options with values larger than GENERAL_STATS_OPTION_MAX_LENGTH characters are not added.

        :param expid: experiment's identifier  
        :type expid: str  
        :return: list of tuples (section, ''), (option, value), (option, value), (section, ''), (option, value), ...  
        :rtype: list  
        """
        general_stats = []
        general_stats_path = os.path.join(
            BasicConfig.LOCAL_ROOT_DIR, expid, "tmp", expid + "_GENERAL_STATS")
        if os.path.exists(general_stats_path):
            parser = AutosubmitConfig.get_parser(
                YAMLParserFactory(), general_stats_path)
            for section in parser.sections():
                general_stats.append((section, ''))
                general_stats += parser.items(section)
        result = []
        for stat_item in general_stats:
            try:
                key, value = stat_item
                if len(value) > GENERAL_STATS_OPTION_MAX_LENGTH:
                    Log.critical("General Stats {}: The value for the key \"{}\" is too long ({} characters) and won't be added to the general_stats plot. Maximum length allowed: {} characters.".format(general_stats_path, key, len(value), GENERAL_STATS_OPTION_MAX_LENGTH))
                    continue
                result.append(stat_item)
            except Exception as e:
                Log.error("Error while processing general_stats of {}".format(expid))
        return result
