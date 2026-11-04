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

"""Autosubmit monitor."""

import os
import subprocess
import time
from collections.abc import Callable
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from sys import platform
from typing import Any

import py3dotplus as pydotplus

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.helpers.utils import NaturalSort, check_experiment_ownership
from autosubmit.job.job import Job, WrapperJob
from autosubmit.job.job_common import Status
from autosubmit.log.log import AutosubmitCritical, Log
from autosubmit.monitor.diagram import create_stats_report

_GENERAL_STATS_OPTION_MAX_LENGTH = 1000
"""Maximum length used in the stats plot."""

_DEFAULT_MKDIR_GROUP_PERMISSION = 0o744
"""Default mkdir mode constant.

This permission mode was previously defined in (now removed)
``autosubmit.history.utils.create_path_if_not_exists_group_permission``
"""

_MONITOR_STATUS_TO_COLOR: dict[int, str] = {
    Status.UNKNOWN: "white",
    Status.WAITING: "gray",
    Status.READY: "lightblue",
    Status.PREPARED: "skyblue",
    Status.SUBMITTED: "cyan",
    Status.HELD: "salmon",
    Status.QUEUING: "pink",
    Status.RUNNING: "green",
    Status.COMPLETED: "yellow",
    Status.FAILED: "red",
    Status.DELAYED: "lightcyan",
    Status.SUSPENDED: "orange",
    Status.SKIPPED: "lightyellow",
}
"""Conversion dict, from status to color."""

_CHECK_STATUS_STATUS_LIST = [
    Status.FAILED,
    Status.RUNNING,
    Status.QUEUING,
    Status.HELD,
    Status.DELAYED,
    Status.UNKNOWN,
    Status.SUSPENDED,
    Status.SKIPPED,
    Status.WAITING,
    Status.READY,
    Status.SUBMITTED,
]
"""A list of statuses used by ``check_final_status`` function to return a lis of colors and labels.

Note, some statuses are not used, like ``Status.COMPLETED`` and ``Status.PREPARED``.
"""


def _display_file_xdg(a_file: str | Path) -> None:
    """Display the PDF for the user.

    Tries to use the X Desktop Group tool ``xdg-open``. If that fails,
    it falls back to ``mimeopen``. If this latter fails too, then it
    propagates the possible ``subprocess.CalledProcessError`` exception,
    or another exception or error raised.

    :param a_file: A file to be displayed.
    :raises subprocess.CalledProcessError: Raised by ``subprocess.check_output`` of
        either ``xdg-open`` or ``mimeopen``.
    """
    try:
        subprocess.check_output(["xdg-open", a_file])
    except subprocess.CalledProcessError:
        subprocess.check_output(["mimeopen", a_file])


def _display_file(a_file: str | Path) -> None:
    """Display a file for the user.

    The file is displayed using the user-preferred application.
    This is achieved first checking if the user is on Linux or not.
    If not, we try to use ``open``.

    If ``open`` fails, then we try the same approach used on Linux
    (maybe it is not macOS nor windows?).

    But if the user is already on Linux, then we simply call
    ``xdg-open``, and if ``xdg-open`` fails, we still fall back to ``mimeopen``.

    :param a_file: A file to be displayed.
    :raises subprocess.CalledProcessError: Raised when the file cannot be opened.
    """
    if platform != "linux":
        try:
            subprocess.check_output(["open", a_file])
        except subprocess.CalledProcessError:
            _display_file_xdg(a_file)
    else:
        _display_file_xdg(a_file)


def _color_status(status: int) -> str:
    """Return the colour associated with the given ``status``.

    :param status: Status code.
    """
    if status not in _MONITOR_STATUS_TO_COLOR:
        return _MONITOR_STATUS_TO_COLOR[Status.UNKNOWN]

    return _MONITOR_STATUS_TO_COLOR[status]


def _check_node_exists(
    exp: pydotplus.Subgraph, job: Job, groups: dict[str, Any], hide_groups: bool
) -> tuple[list[pydotplus.Node], bool]:
    """Check if a node exists and if it must be skipped or not.

    If the list of ``groups`` is empty, or if the given ``job`` name is not listed
    in the groups, this function will return the ``node`` for the experiment job,
    and a boolean flag ``False``, indicating that the ``node`` must not be skipped.

    Otherwise, if the ``job`` name exists in the list of ``groups`` and there
    are more than one group or if the flag ``hide_groups`` is ``True``, the
    function returns the experiment ``node`` followed by ``True``, indicating
    that the ``node`` must be skipped.

    Finally, if the ``job`` name exists in the list of ``groups``, ``hide_groups``
    is ``False``, and there is a single element in the groups for that ``job``
    name, then this function returns the ``node`` and ``False``, indicating that
    the ``node`` must not be skipped.

    :param exp: A graph.
    :param job: A job.
    :param groups: Dictionary with grouping information.
    :param hide_groups: If ``True``, skip all groups.
    :return: A tuple composed of a ``pydotplus.Node`` node element, and a boolean flag
        indicating whether the node must be skipped.
    """
    if not groups:
        node: list[pydotplus.Node] = exp.get_node(job.name)
        return node, False

    if job.name not in groups["jobs"]:
        node: list[pydotplus.Node] = exp.get_node(job.name)
        return node, False

    job_groups = groups["jobs"][job.name]
    group = job_groups[0]
    node: list[pydotplus.Node] = exp.get_node(group)

    if hide_groups:
        return node, True

    if len(job_groups) > 1:
        return node, True

    return node, False


def _create_node(
    job: Job, groups: dict[str, Any], hide_groups: bool
) -> "pydotplus.Node | None":
    """Create a node object for a graph.

    If the list of ``groups`` is provided and contains only the ``job`` and no other elements,
    then, unless ``hide_groups`` is `True`, a new ``Node`` object will be created for the job
    and returned.

    But if there are no ``groups`` or if the ``job`` name does not exist in the list of
    ``groups``, then it will immediately create a new ``Node`` object and return it.

    Finally, if none of the options above match (i.e. job in ``groups`` but ``hide_groups``,
    or no ``groups`` provided or ``job`` name not found in the ``groups``), then this function
    will return ``None``.

    :param job: A job.
    :param groups: Dictionary with grouping information.
    :param hide_groups: If ``True``, skip all groups.
    :return: A new ``Node`` object or ``None``.
    """
    if not groups:
        return pydotplus.Node(
            job.name, shape="box", style="filled", fillcolor=_color_status(job.status)
        )

    if job.name not in groups["jobs"]:
        return pydotplus.Node(
            job.name, shape="box", style="filled", fillcolor=_color_status(job.status)
        )

    if hide_groups:
        return None

    if len(groups["jobs"][job.name]) > 1:
        return None

    group = groups["jobs"][job.name][0]
    node = pydotplus.Node(
        group,
        shape="box3d",
        style="filled",
        fillcolor=_color_status(groups["status"][group]),
    )
    node.set_name(group.replace('"', ""))
    return node


def _check_final_status(
    job_edges_info: list[dict[str, Any]] | None,
    child: Job,
) -> tuple[str | None, str | None, bool | None]:
    """Check the final status between a job and its child using edge information.

    :param job_edges_info: List of edge information dictionaries.
    :param child: The child job.
    :return: Tuple of color, label, and ``fail_ok`` flag, or (``None``, ``None``, ``None``) if not found.
    """
    if not job_edges_info:
        return None, None, None

    # Find the edge info for the child
    child_edge_info = None
    for out_edge in job_edges_info:
        if child.name == out_edge["e_to"]:
            child_edge_info = out_edge
            break

    if not child_edge_info:
        return None, None, None

    status_id = Status.KEY_TO_VALUE[child_edge_info["min_trigger_status"]]
    if child_edge_info["min_trigger_status"] == "COMPLETED":
        # Avoid "yellow" arrows for completed jobs (old normal behaviour)
        edge_color = "black"
    else:
        edge_color = _color_status(status_id)
    label = (
        str(child_edge_info["from_step"]) if child_edge_info["from_step"] > 0 else None
    )
    fail_ok = child_edge_info.get("fail_ok", False)

    return edge_color, label, fail_ok


def _delete_files_but_two_newest(
    expid: str, directory: str, _filter: Callable[[Path], bool]
) -> None:
    """Clean files in an experiment directory while keeping the newest two.

    Removes all files that pass the filter, keeping the newest two files only.

    :param expid: Experiment identifier.
    :param directory: Directory below the experiment root to clean.
    :param _filter: A filter to apply to each file in the directory.
    """
    search_dir = Path(BasicConfig.LOCAL_ROOT_DIR, expid, directory)
    search_dir_files = [f for f in search_dir.iterdir() if f.is_file() and _filter(f)]

    search_dir_files.sort(key=lambda f: f.stat().st_mtime)
    keep_these_files = search_dir_files[-2:]

    to_be_deleted = [f for f in search_dir_files if f not in keep_these_files]
    for f in to_be_deleted:
        f.unlink()


def clean_plot(expid: str) -> None:
    """Clean space on ``BasicConfig.LOCAL_ROOT_DIR/<expid>/plot`` directory.

    Removes all plots except the last two.

    :param expid: Experiment identifier.
    """
    _delete_files_but_two_newest(expid, "plot", lambda f: "statistics" not in f.name)


def clean_stats(expid: str) -> None:
    """Clean space on ``BasicConfig.LOCAL_ROOT_DIR/<expid>/stats`` directory.

    Removes all stats except the last two.

    :param expid: Experiment identifier.
    """
    _delete_files_but_two_newest(expid, "stats", lambda f: "statistics" in f.name)


# TODO: This class can be replaced by module-level functions. There is no need to have
#       an extra layer holding state. The ``monitor.nodes_plotted`` can exist within ``create_list``,
#       passed to ``_add_children`` or queried just before ``_add_child`` is called.
#       This removes state, which is where bugs are easier to be found, and makes the objects
#       (a module is an object, in a certain way?) leaner -- good for memory/performance, but
#       also makes writing unit tests A LOT easier.
class Monitor:
    """Class to handle monitoring of Jobs at HPC.

    :ivar edge_info: Dictionary with edge information between jobs.
    :ivar nodes_plotted: Set of nodes that have already been plotted.
    """

    def __init__(self, edge_info: dict[str, Any] | None = None) -> None:
        """Initialize the Monitor class.

        :param edge_info: Dictionary with edge information between jobs.
        """
        if not edge_info:
            self.edge_info = {}
        else:
            self.edge_info = edge_info

        self.nodes_plotted = None

    def create_tree_list(
        self,
        expid: str,
        joblist: list[Job],
        packages: list[WrapperJob],
        groups: dict[str, list[Job] | dict],
        hide_groups: bool = False,
    ) -> pydotplus.Dot:
        """Create graph from joblist.

        :param expid: Experiment identifier.
        :param joblist: Job list to plot.
        :param packages: List of wrapper jobs.
        :param groups: Dictionary with grouping information.
        :param hide_groups: If ``True``, hide groups in the visualization.
        :return: Created graph object.
        """
        Log.debug("Creating workflow graph...")
        graph = pydotplus.Dot(graph_type="digraph")

        Log.debug("Creating legend...")
        legend = pydotplus.Subgraph(graph_name="Legend", label="Legend", rank="source")

        for status_txt in Status.LOGICAL_ORDER:
            style = (
                "" if status_txt == Status.VALUE_TO_KEY[Status.UNKNOWN] else "filled"
            )
            color = _color_status(Status.KEY_TO_VALUE[status_txt])
            node = pydotplus.Node(
                name=status_txt, shape="box", style=style, fillcolor=color
            )
            legend.add_node(node)

        graph.add_subgraph(legend)

        exp = pydotplus.Subgraph(graph_name="Experiment", label=expid)
        self.nodes_plotted = set()
        Log.debug("Creating job graph...")

        for job in joblist:
            if job.has_parents():
                continue

            if (
                not groups
                or job.name not in groups["jobs"]
                or (job.name in groups["jobs"] and len(groups["jobs"][job.name]) == 1)
            ):
                node_job = pydotplus.Node(
                    job.name,
                    shape="box",
                    style="filled",
                    fillcolor=_color_status(job.status),
                )

                if groups and job.name in groups["jobs"]:
                    group = groups["jobs"][job.name][0]
                    node_job.obj_dict["name"] = group
                    node_job.obj_dict["attributes"]["fillcolor"] = _color_status(
                        groups["status"][group]
                    )
                    node_job.obj_dict["attributes"]["shape"] = "box3d"

                exp.add_node(node_job)
                self._add_children(job, exp, node_job, groups, hide_groups)

        if groups:
            if not hide_groups:
                for job, group in groups["jobs"].items():
                    if len(group) > 1:
                        group_name = "cluster_" + "_".join(group)
                        if group_name not in graph.obj_dict["subgraphs"]:
                            subgraph = pydotplus.graphviz.Cluster(
                                graph_name="_".join(group)
                            )
                            subgraph.obj_dict["attributes"]["color"] = "invis"
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
                                node.obj_dict["name"], previous_node.obj_dict["name"]
                            )
                            if len(edge) == 0:
                                edge = pydotplus.Edge(previous_node, node)
                                edge.obj_dict["attributes"]["dir"] = "none"
                                # constraint false allows the horizontal alignment
                                edge.obj_dict["attributes"]["constraint"] = "false"
                                edge.obj_dict["attributes"]["penwidth"] = 4
                                subgraph.add_edge(edge)

                            previous_node = node
                        if group_name not in graph.obj_dict["subgraphs"]:
                            graph.add_subgraph(subgraph)
            else:
                for edge in deepcopy(exp.obj_dict["edges"]):
                    if edge[0].replace('"', "") in groups["status"]:
                        del exp.obj_dict["edges"][edge]

            graph.set_strict(True)

        graph.add_subgraph(exp)

        packages_subgraphs_dict = {}
        if packages:
            Log.debug("Creating wrapper jobs graph...")
            # Wrapper visualization
            for node in exp.get_nodes():
                name = node.obj_dict["name"]
                for wrapper_job in packages:
                    for job in wrapper_job.job_list:
                        if name == job.name:
                            package = wrapper_job.name
                            if package not in packages_subgraphs_dict:
                                packages_subgraphs_dict[package] = (
                                    pydotplus.graphviz.Cluster(graph_name=package)
                                )
                                packages_subgraphs_dict[package].obj_dict["attributes"][
                                    "color"
                                ] = "black"
                                packages_subgraphs_dict[package].obj_dict["attributes"][
                                    "style"
                                ] = "dashed"
                            packages_subgraphs_dict[package].add_node(node)
                            break

        for package, cluster in packages_subgraphs_dict.items():
            graph.add_subgraph(cluster)

        Log.debug("Graph definition finalized")
        return graph

    def _add_children(
        self,
        job: Job,
        exp: pydotplus.Subgraph,
        node_job: pydotplus.Node,
        groups: dict[str, Any],
        hide_groups: bool,
    ) -> None:
        """Add children nodes to the graph recursively.

        :param job: The parent job.
        :param exp: The graph subgraph.
        :param node_job: The parent node.
        :param groups: Dictionary with grouping information.
        :param hide_groups: If ``True``, hide groups in the visualization.
        """
        if job in self.nodes_plotted:
            return
        self.nodes_plotted.add(job)
        if job.has_children() != 0:
            for child in sorted(job.children, key=lambda k: NaturalSort(k.name)):
                node_child, skip = _check_node_exists(exp, child, groups, hide_groups)
                color, label, fail_ok = _check_final_status(
                    self.edge_info.get(job.name, None), child
                )
                if fail_ok:
                    style = "dotted"
                else:
                    style = "solid"
                if len(node_child) == 0 and not skip:
                    node_child = _create_node(child, groups, hide_groups)
                    if node_child:
                        exp.add_node(node_child)
                        if color:
                            # label = None doesn't disable label, instead it sets it to nothing and complain about invalid syntax
                            if label:
                                exp.add_edge(
                                    pydotplus.Edge(
                                        node_job,
                                        node_child,
                                        style=style,
                                        color=color,
                                        label=label,
                                    )
                                )
                            else:
                                exp.add_edge(
                                    pydotplus.Edge(
                                        node_job, node_child, style=style, color=color
                                    )
                                )
                        else:
                            exp.add_edge(pydotplus.Edge(node_job, node_child))
                    else:
                        skip = True
                elif not skip:
                    node_child = node_child[0]
                    if color:
                        # label = None doesn't disable label, instead it sets it to nothing and complain about invalid syntax
                        if label:
                            exp.add_edge(
                                pydotplus.Edge(
                                    node_job,
                                    node_child,
                                    style=style,
                                    color=color,
                                    label=label,
                                )
                            )
                        else:
                            exp.add_edge(
                                pydotplus.Edge(
                                    node_job, node_child, style=style, color=color
                                )
                            )
                    else:
                        exp.add_edge(pydotplus.Edge(node_job, node_child))
                    skip = True
                if not skip:
                    self._add_children(child, exp, node_child, groups, hide_groups)

    def generate_output(
        self,
        expid: str,
        joblist: list[Job],
        path: str,
        output_format: str = "pdf",
        packages: list[WrapperJob] | None = None,
        show: bool = False,
        groups: dict[str, Any] | None = None,
        hide_groups: bool = False,
        job_list_object=None,
    ) -> None:
        """Plot graph for joblist and store it in a file.

        :param expid: Experiment identifier.
        :param joblist: List of jobs to plot.
        :param path: Path to the experiment.
        :param output_format: File format for plot.
        :param packages: List of wrapper jobs.
        :param show: If ``True``, will open the new plot with the default viewer.
        :param groups: Dictionary with grouping information.
        :param hide_groups: If ``True``, hide groups in the visualization.
        :param job_list_object: Object that has the main txt generation method.
        :raises AutosubmitCritical: Raised when an unsupported format is requested.
        """
        if groups is None:
            groups = {}
        try:
            Log.info("Plotting...")
            now = time.localtime()
            output_date = time.strftime("%Y%m%d_%H%M%S", now)
            plot_file_name = f"{expid}_{output_date}.{output_format}"
            output_file = Path(
                BasicConfig.LOCAL_ROOT_DIR, expid, "plot", plot_file_name
            )
            graph = self.create_tree_list(expid, joblist, packages, groups, hide_groups)

            Log.info(f"Saving workflow plot at '{output_file}'")
            if output_format == "png":
                # noinspection PyUnresolvedReferences
                graph.write_png(str(output_file))
            elif output_format == "pdf":
                # noinspection PyUnresolvedReferences
                graph.write_pdf(str(output_file))
            elif output_format == "ps":
                # noinspection PyUnresolvedReferences
                graph.write_ps(str(output_file))
            elif output_format == "svg":
                # noinspection PyUnresolvedReferences
                graph.write_svg(str(output_file))
            elif output_format == "txt":
                # JobList object is needed, also it acts as a flag.
                if job_list_object is not None:
                    self.generate_output_txt(
                        expid, joblist, path, job_list_object=job_list_object
                    )
            else:
                raise AutosubmitCritical(f"Format {output_format} not supported", 7069)
            if output_format != "txt":
                Log.result(f"Plot created at {output_file}")
                # If the txt has been generated, don't make it again.
                self.generate_output_txt(expid, joblist, path, True)

                if show:
                    try:
                        _display_file(output_file)
                    except subprocess.CalledProcessError:
                        error_msg = f"File {output_file} could not be opened, only the txt option will shown"
                        Log.printlog(error_msg, 7068)
        except AutosubmitCritical:
            raise
        except Exception as e:
            message = str(e)
            if "GraphViz" in message:
                message = "Graphviz is not installed. Autosubmit needs this system package to plot the workflow."
                Log.printlog(message, 7014)
            else:
                Log.printlog(str(e), 7014)
        Log.result("Plotting finished")

    def generate_output_txt(
        self,
        expid: str,
        joblist: list[Job],
        path: str,
        classictxt: bool = False,
        job_list_object=None,
    ) -> None:
        """Generate a representation of the jobs in a txt file.

        :param expid: Experiment identifier.
        :param joblist: Experiment's list of jobs.
        :param path: Path to the experiment.
        :param classictxt: If ``True``, generate classic text format.
        :param job_list_object: Object that has the main txt generation method.
        """
        Log.info("Writing status txt...")

        now = time.localtime()
        output_date = time.strftime("%Y%m%d_%H%M%S", now)

        status_dir = Path(BasicConfig.LOCAL_ROOT_DIR, expid, "status")
        if not status_dir.exists():
            status_dir.mkdir()

        file_path = status_dir / f"{expid}_{output_date}.txt"

        with open(file_path, "w+") as output_file:
            if classictxt:
                for job in joblist:
                    log_out = ""
                    log_err = ""
                    if job.status in [Status.FAILED, Status.COMPLETED]:
                        if type(job.local_logs) is not tuple:
                            job.local_logs = ("", "")
                        if job.local_logs[0]:
                            log_out = path + "/" + job.local_logs[0]
                        if job.local_logs[1]:
                            log_err = path + "/" + job.local_logs[1]

                    output = f"{job.name} {Status.VALUE_TO_KEY[job.status]} {log_out} {log_err}\n"
                    output_file.write(output)
            else:
                # Replaced call to function for a call to the function of the object that
                # was previously implemented, nocolor is set to True because we don't want
                # strange ANSI codes in our plain text file
                if job_list_object is not None:
                    job_list_txt = job_list_object.print_with_status(
                        status_change=None, nocolor=True, existing_list=joblist
                    )
                    output_file.write(job_list_txt)
                else:
                    output_file.write(
                        "Writing jobs, they're grouped by [FC and DATE] \n"
                    )
                    self._write_output_txt_recursive(joblist[0], output_file, "")
        Log.result("Status txt created at {0}", output_file)

    def _write_output_txt_recursive(self, job: Job, output_file, level: str) -> None:
        """Write job information to output file recursively.

        :param job: The job to write.
        :param output_file: The file object to write to.
        :param level: Indentation level.
        """
        output = f"{level}{job.name} {Status.VALUE_TO_KEY[job.status]} \n"
        output_file.write(output)
        if job.has_children() > 0:
            for child in job.children:
                self._write_output_txt_recursive(child, output_file, "_" + level)

    def generate_output_stats(
        self,
        expid: str,
        joblist: list[Job],
        output_format: str = "pdf",
        hide: bool = False,
        section_summary: bool = False,
        jobs_summary: bool = False,
        period_ini: datetime | None = None,
        period_fi: datetime | None = None,
        queue_time_fixes: dict[str, int] | None = None,
    ) -> bool:
        """Plot stats for joblist and store it in a file.

        :param expid: Experiment identifier.
        :param joblist: Job list to plot.
        :param output_format: File format for plot.
        :param hide: If ``True``, will not open the new plot(s) with the default viewer.
        :param section_summary: If ``True``, will plot a summary of the experiment.
        :param jobs_summary: If ``True``, will plot a list of jobs summary.
        :param period_ini: Initial datetime of filtered period.
        :param period_fi: Final datetime of filtered period.
        :param queue_time_fixes: Dictionary with queue time fixes for specific jobs.
        :return: ``True`` if the report was generated successfully, ``False`` otherwise.
        """
        Log.info("Creating stats file")
        is_owner, is_eadmin, _ = check_experiment_ownership(
            expid, BasicConfig, raise_error=False, logger=Log
        )
        now = time.localtime()
        output_date = time.strftime("%Y%m%d_%H%M%S", now)
        output_filename = f"{expid}_statistics_{output_date}.{output_format}"
        output_complete_path_stats = os.path.join(
            BasicConfig.DEFAULT_OUTPUT_DIR, output_filename
        )
        is_default_path = True
        if is_owner or is_eadmin:
            Path(BasicConfig.LOCAL_ROOT_DIR, expid, "stats").mkdir(
                mode=_DEFAULT_MKDIR_GROUP_PERMISSION, parents=True, exist_ok=True
            )
            output_complete_path_stats = os.path.join(
                BasicConfig.LOCAL_ROOT_DIR, expid, "stats", output_filename
            )
            is_default_path = False
        else:
            if os.path.exists(
                os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "stats")
            ) and os.access(
                os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "stats"), os.W_OK
            ):
                output_complete_path_stats = os.path.join(
                    BasicConfig.LOCAL_ROOT_DIR, expid, "stats", output_filename
                )
                is_default_path = False
            elif os.path.exists(
                os.path.join(
                    BasicConfig.LOCAL_ROOT_DIR, expid, BasicConfig.LOCAL_TMP_DIR
                )
            ) and os.access(
                os.path.join(
                    BasicConfig.LOCAL_ROOT_DIR, expid, BasicConfig.LOCAL_TMP_DIR
                ),
                os.W_OK,
            ):
                output_complete_path_stats = os.path.join(
                    BasicConfig.LOCAL_ROOT_DIR,
                    expid,
                    BasicConfig.LOCAL_TMP_DIR,
                    output_filename,
                )
                is_default_path = False
        if is_default_path:
            Log.info(
                f"You don't have enough permissions to the experiment's ({expid}) folder. The output file will be created in the default location: {BasicConfig.DEFAULT_OUTPUT_DIR}"
            )

            Path(BasicConfig.DEFAULT_OUTPUT_DIR).mkdir(
                mode=_DEFAULT_MKDIR_GROUP_PERMISSION, parents=True, exist_ok=True
            )

        report_created = create_stats_report(
            expid,
            joblist,
            str(output_complete_path_stats),
            section_summary,
            jobs_summary,
            period_ini,
            period_fi,
            queue_time_fixes,
        )
        if hide or not report_created:
            return False

        try:
            _display_file(str(output_complete_path_stats))
        except subprocess.CalledProcessError:
            Log.printlog(
                f"File {output_complete_path_stats} could not be opened, only the txt option will show",
                7068,
            )

        return True
