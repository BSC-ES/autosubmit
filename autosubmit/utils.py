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

import re
from pathlib import Path

import networkx as nx
from ruamel.yaml import YAML

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.log.log import AutosubmitCritical
from autosubmit.platforms.locplatform import LocalPlatform

REFERENCE_PATTERN = re.compile(r"%(.*?)%")

__all__ = [
    "as_conf_default_values",
    "create_json",
    "expand_values",
    "get_chunks",
    "get_members",
]


def as_conf_default_values(
    autosubmit_version: str,
    exp_id: str,
    hpc: str = "",
    git_repo: str = "",
    git_branch: str = "main",
    git_as_conf: str = "",
) -> None:
    """Replace default values in as_conf files.

    :param autosubmit_version: autosubmit version
    :param exp_id: experiment id
    :param hpc: platform
    :param git_repo: path to project git repository
    :param git_branch: main branch
    :param git_as_conf: path to as_conf file in git repository
    :return: None
    """
    # open and replace values
    yaml = YAML(typ="rt")
    for as_conf_file in Path(BasicConfig.LOCAL_ROOT_DIR, f"{exp_id}/conf").iterdir():
        as_conf_file_name = as_conf_file.name.lower()
        if as_conf_file_name.endswith((".yml", ".yaml")):
            with open(as_conf_file, "r+") as file:
                yaml_data = yaml.load(file)
                if "CONFIG" in yaml_data:
                    yaml_data["CONFIG"]["AUTOSUBMIT_VERSION"] = autosubmit_version

                if "MAIL" in yaml_data:
                    yaml_data["MAIL"]["NOTIFICATIONS"] = False
                    yaml_data["MAIL"]["TO"] = ""

                if "DEFAULT" in yaml_data:
                    yaml_data["DEFAULT"]["EXPID"] = exp_id
                    if hpc != "":
                        yaml_data["DEFAULT"]["HPCARCH"] = hpc
                    elif not yaml_data["DEFAULT"]["HPCARCH"]:
                        yaml_data["DEFAULT"]["HPCARCH"] = LocalPlatform.TYPE.value

                if "LOCAL" in yaml_data:
                    yaml_data["LOCAL"]["PROJECT_PATH"] = ""

                if "GIT" in yaml_data:
                    if git_repo != "":
                        yaml_data["GIT"]["PROJECT_ORIGIN"] = f"{git_repo}"
                    if git_branch != "":
                        yaml_data["GIT"]["PROJECT_BRANCH"] = f"{git_branch}"

                if "PROJECT" in yaml_data:
                    if git_repo != "":
                        yaml_data["PROJECT"]["PROJECT_TYPE"] = "git"
                        destination = yaml_data["PROJECT"].get(
                            "PROJECT_DESTINATION", ""
                        )
                        # Overwrite only if empty
                        if not str(destination).strip():
                            yaml_data["PROJECT"]["PROJECT_DESTINATION"] = "git_project"

                if "DEFAULT" in yaml_data and git_repo and git_as_conf:
                    yaml_data["DEFAULT"]["CUSTOM_CONFIG"] = f"%PROJDIR%/{git_as_conf}"

            yaml.dump(yaml_data, as_conf_file)


def expand_values(raw_value: str, known_values: list[str]) -> set[str]:
    """Expand ranges, colon, dash, space-separated values.

    'ANY' expands to known_values if given.
    :param raw_value: string with the values to expand
    :param known_values: list of known valuses to expand 'ANY' to
    :return: set of expanded values
    """
    set_known_values: set[str] = set(known_values) if known_values else set()

    if raw_value is None:
        return set_known_values

    value = str(raw_value).strip().upper()
    if not value or value == "ANY":
        return set_known_values

    expanded_values: set[str] = set()
    for token in value.split():
        if "-" in token or ":" in token:
            sep = "-" if "-" in token else ":"
            start, end = token.split(sep, 1)
            expanded_values.update(str(i) for i in range(int(start), int(end) + 1))
        else:
            expanded_values.add(token)
    return expanded_values


def get_chunks(text: list[dict[str, str]]) -> list[str]:
    """Function to get a list of chunks from JSON.

    :param text: JSON member definition
    :return: list of chunks
    """
    data = []
    for element in text:
        if element.find("-") != -1:
            numbers = element.split("-")
            for count in range(int(numbers[0]), int(numbers[1]) + 1):
                data.append(str(count))
        else:
            data.append(element)

    return data


def get_members(text: str) -> list[dict[str, str]]:
    """Function to get a list of members from JSON.

    :param text: JSON member definition.
    :return: list of members
    """
    count = 0
    data = []
    # noinspection PyUnusedLocal
    for element in text:
        if count % 2 == 0:
            ms = {"m": text[count], "cs": get_chunks(text[count + 1])}
            data.append(ms)
            count += 1
        else:
            count += 1

    return data


def create_json(text: str):
    """Function to parse rerun specification from JSON format.

    :param text: text to parse
    :type text: str
    :return: parsed output
    """
    import json

    from pyparsing import nestedExpr

    count = 0
    data = []

    # text = "[ 19601101 [ fc0 [1 2 3 4] fc1 [1] ] 16651101 [ fc0 [1-30 31 32] ] ]"

    def parse_date(datestring):
        result = []
        startindex = datestring.find("(")
        endindex = datestring.find(")")
        if startindex > 0 and endindex > 0:
            try:
                startstring = datestring[:startindex]
                startrange = datestring[startindex + 1 :].split("-")[0]
                endrange = datestring[startindex:-1].split("-")[1]
                startday = int(startrange[-2:])
                endday = int(endrange[-2:])

                frommonth = int(startrange[:2])
                tomonth = int(endrange[:2])

                for i in range(frommonth, tomonth + 1):
                    for j in range(startday, endday + 1):
                        result.append(startstring + f"{i:.2d}" + f"{j:.2d}")
            except Exception as exp:
                raise AutosubmitCritical(
                    f"Autosubmit couldn't parse your input format. Exception: {exp}"
                )

        else:
            result = [datestring]
        return result

    out = nestedExpr("[", "]").parseString(text).asList()

    # noinspection PyUnusedLocal
    for element in out[0]:
        if count % 2 == 0:
            datelist = parse_date(out[0][count])
            for item in datelist:
                sd = {"sd": item, "ms": get_members(out[0][count + 1])}
                data.append(sd)
            count += 1
        else:
            count += 1

    sds = {"sds": data}
    result = json.dumps(sds)
    return result

def resolve_path(data, path):
    """Resolve a dotted path, such as B.E.F, in a nested dictionary."""
    current = data

    for key in path.split("."):
        if not isinstance(current, dict) or key not in current:
            return False, None

        current = current[key]

    return True, current


def build_dependency_graph(data):
    """
    Build a directed graph from a nested dictionary.

    An edge A -> B means that evaluating A requires B.
    """
    graph = nx.DiGraph()
    unresolved = []

    def visit(value, path):
        graph.add_node(path)

        if isinstance(value, dict):
            for key, child in value.items():
                child_path = f"{path}.{key}" if path else str(key)

                # Evaluating a dictionary requires evaluating its children.
                graph.add_edge(path, child_path, kind="structure")

                visit(child, child_path)

        elif isinstance(value, str):
            for reference in REFERENCE_PATTERN.findall(value):
                reference = reference.strip()

                exists, _ = resolve_path(data, reference)

                if exists:
                    graph.add_edge(
                        path,
                        reference,
                        kind="reference",
                    )
                else:
                    unresolved.append((path, reference))

    for key, value in data.items():
        visit(value, str(key))

    return graph, unresolved


def validate_dependencies(data):
    graph, unresolved = build_dependency_graph(data)
    # Find cycles in the directed graph.
    cycles = list(nx.simple_cycles(graph))

    if unresolved:
        for variable in unresolved:
            path_current = path_normal = variable[1]

            aux_current_path = "CURRENT_"+path_current.split(".")[-1]
            final_current_path = path_current.split(".")[-1] = aux_current_path

            invalid_normal_path, _ = resolve_path(data, path_normal)
            invalid_current_path, _ = resolve_path(data, final_current_path)

            if invalid_normal_path or invalid_current_path or cycles:
                raise AutosubmitCritical(
                    f"Recursion was found validating the configuration files! \nPlease double check the following variable(s) {cycles}"
                )
