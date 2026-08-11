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

from pathlib import Path

from ruamel.yaml import YAML

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.log.log import AutosubmitCritical
from autosubmit.platforms.locplatform import LocalPlatform

__all__ = [
    "as_conf_default_values",
    "create_json",
    "expand_values",
    "get_chunks",
    "get_members",
]


def as_conf_default_values(autosubmit_version: str, exp_id: str, hpc: str = "", minimal_configuration: bool = False,
                           git_repo: str = "", git_branch: str = "main", git_as_conf: str = "") -> None:
    """Replace default values in as_conf files.

    :param autosubmit_version: autosubmit version
    :param exp_id: experiment id
    :param hpc: platform
    :param minimal_configuration: minimal configuration
    :param git_repo: path to project git repository
    :param git_branch: main branch
    :param git_as_conf: path to as_conf file in git repository
    :return: None
    """
    # open and replace values
    yaml = YAML(typ='rt')
    for as_conf_file in Path(BasicConfig.LOCAL_ROOT_DIR, f"{exp_id}/conf").iterdir():
        as_conf_file_name = as_conf_file.name.lower()
        if as_conf_file_name.endswith(('.yml', '.yaml')):
            with open(as_conf_file, 'r+') as file:
                yaml_data = yaml.load(file)
                if 'CONFIG' in yaml_data:
                    yaml_data['CONFIG']['AUTOSUBMIT_VERSION'] = autosubmit_version

                if 'MAIL' in yaml_data:
                    yaml_data['MAIL']['NOTIFICATIONS'] = False
                    yaml_data['MAIL']['TO'] = ""

                if 'DEFAULT' in yaml_data:
                    yaml_data['DEFAULT']['EXPID'] = exp_id
                    if hpc != "":
                        yaml_data['DEFAULT']['HPCARCH'] = hpc
                    elif not yaml_data['DEFAULT']['HPCARCH']:
                        yaml_data['DEFAULT']['HPCARCH'] = LocalPlatform.TYPE.value

                if 'LOCAL' in yaml_data:
                    yaml_data['LOCAL']['PROJECT_PATH'] = ""

                if 'GIT' in yaml_data:
                    if git_repo != "":
                        yaml_data['GIT']['PROJECT_ORIGIN'] = f'{git_repo}'
                    if git_branch != "":
                        yaml_data['GIT']['PROJECT_BRANCH'] = f'{git_branch}'
                
                if 'PROJECT' in yaml_data:
                    if git_repo != "":
                        yaml_data['PROJECT']['PROJECT_TYPE'] = 'git'
                        destination = yaml_data['PROJECT'].get('PROJECT_DESTINATION', '')
                        # Overwrite only if empty
                        if not str(destination).strip():
                            yaml_data['PROJECT']['PROJECT_DESTINATION'] = 'git_project'

                if 'DEFAULT' in yaml_data and git_repo and git_as_conf:
                    yaml_data['DEFAULT']['CUSTOM_CONFIG'] = f"%PROJDIR%/{git_as_conf}"

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
