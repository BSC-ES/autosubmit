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

"""Code to handle upgrading Autosubmit scripts between AS versions."""

import locale
import re
import shutil
from io import StringIO
from pathlib import Path
from typing import Any, Optional

from configobj import ConfigObj
from ruamel.yaml import YAML

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.database.db_common import (
    update_experiment_description_version
)
from autosubmit.experiment.experiment_common import check_ownership
from autosubmit.helpers.version import get_version
from autosubmit.log.log import Log

__all__ = [
    'ini_to_yaml',
    'upgrade_scripts'
]

_LISTS_REGEX = re.compile(r"""
    =\s*                 # equals sign + optional spaces
    \[                   # opening bracket
    (?P<content>         # capture group "content"
        [^]]*            # anything except closing bracket
    )
    ]                    # closing bracket
""", re.VERBOSE | re.IGNORECASE)
"""Regex to match patterns like '= [bla]', '= [a, b, c]'."""


def _replace_list(match):
    """Convert '= [a, b]' by ='= "a, b"'."""
    content = match.group("content").strip()
    return f"= \"{content}\""


def _update_dict(config_obj: ConfigObj) -> dict[str, Any]:
    """Convert a ConfigObj dictionary to a nested dictionary.

    It is iterative, not recursive, so no risk of YAML-bomb files causing stack issues.

    :param config_obj: The original ConfigObj instance (dictionary-compatible).
    :return: The resulting dictionary.
    """
    yaml_dict: dict[str, Any] = {}

    stack: list[tuple[Optional[str], dict[str, Any]]] = [(None, config_obj)]

    while stack:
        current_key, source_dict = stack.pop()

        for k, v in source_dict.items():
            keys = k.split(".")
            current = yaml_dict if not current_key else yaml_dict[current_key]

            # Traverse down the nested keys
            for nested_key in keys[:-1]:
                current = current.setdefault(nested_key, {})

            last_key = keys[-1]

            if isinstance(v, dict):
                # Does the YAML data already contain this, and as a dictionary?
                # If so, we use that.
                existing = current.get(k)
                if not isinstance(existing, dict):
                    current[last_key] = {}

                stack.append((last_key, v))
            else:
                current[last_key] = v
                current.setdefault(last_key, v)

    return yaml_dict


def ini_to_yaml(ini_file: Path) -> Path:
    """Convert an Autosubmit conf file to YAML.

    Creates a backup of the conf file before conversion.

    INI lists such as "a = [b, c]" are converted to YAML lists ["b", "c"].
    An intermediary step is required to convert from "a = [b, c]" to "a = b, c".

    Configuration keys such as "a.b.c" are converted to nested dictionaries
    {"a": {"b": {"c": ...}}}.

    After the conversion, for each INI file we should have the backup INI file
    and the new YAML file.

    If the file name contains "jobs" or "platform" in ANY part of its name,
    then the generated YAML data will become ``{"JOBS": yaml_data}`` or
    ``{"PLATFORMS": yaml_data}``.

    :param ini_file: Path to the AS3 conf file to convert.
    :return: The YAML file.
    """
    yaml_file_path = ini_file.with_suffix(".yml")
    if yaml_file_path.exists():
        Log.debug(f'AS3 conf file {ini_file} not upgraded. YAML file already exists: {yaml_file_path}')
        return yaml_file_path

    # Read the file name from the command line argument
    backup_path = ini_file.parent / f"{ini_file.name}_as_v3_backup"
    if not backup_path.exists():
        Log.info(f"Backup created at {backup_path}")
        shutil.copyfile(ini_file, backup_path)
    else:
        Log.info(f"Backup already exists at {backup_path}")

    encoding = locale.getlocale()[1]
    content = ini_file.read_text(encoding=encoding)
    content = _LISTS_REGEX.sub(_replace_list, content)

    config_dict = ConfigObj(
        StringIO(content),
        stringify=True,
        list_values=False,
        interpolation=False,
        unrepr=False
    )

    yaml_dict = _update_dict(config_dict)

    if "platform" in ini_file.name.lower():
        yaml_dict = {"PLATFORMS": yaml_dict}
    elif "jobs" in ini_file.name.lower():
        yaml_dict = {"JOBS": yaml_dict}

    yaml_file_path = ini_file.with_suffix(".yml")
    with open(yaml_file_path, 'w', encoding=encoding) as yaml_file:
        YAML().dump(yaml_dict, yaml_file)
    return yaml_file_path


def upgrade_scripts(expid: str, files: Optional[list[str]] = None) -> bool:
    """Upgrade scripts from Autosubmit 3 to 4."""
    files_or_extension_patterns: tuple[str, ...] = tuple(files) if files else ("*.conf", "*.CONF")

    Log.info("Checking if experiment exists...")
    check_ownership(expid, raise_error=True)
    as_conf = AutosubmitConfig(expid, BasicConfig, YAMLParserFactory())
    as_conf.reload(force_load=True)
    as_conf.check_conf_files()
    as_conf.load_parameters()

    exp_conf_dir = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf"
    conf_files = {f for pattern in files_or_extension_patterns for f in exp_conf_dir.rglob(pattern)}

    # TODO: Use tqdm to show the user the progress?
    # Convert conf files into YAML files.
    Log.info(f"Upgrading AS3 {len(conf_files)} conf files (.conf) to AS 4 YAML (.yml)...")
    yaml_files = []
    for conf_file in conf_files:
        Log.debug(f'Upgrading AS3 conf file {conf_file} to AS4 YAML')
        try:
            yaml_file = ini_to_yaml(conf_file)
            yaml_files.append(yaml_file)
        except Exception as e:
            Log.warning(f'Failed to upgrade AS3 conf file {conf_file}: {e}')

    warnings = []
    substituted = []

    # Adjust placeholders.
    exp_conf_dir = Path(as_conf.basic_config.LOCAL_ROOT_DIR) / expid / "conf"
    Log.info(f"Fixing placeholder variables (%_%) inside the new {len(yaml_files)} YAML files")
    for yaml_file in yaml_files:
        template_path = exp_conf_dir / Path(yaml_file).name
        try:
            w, s = _fix_placeholders(template_path, as_conf)
            if w != "":
                warnings.append(f"Warnings for: {template_path.name}\n{w}\n")
            if s != "":
                substituted.append(f"Variables changed for: {template_path.name}\n{s}\n")
        except Exception as e:
            Log.printlog(f"Failed to fix placeholders in the new AS4 YAML file {template_path}: {str(e)}")

    # We now must have new YAML files. Let's reload them.
    as_conf.reload(force_load=True)
    as_conf.check_conf_files()
    as_conf.load_parameters()

    # Update files in proj/ folder.
    exp_project_dir = Path(as_conf.get_project_dir())
    template_path = Path()

    Log.info("Looking for %_% variables inside templates")
    for section, value in as_conf.jobs_data.items():
        try:
            template_path = exp_project_dir / Path(value.get("FILE", ""))
            w, s = _fix_placeholders(template_path, as_conf)
            if w != "":
                warnings.append(f"Warnings for: {template_path.name}\n{w}\n")
            if s != "":
                substituted.append(f"Variables changed for: {template_path.name}\n{s}\n")
        except Exception as e:
            Log.printlog(f"Failed to fix placeholders in template file {template_path}: {str(e)}")

    if substituted:
        Log.printlog("\n".join(substituted), Log.RESULT)
    if warnings:
        Log.printlog("\n".join(warnings), Log.ERROR)

    # Commit.
    as_version = get_version()
    Log.info(f"Changing experiment {expid} version from {as_conf.get_version()} to {as_version}")
    as_conf.set_version(as_version)
    update_experiment_description_version(expid, version=as_version)

    return True


def _fix_placeholders(template_or_script_path: Path, as_conf: AutosubmitConfig) -> tuple[list[str], list[str]]:
    """Adjusts Autosubmit 3 placeholders to the new format in Autosubmit 4.

    All variables are made upper case.

    :param template_or_script_path: Path with all the INI configuration files, or Template files with placeholders.
    :param as_conf: Autosubmit configuration object.
    :return: A tuple with a list of warnings and substituted values.
    """
    # Do a backup and tries to update
    warnings = []
    success = []
    Log.info(f"Checking for AS3 conf or templates in: {template_or_script_path}")

    if not template_or_script_path.exists():
        Log.warning(f"Skipping template not found: {template_or_script_path}")
        return warnings, success

    with open(template_or_script_path, 'r', encoding=locale.getlocale()[1]) as f:
        template_or_script_content = f.read()
    # TODO: quite sure this is duplicating work done in the config module, we
    #       can reuse the same code.
    # Look for %_%, and make them all uppercase (Autosubmit 4 default format)
    variables = re.findall('%(?<!%%)[a-zA-Z0-9_.-]+%(?!%%)', template_or_script_content, flags=re.IGNORECASE)
    variables = [variable[1:-1].upper() for variable in variables]
    results: dict[str, set] = {}
    # Change format
    parameters = as_conf.load_parameters()
    for old_format_key in variables:
        for key in parameters.keys():
            last_key = key.split(".")[-1]
            if last_key == old_format_key:
                if old_format_key not in results:
                    results[old_format_key] = set()
                strip_char = "'"
                results[old_format_key].add(f"%{key.strip(strip_char)}%")
    for key, new_key in results.items():
        new_key_list = list(new_key)
        if len(new_key_list) > 1:
            if "JOBS" not in new_key_list[0] and "PLATFORMS" not in new_key_list:
                warnings.append(
                    f"Duplicate variable found: {key} to {new_key}. Adjust your script to use one of these.")
        else:
            new_key = new_key.pop().upper()
            success.append(f"Translated {key} to {new_key}")
            template_or_script_content = re.sub('%(?<!%%)' + key + '%(?!%%)', new_key, template_or_script_content,
                                                flags=re.I)
    # Deletes unused keys from confs
    if 'autosubmit' in template_or_script_path.name.lower():
        template_or_script_content = re.sub('(?m)^( )*(EXPID:)( )*[a-zA-Z0-9._-]*(\n)*', "", template_or_script_content,
                                            flags=re.I)
    # Write the final result
    with open(template_or_script_path, "w") as f:
        f.write(template_or_script_content)

    if not warnings and not success:
        Log.result(f"Completed check for {template_or_script_path}.\nNo %_% variables found.")
    else:
        Log.result(f"Completed check for {template_or_script_path}")

    return warnings, success
