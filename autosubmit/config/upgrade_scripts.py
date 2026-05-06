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


def _config_obj_to_nested_dict(config_obj: ConfigObj) -> dict[str, Any]:
    """Convert a ConfigObj to a nested dictionary.

    e.g., "a.b.c" turns into {"a": {"b": {"c": ...}}}.

    :param config_obj: The ConfigObj to convert.
    :return: The nested dictionary.
    """
    yaml_dict: dict[str, Any] = {}

    for key, value in config_obj.items():
        keys = key.split(".")
        current = yaml_dict

        # Traverse down the nested keys
        for k in keys[:-1]:
            current = current.setdefault(k, {})

        last_key = keys[-1]

        if isinstance(value, dict):
            # Merge dicts recursively
            current.setdefault(last_key, {})
            _update_dict(current[last_key], value)
        else:
            # Only set scalar if it doesn't exist yet
            current.setdefault(last_key, value)

    return yaml_dict


def ini_to_yaml(ini_file: Path) -> Path:
    """Convert an Autosubmit INI file to YAML.

    Creates a backup of the INI file before conversion.

    INI lists such as "a = [b, c]" are converted to YAML lists ["b", "c"].
    An intermediary step is required to convert from "a = [b, c]" to "a = b, c".

    Configuration keys such as "a.b.c" are converted to nested dictionaries
    {"a": {"b": {"c": ...}}}.

    After the conversion, for each INI file we should have the backup INI file
    and the new YAML file.

    If the file name contains "jobs" or "platform" in ANY part of its name,
    then the generated YAML data will become {"JOBS": yaml_data} or
    {"PLATFORMS": yaml_data}.

    :param ini_file: Path to the INI file to convert.
    :return: The YAML file.
    """
    encoding = locale.getlocale()[1]
    # Read the file name from the command line argument
    backup_path = ini_file.parent / f"{ini_file.name}_as_v3_backup"
    if not backup_path.exists():
        Log.info(f"Backup created at {backup_path}")
        shutil.copyfile(ini_file, backup_path)

    content = ini_file.read_text(encoding=encoding)
    content = _LISTS_REGEX.sub(_replace_list, content)

    config_dict = ConfigObj(
        StringIO(content),
        stringify=True,
        list_values=False,
        interpolation=False,
        unrepr=False
    )

    yaml_dict = _config_obj_to_nested_dict(config_dict)

    if "platform" in ini_file.name.lower():
        yaml_dict = {"PLATFORMS": yaml_dict}
    elif "jobs" in ini_file.name.lower():
        yaml_dict = {"JOBS": yaml_dict}

    yaml_file_path = ini_file.with_suffix(".yml")
    with open(yaml_file_path, 'w', encoding=encoding) as yaml_file:
        YAML().dump(yaml_dict, yaml_file)
    return yaml_file_path


def _update_dict(original_dict: dict[str, Any], updated_dict: dict[str, Any]) -> dict[str, Any]:
    """Update a dictionary recursively, merging both, returning the resulting dictionary.

    It is not recursive, so no risk of YAML-bomb files causing stack issues.
    Performs an iterative deep-merge.

    :param original_dict: The original dictionary.
    :param updated_dict: The dictionary to update.
    :return: The resulting dictionary.
    """
    stack: list[tuple[dict[str, Any], dict[str, Any]]] = [(original_dict, updated_dict)]

    while stack:
        target, source = stack.pop()

        for k, v in source.items():
            if isinstance(v, dict):
                existing = target.get(k)

                if not isinstance(existing, dict):
                    existing = {}
                    target[k] = existing

                stack.append((existing, v))
            else:
                target[k] = v

    return original_dict


def upgrade_scripts(expid: str, files: Optional[list[str]] = None) -> bool:
    """Upgrade scripts from Autosubmit 3 to 4."""
    if not files:
        files = ('*.conf', '*.CONF')

    Log.info("Checking if experiment exists...")
    check_ownership(expid, raise_error=True)
    as_conf = AutosubmitConfig(expid, BasicConfig, YAMLParserFactory())
    as_conf.reload(force_load=True)
    as_conf.check_conf_files()
    as_conf.load_parameters()

    exp_conf_dir = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf"
    ini_files = {f for pattern in files for f in exp_conf_dir.rglob(pattern)}

    # TODO: Use tqdm to show the user the progress?
    Log.info(f"Converting {len(ini_files)} INI files (.conf) into YAML files (.yml)")
    yaml_files = []
    for ini_file in ini_files:
        yaml_file = Path(ini_file.stem + ".yml")
        if yaml_file.exists():
            Log.debug(f'INI file {ini_file} not upgraded. YAML file already exists: {yaml_file}')
            continue

        Log.debug(f'Converting INI file {ini_file} into YAML file: {yaml_file}')
        try:
            ini_to_yaml(ini_file)
            yaml_files.append(yaml_file)
        except Exception as e:
            Log.warning(f'Failed to convert INI file {ini_file} into {yaml_file}: {e}')

    warnings = []
    substituted = []

    # Update files in conf/ folder.
    exp_project_dir = Path(as_conf.basic_config.LOCAL_ROOT_DIR) / expid / "proj"
    Log.info(f"Fixing placeholder variables (%_%) inside the new {len(yaml_files)} YAML files")
    for yaml_file in yaml_files:
        template_path = exp_project_dir / Path(yaml_file).name
        try:
            w, s = _update_old_script(exp_project_dir, template_path, as_conf)
            if w != "":
                warnings.append(f"Warnings for: {template_path.name}\n{w}\n")
            if s != "":
                substituted.append(f"Variables changed for: {template_path.name}\n{s}\n")
        except Exception as e:
            Log.printlog(f"Couldn't read {template_path} template.\ntrace:{str(e)}")

    # Update files in proj/ folder.
    exp_project_dir = Path(as_conf.get_project_dir())
    template_path = Path()

    Log.info("Looking for %_% variables inside templates")
    for section, value in as_conf.jobs_data.items():
        try:
            template_path = exp_project_dir / Path(value.get("FILE", ""))
            w, s = _update_old_script(template_path.parent, template_path, as_conf)
            if w != "":
                warnings.append(f"Warnings for: {template_path.name}\n{w}\n")
            if s != "":
                substituted.append(f"Variables changed for: {template_path.name}\n{s}\n")
        except Exception as e:
            Log.printlog(f"Couldn't read {template_path} template.\ntrace:{str(e)}")

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


def _update_old_script(
        root_dir: Path,
        template_path: Path,
        as_conf: AutosubmitConfig
) -> tuple[list[str], list[str]]:
    """Backs up the configuration and tries to update them.

    Returns a tuple with warnings and substituted values.

    Lower case variables are replaced by upper case variables.

    :return: A tuple with a list of warnings and substituted values.
    """
    # Do a backup and tries to update
    warnings = []
    substituted = []
    Log.info(f"Checking {template_path}")
    if template_path.exists():
        backup_path = root_dir / Path(template_path.name + "_AS_v3_backup_placeholders")
        if not backup_path.exists():
            Log.info(f"Backup stored at {backup_path}")
            shutil.copyfile(template_path, backup_path)
        with open(template_path, 'r', encoding=locale.getlocale()[1]) as f:
            template_content = f.read()
        # Look for %_%
        variables = re.findall('%(?<!%%)[a-zA-Z0-9_.-]+%(?!%%)', template_content, flags=re.IGNORECASE)
        variables = [variable[1:-1].upper() for variable in variables]
        results = {}
        # Change format
        for old_format_key in variables:
            for key in as_conf.load_parameters().keys():
                key_affix = key.split(".")[-1]
                if key_affix == old_format_key:
                    if old_format_key not in results:
                        results[old_format_key] = set()

                    results[old_format_key].add("%" + key.strip("'") + "%")
        for key, new_key in results.items():
            if len(new_key) > 1:
                if list(new_key)[0].find("JOBS") > -1 or list(new_key)[0].find("PLATFORMS") > -1:
                    pass
                else:
                    warnings.append(f"{key} couldn't translate to {new_key} since it is a duplicate variable. "
                                    f"Please chose one of the keys value.")
            else:
                new_key = new_key.pop().upper()
                substituted.append(f"{key.upper()} translated to {new_key}")
                template_content = re.sub('%(?<!%%)' + key + '%(?!%%)', new_key, template_content, flags=re.I)
        # write_it
        # Deletes unused keys from confs
        if template_path.name.lower().find("autosubmit") > -1:
            template_content = re.sub('(?m)^( )*(EXPID:)( )*[a-zA-Z0-9._-]*(\n)*', "", template_content, flags=re.I)
        # Write the final result
        with open(template_path, "w") as f:
            f.write(template_content)

    if not warnings and not substituted:
        Log.result(f"Completed check for {template_path}.\nNo %_% variables found.")
    else:
        Log.result(f"Completed check for {template_path}")

    return warnings, substituted
