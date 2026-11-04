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

"""Configuration utility code."""

import locale
import re
import shutil
from collections.abc import Mapping
from io import StringIO
from pathlib import Path
from typing import Any

from configobj import ConfigObj
from ruamel.yaml import YAML

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.log.log import AutosubmitCritical, Log

__all__ = [
    "as_bool",
    "as_float",
    "as_int",
    "as_mapping",
    "copy_as_config",
    "ini_to_yaml",
]

_TRUTHY = frozenset({"true", "1", "yes", "on"})
_FALSY = frozenset({"false", "0", "no", "off"})


def as_mapping(value: Any, name: str) -> Mapping[Any, Any]:
    """Return the value when it is a mapping.

    ``None`` is treated as an empty mapping so optional sections can be omitted.

    :param value: Value to normalize.
    :param name: Configuration key used in the error message.
    :return: The original mapping, or an empty mapping.
    :raises AutosubmitCritical: If the value exists but is not a mapping.
    """
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return value
    raise AutosubmitCritical(
        f"Expected a mapping for {name}, got {type(value).__name__}: {value!r}",
        7014,
    )


def as_bool(value: Any, default: bool, name: str) -> bool:
    if value is None:
        return default
    elif isinstance(value, bool):
        return value
    elif isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _TRUTHY:
            return True
        elif normalized in _FALSY:
            return False
    Log.warning(f"Invalid value for {name}: {value!r}. Using default {default}.")
    return default


def as_int(value: Any, default: int, name: str) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        Log.warning(f"Invalid value for {name}: {value!r}. Using default {default}.")
        return default


def as_float(value: Any, default: float, name: str) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        Log.warning(f"Invalid value for {name}: {value!r}. Using default {default}.")
        return default


_LISTS_REGEX = re.compile(
    r"""
    =\s*                 # equals sign + optional spaces
    \[                   # opening bracket
    (?P<content>         # capture group "content"
        [^]]*            # anything except closing bracket
    )
    ]                    # closing bracket
""",
    re.VERBOSE | re.IGNORECASE,
)
"""Regex to match patterns like '= [bla]', '= [a, b, c]'."""


def _replace_list(match):
    """Convert '= [a, b]' by ='= "a, b"'."""
    content = match.group("content").strip()
    return f'= "{content}"'


def _update_dict(config_obj: ConfigObj) -> dict[str, Any]:
    """Convert a ConfigObj dictionary to a nested dictionary.

    It is iterative, not recursive, so no risk of YAML-bomb files causing stack issues.

    :param config_obj: The original ConfigObj instance (dictionary-compatible).
    :return: The resulting dictionary.
    """
    yaml_dict: dict[str, Any] = {}

    stack: list[tuple[str | None, dict[str, Any]]] = [(None, config_obj)]

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
        Log.debug(
            f"AS3 conf file {ini_file} not upgraded. YAML file already exists: {yaml_file_path}"
        )
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
        unrepr=False,
    )

    yaml_dict = _update_dict(config_dict)

    if "platform" in ini_file.name.lower():
        yaml_dict = {"PLATFORMS": yaml_dict}
    elif "jobs" in ini_file.name.lower():
        yaml_dict = {"JOBS": yaml_dict}

    yaml_file_path = ini_file.with_suffix(".yml")
    with open(yaml_file_path, "w", encoding=encoding) as yaml_file:
        YAML().dump(yaml_dict, yaml_file)
    return yaml_file_path


def copy_as_config(expid: str, copy_expid: str):
    copy_exp_conf_path = Path(BasicConfig.LOCAL_ROOT_DIR, copy_expid, 'conf')

    target_exp_conf_path = Path(BasicConfig.LOCAL_ROOT_DIR, expid, 'conf')
    target_exp_conf_path.mkdir(parents=True, exist_ok=True)

    for conf_file in copy_exp_conf_path.iterdir():
        # Copy only relevant files
        target_new_file = Path(target_exp_conf_path, conf_file.name.replace(copy_expid, expid))
        if conf_file.name.lower().endswith((".conf", ".yml", ".yaml")):
            shutil.copy(conf_file, target_new_file)
            # If ends with ``.conf`` convert it to an Autosubmit 4 YAML file.
            if conf_file.name.endswith(".conf"):
                try:
                    ini_to_yaml(target_new_file)
                except Exception as e:
                    Log.warning(f"Error converting {target_new_file} to YAML: {str(e)}")
