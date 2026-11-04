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
from pathlib import Path

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.config.registry import load_config
from autosubmit.config.utils import ini_to_yaml
from autosubmit.database.db_common import update_experiment_description_version
from autosubmit.experiment.utils import check_ownership, create_required_folders
from autosubmit.helpers.version import get_version
from autosubmit.log.log import Log

__all__ = ["upgrade_scripts"]


def upgrade_scripts(expid: str, files: list[str] | None) -> bool:
    """Upgrade scripts from Autosubmit 3 to 4."""
    files_or_extension_patterns: tuple[str, ...] = (
        tuple(files) if files else ("*.conf", "*.CONF")
    )

    Log.info("Checking if experiment exists...")
    check_ownership(expid)
    as_conf = load_config(expid)
    as_conf.reload(force_load=True)
    as_conf.check_conf_files()
    as_conf.load_parameters()

    exp_conf_dir = BasicConfig.expid_dir(expid) / "conf"
    conf_files = {
        f
        for pattern in files_or_extension_patterns
        for f in exp_conf_dir.rglob(pattern)
    }

    # TODO: Use tqdm to show the user the progress?
    # Convert conf files into YAML files.
    Log.info(
        f"Upgrading AS3 {len(conf_files)} conf files (.conf) to AS 4 YAML (.yml)..."
    )
    yaml_files = []
    for conf_file in conf_files:
        Log.debug(f"Upgrading AS3 conf file {conf_file} to AS4 YAML")
        try:
            yaml_file = ini_to_yaml(conf_file)
            yaml_files.append(yaml_file)
        except Exception as e:
            Log.warning(f"Failed to upgrade AS3 conf file {conf_file}: {e}")

    warnings = []
    substituted = []

    # Adjust placeholders.
    exp_conf_dir = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf"
    Log.info(
        f"Fixing placeholder variables (%_%) inside the new {len(yaml_files)} YAML files"
    )
    for yaml_file in yaml_files:
        template_path = exp_conf_dir / Path(yaml_file).name
        try:
            w, s = _fix_placeholders(template_path, as_conf)
            if w != "":
                warnings.append(f"Warnings for: {template_path.name}\n{w}\n")
            if s != "":
                substituted.append(
                    f"Variables changed for: {template_path.name}\n{s}\n"
                )
        except Exception as e:
            Log.warning(
                f"Failed to fix placeholders in the new AS4 YAML file {template_path}: {str(e)}"
            )

    # We now must have new YAML files. Let's reload them.
    as_conf.reload(force_load=True)
    as_conf.check_conf_files()
    as_conf.load_parameters()

    # Update files in proj/ folder.
    exp_project_dir = Path(as_conf.get_project_dir())
    template_path = Path()

    Log.info("Looking for %_% variables inside templates")
    for value in as_conf.jobs_data.values():
        try:
            template_path = exp_project_dir / Path(value.get("FILE", ""))
            w, s = _fix_placeholders(template_path, as_conf)
            if w != "":
                warnings.append(f"Warnings for: {template_path.name}\n{w}\n")
            if s != "":
                substituted.append(
                    f"Variables changed for: {template_path.name}\n{s}\n"
                )
        except Exception as e:
            Log.warning(
                f"Failed to fix placeholders in template file {template_path}: {str(e)}"
            )

    if substituted:
        Log.printlog("\n".join(substituted), Log.RESULT)
    if warnings:
        Log.error("\n".join(warnings))

    Log.info("Creating required folders...")
    create_required_folders(expid)

    # Commit.
    as_version = get_version()
    Log.info(
        f"Changing experiment {expid} version from {as_conf.get_version()} to {as_version}"
    )
    as_conf.set_version(as_version)
    update_experiment_description_version(expid, version=as_version)

    return True


def _fix_placeholders(
    template_or_script_path: Path, as_conf: AutosubmitConfig
) -> tuple[list[str], list[str]]:
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

    with open(template_or_script_path, "r", encoding=locale.getlocale()[1]) as f:
        template_or_script_content = f.read()
    # TODO: quite sure this is duplicating work done in the config module, we
    #       can reuse the same code.
    # Look for %_%, and make them all uppercase (Autosubmit 4 default format)
    variables = re.findall(
        "%(?<!%%)[a-zA-Z0-9_.-]+%(?!%%)",
        template_or_script_content,
        flags=re.IGNORECASE,
    )
    variables = [variable[1:-1].upper() for variable in variables]
    results: dict[str, set] = {}
    # Change format
    parameters = as_conf.load_parameters()

    # Build an index of parameters by their last part to avoid
    # iterating over all the parameters for each variable.
    by_last_key: dict[str, set[str]] = {}
    for key in parameters:
        last_key = key.split(".")[-1]
        value = "%" + key.strip("'") + "%"
        by_last_key.setdefault(last_key, set()).add(value)

    # Look up matching parameters directly (without performing a nested search).
    for old_format_key in variables:
        matches = by_last_key.get(old_format_key)
        if matches is not None:
            results.setdefault(old_format_key, set()).update(matches)

    for key, new_key in results.items():
        new_key_list = list(new_key)
        if len(new_key_list) > 1:
            if "JOBS" not in new_key_list[0] and "PLATFORMS" not in new_key_list:
                warnings.append(
                    f"Duplicate variable found: {key} to {new_key}. Adjust your script to use one of these."
                )
        else:
            new_key = new_key.pop().upper()
            success.append(f"Translated {key} to {new_key}")
            template_or_script_content = re.sub(
                "%(?<!%%)" + key + "%(?!%%)",
                new_key,
                template_or_script_content,
                flags=re.IGNORECASE,
            )
    # Deletes unused keys from confs
    if "autosubmit" in template_or_script_path.name.lower():
        template_or_script_content = re.sub(
            "(?m)^( )*(EXPID:)( )*[a-zA-Z0-9._-]*(\n)*",
            "",
            template_or_script_content,
            flags=re.IGNORECASE,
        )
    # Write the final result
    with open(template_or_script_path, "w") as f:
        f.write(template_or_script_content)

    if not warnings and not success:
        Log.result(
            f"Completed check for {template_or_script_path}.\nNo %_% variables found."
        )
    else:
        Log.result(f"Completed check for {template_or_script_path}")

    return warnings, success
