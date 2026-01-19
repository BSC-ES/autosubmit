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
from pathlib import Path

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
    'upgrade_scripts'
]


def upgrade_scripts(expid: str, files="") -> bool:
    """Upgrade scripts from Autosubmit 3 to 4."""

    if not files:
        files = ('*.yml', '*.yaml', '*.conf')

    Log.info("Checking if experiment exists...")

    # Check that the user is the owner and the configuration is well configured
    check_ownership(expid, raise_error=True)
    folder = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf"
    factory = YAMLParserFactory()
    # update scripts to yml format
    for f in folder.rglob("*.yml"):
        # Tries to convert an invalid yml to correct one
        try:
            parser = factory.create_parser()
            parser.load(Path(f))
        except Exception as e:
            Log.error(f"Failed loading the file {str(f)}: {str(e)}")
            try:
                AutosubmitConfig.ini_to_yaml(f.parent, Path(f))
            except Exception as e2:
                Log.error(f"Couldn't convert conf file {str(f)} to yml {f.parent}: {str(e2)}")
                return False

    # Converts all ini into yaml
    Log.info("Converting all .conf files into .yml.")
    for f in folder.rglob("*.conf"):
        if not Path(f.stem + ".yml").exists():
            try:
                AutosubmitConfig.ini_to_yaml(Path(f).parent, Path(f))
            except Exception:
                Log.warning(f"Couldn't convert conf file to yml: {Path(f).parent}")
                return False
    as_conf = AutosubmitConfig(expid, BasicConfig, YAMLParserFactory())
    as_conf.reload(force_load=True)
    # Load current variables
    as_conf.check_conf_files()
    # Load current parameters ( this doesn't read job parameters)
    as_conf.load_parameters()

    # Update configuration files
    warn = ""
    substituted = ""
    root_dir = Path(as_conf.basic_config.LOCAL_ROOT_DIR) / expid / "conf"
    Log.info("Looking for %_% variables inside conf files")
    for f in _get_files(root_dir, files):
        template_path = root_dir / Path(f).name
        try:
            w, s = _update_old_script(root_dir, template_path, as_conf)
            if w != "":
                warn += f"Warnings for: {template_path.name}\n{w}\n"
            if s != "":
                substituted += f"Variables changed for: {template_path.name}\n{s}\n"
        except BaseException as e:
            Log.printlog(f"Couldn't read {template_path} template.\ntrace:{str(e)}")
    if substituted != "" and warn != "":
        Log.result(substituted)
        Log.result(warn)
    # Update templates
    root_dir = Path(as_conf.get_project_dir())
    template_path = Path()
    warn = ""
    substituted = ""
    Log.info("Looking for %_% variables inside templates")
    for section, value in as_conf.jobs_data.items():
        try:
            template_path = root_dir / Path(value.get("FILE", ""))
            w, s = _update_old_script(template_path.parent, template_path, as_conf)
            if w != "":
                warn += f"Warnings for: {template_path.name}\n{w}\n"
            if s != "":
                substituted += f"Variables changed for: {template_path.name}\n{s}\n"
        except BaseException as e:
            Log.printlog(f"Couldn't read {template_path} template.\ntrace:{str(e)}")
    if substituted != "":
        Log.printlog(substituted, Log.RESULT)
    if warn != "":
        Log.printlog(warn, Log.ERROR)

    as_version = get_version()

    Log.info(f"Changing {expid} experiment version from {as_conf.get_version()} to {as_version}")
    as_conf.set_version(as_version)
    update_experiment_description_version(expid, version=as_version)
    return True


def _get_files(root_dir_, extensions, files_filter=""):
    """Get the list of files by extension and filters."""
    all_files = []
    if len(files_filter) > 0:
        for ext in extensions:
            all_files.extend(root_dir_.rglob(ext))
    else:
        if ',' in files_filter:
            files_filter = files_filter.split(',')
        elif ' ' in files_filter:
            files_filter = files_filter.split(' ')
        for file in files_filter:
            all_files.append(file)
    return all_files


def _update_old_script(root_dir: Path, template_path: Path, as_conf: AutosubmitConfig):
    # Do a backup and tries to update
    warnings = []
    substituted = []
    Log.info(f"Checking {template_path}")
    if template_path.exists():
        backup_path = root_dir / Path(template_path.name + "_AS_v3_backup_placeholders")
        if not backup_path.exists():
            Log.info(f"Backup stored at {backup_path}")
            shutil.copyfile(template_path, backup_path)
        template_content = open(template_path, 'r', encoding=locale.getlocale()[1]).read()
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
        # Write final result
        open(template_path, "w").write(template_content)

    if not warnings and not substituted:
        Log.result(f"Completed check for {template_path}.\nNo %_% variables found.")
    else:
        Log.result(f"Completed check for {template_path}")

    return "\n".join(warnings), "\n".join(substituted)
