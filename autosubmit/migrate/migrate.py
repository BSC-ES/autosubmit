import tarfile

import time

import os
from typing import Any, Dict, List, Set

from bscearth.utils.date import Log

from autosubmit.helpers.utils import restore_platforms
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from log.log import Log, AutosubmitCritical
from autosubmit.job.job_utils import _get_submitter

from pathlib import Path


class Migrate:

    def __init__(self, experiment_id, only_remote):
        self.as_conf = None
        self.experiment_id = experiment_id
        self.only_remote = only_remote
        self.platforms_to_test = None
        self.platforms_to_migrate = None
        self.submit = None
        self.basic_config = BasicConfig()
        self.basic_config.read()

    def migrate_pickup(self) -> bool:
        """
        Pickup the experiment by copying remote files and directories to the local root directory.

        This function establishes connections to all platforms in use, copies remote files and directories
        to the local root directory, and checks if the experiment can run.

        :raises AutosubmitCritical: If the experiment is archived or if there are issues with remote platform configuration.
        :return: True if the experiment is successfully picked up.
        """
        Log.info(f'Pickup experiment {self.experiment_id}')
        exp_path = Path(self.basic_config.LOCAL_ROOT_DIR) / self.experiment_id
        if not exp_path.exists():
            raise AutosubmitCritical(
                "Experiment seems to be archived, no action is performed\nHint: Try to pickup without the remote flag",
                7012)

        as_conf = AutosubmitConfig(self.experiment_id, self.basic_config, YAMLParserFactory())
        as_conf.reload()
        as_conf.experiment_data["PLATFORMS"] = as_conf.misc_data.get("PLATFORMS", {})
        platforms = self.load_platforms_in_use(as_conf)

        error = False
        Log.info("Checking remote platforms")
        already_moved = set()

        try:
            restore_platforms(platforms)
        except Exception as e:
            self._handle_restore_platforms_error(e)

        for p in platforms:
            if p.temp_dir and p.temp_dir not in already_moved:
                if p.root_dir != p.temp_dir and p.temp_dir:
                    already_moved.add(p.temp_dir)
                    Log.info(f"Copying remote files/dirs on {p.name}")
                    Log.info(f"Copying from {p.temp_dir}/{self.experiment_id} to {p.root_dir}")
                    if not p.move_folder_rsync(Path(p.temp_dir) / self.experiment_id, Path(p.root_dir).parent):
                        error = True
                        break
                else:
                    Log.result(f"Files/dirs on {p.name} have been successfully picked up")

        if error:
            raise AutosubmitCritical(
                f"Unable to pickup all platforms, the non-moved files are on the TEMP_DIR\n You can try again with autosubmit {self.experiment_id} -p --onlyremote",
                7012)
        else:
            Log.result("The experiment has been successfully picked up.")
            Log.info("Checking if the experiment can run:")
            as_conf = AutosubmitConfig(self.experiment_id, self.basic_config, YAMLParserFactory())
            try:
                as_conf.check_conf_files(False)
                restore_platforms(platforms)
            except Exception as e:
                Log.warning(
                    "Before running, configure your platform settings. Remember that the as_misc pickup platforms aren't load outside the migrate")
                Log.warning(f"The experiment cannot run, check the configuration files:\n{e}")
            return True

    def _handle_restore_platforms_error(self, e: AutosubmitCritical) -> None:
        raise AutosubmitCritical(
            e.message + "\nInvalid Remote Platform configuration, recover them manually or:\n 1) Configure platform.yml with the correct info\n 2) autosubmit expid -p --onlyremote",
            7014, e.trace)

    def check_migrate_config(self, as_conf: AutosubmitConfig, platforms_to_test: List[Any],
                             pickup_data: Dict[str, Dict[str, str]]) -> None:
        """
        Checks if the configuration file has the necessary information to migrate the data.

        :param as_conf: Autosubmit configuration file.
        :param platforms_to_test: List of platforms to test.
        :param pickup_data: Data to migrate.
        :raises AutosubmitCritical: If there are missing or invalid platform configurations.
        """
        missing_platforms = []
        scratch_dirs = set()
        platforms_to_migrate = {}

        for platform in platforms_to_test:
            if platform.name not in pickup_data:
                if platform.name.upper() != "LOCAL" and platform.scratch not in scratch_dirs:
                    missing_platforms.append(platform.name)
            else:
                pickup_data[platform.name]["ROOTDIR"] = platform.root_dir
                platforms_to_migrate[platform.name] = pickup_data[platform.name]
                scratch_dirs.add(pickup_data[platform.name].get("SCRATCH_DIR", ""))

        if missing_platforms:
            raise AutosubmitCritical(f"Missing platforms in the offer conf: {','.join(missing_platforms)}", 7014)

        missconf_platforms = ""
        for platform_name, platform_data in platforms_to_migrate.items():
            if platform_name.upper() == "LOCAL":
                continue

            Log.info(f"Checking [{platform_name}] from as_misc configuration files...")
            valid_user = self._validate_user(as_conf, platform_name, platform_data)
            valid_project = self._validate_field(as_conf, platform_name, platform_data, "PROJECT")
            scratch_dir = self._validate_field(as_conf, platform_name, platform_data, "SCRATCH_DIR")
            valid_host = self._validate_field(as_conf, platform_name, platform_data, "HOST")
            valid_tmp_dir = platform_data.get("TEMP_DIR", False)

            if not valid_tmp_dir:
                continue
            elif not valid_user or not valid_project or not scratch_dir or not valid_host:
                self._log_invalid_config(as_conf, platform_name, platform_data)
                missconf_platforms += f', {platform_name}'
            else:
                Log.info(f"Valid configuration for platform [{platform_name}]")
                Log.result(f"Using platform: [{platform_name}] to migrate [{platform_data['ROOTDIR']}] data")

        if missconf_platforms:
            raise AutosubmitCritical(f"Invalid migrate configuration for platforms: {missconf_platforms[2:]}", 7014)

    @staticmethod
    def _validate_user(as_conf: AutosubmitConfig, platform_name: str, platform_data: Dict[str, str]) -> bool:
        user_offer = as_conf.platforms_data[platform_name].get("USER", None)
        user_pickup = platform_data.get("USER", None)
        if user_offer and user_pickup:
            return user_offer == user_pickup and platform_data.get("SAME_USER", False)
        return False

    @staticmethod
    def _validate_field(as_conf: AutosubmitConfig, platform_name: str, platform_data: Dict[str, str],
                        field: str) -> bool:
        return as_conf.platforms_data[platform_name].get(field, None) and platform_data.get(field, None)

    def _log_invalid_config(self, as_conf: AutosubmitConfig, platform_name: str, platform_data: Dict[str, str]) -> None:
        Log.printlog(f" Offer  USER: {as_conf.platforms_data[platform_name].get('USER', None)}\n"
                     f" Pickup USER: {platform_data.get('USER', None)}\n"
                     f" Offer  PROJECT: {as_conf.platforms_data[platform_name].get('PROJECT', None)}\n"
                     f" Pickup PROJECT: {platform_data.get('PROJECT', None)}\n"
                     f" Offer  SCRATCH_DIR: {as_conf.platforms_data[platform_name].get('SCRATCH_DIR', None)}\n"
                     f" Pickup SCRATCH_DIR: {platform_data.get('SCRATCH_DIR', None)}\n"
                     f" Shared TEMP_DIR: {platform_data.get('TEMP_DIR', '')}\n")
        Log.printlog(f"Invalid configuration for platform [{platform_name}]\nTrying next platform...", Log.ERROR)

    @staticmethod
    def load_platforms_in_use(as_conf: AutosubmitConfig) -> List[Any]:
        """
        Load the platforms in use for the experiment.

        This function retrieves the platforms used in the experiment based on the configuration.
        It raises an exception if no platforms are configured.

        :param as_conf: Autosubmit configuration file.
        :return: A list of platforms in use, excluding the local platform.
        :raises AutosubmitCritical: If no platforms are configured.
        """
        platforms_to_test = set()
        submitter = _get_submitter(as_conf)
        submitter.load_platforms(as_conf)
        if submitter.platforms is None:
            raise AutosubmitCritical("No platforms configured!!!", 7014)
        platforms = submitter.platforms
        for job_data in as_conf.experiment_data["JOBS"].values():
            platform_name = job_data.get("PLATFORM",
                                         as_conf.experiment_data.get("DEFAULT", {}).get("HPCARCH", "")).upper()
            platforms_to_test.add(platforms[platform_name])
        return [platform for platform in platforms_to_test if platform.name != "local"]

    def migrate_pickup_jobdata(self) -> None:
        """
        Unarchive job data files for the experiment.

        This function unarchives the job data files (`job_data_{expid}.tar`) into the job data directory.
        It checks for the existence of the tar file and extracts its contents if it exists.

        :raises AutosubmitCritical: If the tar file cannot be read.
        """
        Log.info(f'Unarchiving job_data_{self.experiment_id}.tar')
        job_data_dir = Path(self.basic_config.JOBDATA_DIR) / f"job_data_{self.experiment_id}"
        tar_file_path = Path(self.basic_config.JOBDATA_DIR) / f"{self.experiment_id}_jobdata.tar"

        if tar_file_path.exists():
            try:
                with tarfile.open(tar_file_path, 'r') as tar:
                    tar.extractall(path=job_data_dir)
                    tar.close()
                tar_file_path.unlink()
            except Exception as e:
                raise AutosubmitCritical("Can not read tar file", 7012, str(e))

    def migrate_offer_jobdata(self) -> bool:
        """
        Archive job data files for the experiment.

        This function archives the job data files (`job_data_{expid}.db` and `job_data_{expid}.sql`)
        into a tar file named `job_data_{expid}.tar`. It checks for the existence of the job data files
        and creates the tar file if they exist.

        :raises AutosubmitCritical: If the tar file cannot be written.
        :return: True if the job data is archived successfully.
        """
        Log.info(f'Archiving job_data_{self.experiment_id}.db and job_data_{self.experiment_id}.sql')
        job_data_dir = Path(self.basic_config.JOBDATA_DIR) / f"job_data_{self.experiment_id}"
        Log.info("Creating tar file ... ")
        try:
            compress_type = "w"
            output_filepath = Path(self.basic_config.JOBDATA_DIR) / f'{self.experiment_id}_jobdata.tar'
            db_exists = (job_data_dir / f"{self.experiment_id}.db").exists()
            sql_exists = (job_data_dir / f"{self.experiment_id}.sql").exists()
            if output_filepath.exists() and (db_exists or sql_exists):
                output_filepath.unlink()
            elif db_exists or sql_exists:
                with tarfile.open(output_filepath, compress_type) as tar:
                    if db_exists:
                        tar.add(job_data_dir / f"{self.experiment_id}.db", arcname=f"{self.experiment_id}.db")
                    if sql_exists:
                        tar.add(job_data_dir / f"{self.experiment_id}.sql", arcname=f"{self.experiment_id}.sql")
                    tar.close()
                output_filepath.chmod(0o775)
        except Exception as e:
            raise AutosubmitCritical("Can not write tar file", 7012, str(e))
        Log.result("Job data archived successfully")
        return True

    def migrate_offer_remote(self) -> None:
        """
        Perform the migration of remote platforms for the experiment.

        This function initializes the configuration, loads the migration data, checks the configuration,
        and performs the migration of remote platforms. It handles the conversion of absolute symlinks
        to relative ones and moves the files/directories to the specified temporary directories.
        Merge platform keys with migrate keys that should be the old credentials.
        Migrate file consists of:
            - platform_name: must match the platform name in the platforms configuration file, must have the old user
            - USER: user
            - PROJECT: project
            - Host (optional): host of the machine if using alias
            - TEMP_DIR: temp dir for current platform, because it can be different for each of them
        :raises AutosubmitCritical: If no migration information is found or if there are issues with the platforms.
        """
        as_conf = self._initialize_configuration()
        pickup_data = self._load_migrate_data(as_conf)
        platforms_to_test = self.load_platforms_in_use(as_conf)
        Log.info(f'Migrating experiment {self.experiment_id}')
        Log.info("Checking remote platforms")
        self.check_migrate_config(as_conf, platforms_to_test, pickup_data)
        restore_platforms(platforms_to_test)
        platforms_with_issues = self._migrate_platforms(platforms_to_test, pickup_data)
        if platforms_with_issues:
            raise AutosubmitCritical(f'Platforms with issues: {platforms_with_issues}', 7014)

    def _initialize_configuration(self) -> AutosubmitConfig:
        as_conf = AutosubmitConfig(self.experiment_id, self.basic_config, YAMLParserFactory())
        as_conf.check_conf_files(False)
        return as_conf

    @staticmethod
    def _load_migrate_data(as_conf: AutosubmitConfig) -> Dict[str, Dict[str, str]]:
        pickup_data = as_conf.misc_data.get("PLATFORMS", {})
        if not pickup_data:
            raise AutosubmitCritical("No migrate information found", 7014)
        return pickup_data

    def _migrate_platforms(self, platforms_to_test: List[Any], pickup_data: Dict[str, Dict[str, str]]) -> List[str]:
        platforms_with_issues = []
        for p in platforms_to_test:
            if p.temp_dir == "":
                p.temp_dir = pickup_data.get(p.name, {}).get("TEMP_DIR", "")
            Log.info(f"Using temp dir: {p.temp_dir}")
            if p.root_dir != p.temp_dir and len(p.temp_dir) > 0:
                if not self._convert_symlinks(p):
                    platforms_with_issues.append(p.name)
                    continue
                if not self._move_files(p):
                    platforms_with_issues.append(p.name)
        return platforms_with_issues

    @staticmethod
    def _convert_symlinks(platform: Any) -> bool:
        try:
            Log.info(f"Converting the absolute symlinks into relatives on platform [{platform.name}]")
            command = f"cd {platform.remote_log_dir} ; find {platform.root_dir} -type l -lname '/*' -printf 'var=\"$(realpath -s --relative-to=\"%p\" \"$(readlink \"%p\")\")\" && var=${{var:3}} && ln -sf $var \"%p\" \\n' > convertLink.sh"
            if not platform.check_absolute_file_exists(platform.temp_dir):
                Log.printlog(f'{platform.temp_dir} does not exist on platform [{platform.name}]', 7014)
                return False
            thread = platform.send_command_non_blocking(f"{command} ", True)
            start_time = time.time()
            while thread.is_alive():
                if time.time() - start_time >= 10:
                    Log.info(f"Waiting for the absolute symlinks conversion to finish on platform [{platform.name}]")
                    start_time = time.time()
                time.sleep(1)
            platform.send_command(f"cd {platform.remote_log_dir} ; cat convertLink.sh", True)
            ssh_output = platform.get_ssh_output()
            if ssh_output.startswith("var="):
                command = f"cd {platform.remote_log_dir} ; chmod +x convertLink.sh ; ./convertLink.sh ; rm convertLink.sh"
                platform.send_command(command, True)
                Log.result(f"Absolute symlinks converted on platform [{platform.name}]")
            else:
                Log.result(f"No absolute symlinks found in [{platform.root_dir}] for platform [{platform.name}]")
            return True
        except Exception as e:
            Log.printlog(f"Absolute symlinks failed to convert due to [{str(e)}] on platform [{platform.name}]", 7014)
            return False

    def _move_files(self, platform: Any) -> bool:
        tmp_experiment_path = Path(platform.temp_dir).joinpath(self.experiment_id)

        try:
            Log.info(f"Moving remote files/dirs on platform [{platform.name}] to [{platform.temp_dir}]")
            platform.send_command(f"chmod 777 -R {platform.root_dir}")
            platform.send_command(f"mkdir -p {platform.temp_dir}")
            platform.send_command(f"chmod 777 -R {platform.temp_dir}")
            if platform.check_absolute_file_exists(platform.root_dir):
                if platform.check_absolute_file_exists(tmp_experiment_path):
                    Log.printlog(
                        f"Directory [{str(tmp_experiment_path)}] already exists. New data won't be moved until you move the old data",
                        6000)
                    return False
            if not platform.move_file(platform.root_dir, tmp_experiment_path, False):
                if not platform.move_folder_rsync(platform.root_dir, platform.temp_dir):
                    Log.result(f"No data found in [{platform.root_dir}] for platform [{platform.name}]")
            Log.result(
                f"Remote files/dirs on platform [{platform.name}] have been successfully moved to [{str(tmp_experiment_path)}]")
            return True
        except Exception as e:
            Log.printlog(
                f"Cant move files/dirs on platform [{platform.name}] to [{str(tmp_experiment_path)}] due to [{str(e)}]", 6000)
            return False
