import tarfile

import time

import os

from bscearth.utils.date import Log

from autosubmit.helpers.utils import restore_platforms
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from log.log import Log, AutosubmitCritical, AutosubmitError
from autosubmit.job.job_utils import _get_submitter

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

    def migrate_pickup(self):
        Log.info(f'Pickup experiment {self.experiment_id}')
        exp_path = os.path.join(
            self.basic_config.LOCAL_ROOT_DIR, self.experiment_id)
        if not os.path.exists(exp_path):
            raise AutosubmitCritical(
                "Experiment seems to be archived, no action is performed\nHint: Try to pickup without the remote flag", 7012)
        as_conf = AutosubmitConfig(
            self.experiment_id, self.basic_config, YAMLParserFactory())
        as_conf.reload()
        as_conf.experiment_data["PLATFORMS"] = as_conf.misc_data.get("PLATFORMS",{})
        platforms = self.load_platforms_in_use(as_conf)

        error = False
        Log.info("Checking remote platforms")
        already_moved = set()
        # establish the connection to all platforms on use
        try:
            restore_platforms(platforms)
        except AutosubmitCritical as e:
            raise AutosubmitCritical(
                e.message + "\nInvalid Remote Platform configuration, recover them manually or:\n 1) Configure platform.yml with the correct info\n 2) autosubmit expid -p --onlyremote",
                7014, e.trace)
        except Exception as e:
            raise AutosubmitCritical(
                "Invalid Remote Platform configuration, recover them manually or:\n 1) Configure platform.yml with the correct info\n 2) autosubmit expid -p --onlyremote",
                7014, str(e))
        for p in platforms:
            if p.temp_dir is not None and p.temp_dir not in already_moved:
                if p.root_dir != p.temp_dir and len(p.temp_dir) > 0:
                    already_moved.add(p.temp_dir)
                    Log.info(
                        "Copying remote files/dirs on {0}", p.name)
                    Log.info("Copying from {0} to {1}", os.path.join(
                        p.temp_dir, self.experiment_id), p.root_dir)
                    finished = False
                    limit = 150
                    rsync_retries = 0
                    try:
                        # Avoid infinite loop unrealistic upper limit, only for rsync failure
                        while not finished and rsync_retries < limit:
                            finished = False
                            pipeline_broke = False
                            Log.info(
                                "Rsync launched {0} times. Can take up to 150 retrials or until all data is transferred".format(
                                    rsync_retries + 1))
                            try:
                                p.send_command(
                                    "rsync --timeout=3600 --bwlimit=20000 -aq --remove-source-files " + os.path.join(
                                        p.temp_dir, self.experiment_id) + " " + p.root_dir[:-5])
                            except BaseException as e:
                                Log.debug("{0}".format(str(e)))
                                rsync_retries += 1
                                try:
                                    if p.get_ssh_output_err() == "":
                                        finished = True
                                    elif p.get_ssh_output_err().lower().find("no such file or directory") == -1:
                                        finished = True
                                    else:
                                        finished = False
                                except Exception as e:
                                    finished = False
                                pipeline_broke = True
                            if not pipeline_broke:
                                if p.get_ssh_output_err().lower().find("no such file or directory") == -1:
                                    finished = True
                                elif p.get_ssh_output_err().lower().find(
                                        "warning: rsync") != -1 or p.get_ssh_output_err().lower().find(
                                    "closed") != -1 or p.get_ssh_output_err().lower().find(
                                    "broken pipe") != -1 or p.get_ssh_output_err().lower().find(
                                    "directory has vanished") != -1:
                                    rsync_retries += 1
                                    finished = False
                                elif p.get_ssh_output_err() == "":
                                    finished = True
                                else:
                                    error = True
                                    finished = False
                                    break
                            p.send_command(
                                "find {0} -depth -type d -empty -delete".format(
                                    os.path.join(p.temp_dir, self.experiment_id)))
                            Log.result(
                                "Empty dirs on {0} have been successfully deleted".format(p.temp_dir))
                        if finished:
                            p.send_command("chmod 755 -R " + p.root_dir)
                            Log.result(
                                "Files/dirs on {0} have been successfully picked up", p.name)
                            # p.send_command(
                            #    "find {0} -depth -type d -empty -delete".format(os.path.join(p.temp_dir, experiment_id)))
                            Log.result(
                                "Empty dirs on {0} have been successfully deleted".format(p.temp_dir))
                        else:
                            Log.printlog("The files/dirs on {0} cannot be copied to {1}.".format(
                                os.path.join(p.temp_dir, self.experiment_id), p.root_dir), 6012)
                            error = True
                            break

                    except IOError as e:
                        raise AutosubmitError(
                            "I/O Issues", 6016, e.message)
                    except BaseException as e:
                        error = True
                        Log.printlog("The files/dirs on {0} cannot be copied to {1}.\nTRACE:{2}".format(
 os.path.join(p.temp_dir, self.experiment_id), p.root_dir, str(e)), 6012)
                        break
                else:
                    Log.result(
                        "Files/dirs on {0} have been successfully picked up", p.name)
        if error:
            raise AutosubmitCritical(
                "Unable to pickup all platforms, the non-moved files are on the TEMP_DIR\n You can try again with autosubmit {0} -p --onlyremote".format(
                    self.experiment_id), 7012)
        else:
            Log.result("The experiment has been successfully picked up.")
            Log.info("Checking if the experiment can run:")
            as_conf = AutosubmitConfig(
                self.experiment_id, self.basic_config, YAMLParserFactory())
            try:
                as_conf.check_conf_files(False)
                restore_platforms(platforms)
            except BaseException as e:
                Log.warning(f"Before running, configure your platform settings. Remember that the as_misc pickup platforms aren't load outside the migrate")
                Log.warning(f"The experiment cannot run, check the configuration files:\n{e}")
            return True

    def check_migrate_config(self, as_conf, platforms_to_test, pickup_data ):
        """
        Checks if the configuration file has the necessary information to migrate the data
        :param as_conf: Autosubmit configuration file
        :param platforms_to_test: platforms to test
        :param pickup_data: data to migrate

        """
        # check if all platforms_to_test are present in the pickup_data
        missing_platforms = set()
        scratch_dirs = set()
        platforms_to_migrate = dict()
        for platform in platforms_to_test:
            if platform.name not in pickup_data.keys():
                if platform.name.upper() != "LOCAL" and platform.scratch not in scratch_dirs:
                    missing_platforms.add(platform.name)
            else:
                pickup_data[platform.name]["ROOTDIR"] = platform.root_dir
                platforms_to_migrate[platform.name] = pickup_data[platform.name]
                scratch_dirs.add(pickup_data[platform.name].get("SCRATCH_DIR", ""))
        if missing_platforms:
            raise AutosubmitCritical(f"Missing platforms in the offer conf: {missing_platforms}", 7014)
        missconf_plaforms = ""
        for platform_pickup_name, platform_pickup_data in platforms_to_migrate.items():
            if platform_pickup_name.upper() == "LOCAL":
                continue

            Log.info(f"Checking [{platform_pickup_name}] from as_misc configuration files...")
            valid_user = as_conf.platforms_data[platform_pickup_name].get("USER", None) and platform_pickup_data.get("USER", None)
            if valid_user:
                if as_conf.platforms_data[platform_pickup_name].get("USER", None) == platform_pickup_data.get("USER", None):
                    if platform_pickup_data.get("SAME_USER",False):
                        valid_user = True
                    else:
                        valid_user = False
            valid_project = as_conf.platforms_data[platform_pickup_name].get("PROJECT", None) and platform_pickup_data.get("PROJECT", None)
            scratch_dir = as_conf.platforms_data[platform_pickup_name].get("SCRATCH_DIR", None) and platform_pickup_data.get("SCRATCH_DIR", None)
            valid_host = as_conf.platforms_data[platform_pickup_name].get("HOST", None) and platform_pickup_data.get("HOST", None)
            valid_tmp_dir = platform_pickup_data.get("TEMP_DIR", False)
            if not valid_tmp_dir:
                continue
            elif not valid_user or not valid_project or not scratch_dir or not valid_host:
                Log.printlog(f" Offer  USER: {as_conf.platforms_data[platform_pickup_name].get('USER',None)}\n"
                             f" Pickup USER: {platform_pickup_data.get('USER',None)}\n"
                             f" Offer  PROJECT: {as_conf.platforms_data[platform_pickup_name].get('PROJECT',None)}\n"
                             f" Pickup PROJECT: {platform_pickup_data.get('PROJECT',None)}\n"
                             f" Offer  SCRATCH_DIR: {as_conf.platforms_data[platform_pickup_name].get('SCRATCH_DIR',None)}\n"
                             f" Pickup SCRATCH_DIR: {platform_pickup_data.get('SCRATCH_DIR',None)}\n"
                             f" Shared TEMP_DIR: {platform_pickup_data.get('TEMP_DIR', '')}\n")
                Log.printlog(f"Invalid configuration for platform [{platform_pickup_name}]\nTrying next platform...",Log.ERROR)
                missconf_plaforms = missconf_plaforms + f', {platform_pickup_name}'
            else:
                Log.info("Valid configuration for platform [{0}]".format(platform_pickup_name))
                Log.result(f"Using platform: [{platform_pickup_name}] to migrate [{pickup_data[platform_pickup_name]['ROOTDIR']}] data")
        if missconf_plaforms:
            raise AutosubmitCritical(f"Invalid migrate configuration for platforms: {missconf_plaforms[2:]}", 7014)

    def load_platforms_in_use(self, as_conf):
        platforms_to_test = set()
        submitter = _get_submitter(as_conf)
        submitter.load_platforms(as_conf)
        if submitter.platforms is None:
            raise AutosubmitCritical("No platforms configured!!!", 7014)
        platforms = submitter.platforms
        for job_data in as_conf.experiment_data["JOBS"].values():
            platforms_to_test.add(platforms[job_data.get("PLATFORM", as_conf.experiment_data.get("DEFAULT", {}).get("HPCARCH", "")).upper()])
        return [ platform for platform in platforms_to_test if platform.name != "local" ]

    def migrate_pickup_jobdata(self):
        # Unarchive job_data_{expid}.tar
        Log.info(f'Unarchiving job_data_{self.experiment_id}.tar')
        job_data_dir = f"{self.basic_config.JOBDATA_DIR}/job_data_{self.experiment_id}"
        if os.path.exists(os.path.join(self.basic_config.JOBDATA_DIR, f"{self.experiment_id}_jobdata.tar")):
            try:
                with tarfile.open(os.path.join(self.basic_config.JOBDATA_DIR, f"{self.experiment_id}_jobdata.tar", 'r')) as tar:
                    tar.extractall(path=job_data_dir)
                    tar.close()
                os.remove(os.path.join(self.basic_config.JOBDATA_DIR, f"{self.experiment_id}_jobdata.tar"))
            except Exception as e:
                raise AutosubmitCritical("Can not read tar file", 7012, str(e))

    def migrate_offer_jobdata(self):
        # archive job_data_{expid}.db and job_data_{expid}.sql
        Log.info(f'Archiving job_data_{self.experiment_id}.db and job_data_{self.experiment_id}.sql')
        job_data_dir = f"{self.basic_config.JOBDATA_DIR}/job_data_{self.experiment_id}"
        # Creating tar file
        Log.info("Creating tar file ... ")
        try:
            compress_type = "w"
            output_filepath = f'{self.experiment_id}_jobdata.tar'
            db_exists = os.path.exists(f"{job_data_dir}.db")
            sql_exists = os.path.exists(f"{job_data_dir}.sql")
            if os.path.exists(os.path.join(self.basic_config.JOBDATA_DIR, output_filepath)) and (db_exists or sql_exists):
                os.remove(os.path.join(self.basic_config.JOBDATA_DIR, output_filepath))
            elif db_exists or sql_exists:
                with tarfile.open(os.path.join(self.basic_config.JOBDATA_DIR, output_filepath), compress_type) as tar:
                    if db_exists:
                        tar.add(f"{job_data_dir}.db", arcname=f"{self.experiment_id}.db")
                    if sql_exists:
                        tar.add(f"{job_data_dir}.sql", arcname=f"{self.experiment_id}.sql")
                    tar.close()
                    os.chmod(os.path.join(self.basic_config.JOBDATA_DIR, output_filepath), 0o775)
        except Exception as e:
            raise AutosubmitCritical("Can not write tar file", 7012, str(e))
        Log.result("Job data archived successfully")
        return True

    def migrate_offer_remote(self):
        exit_with_errors = False
        # Init the configuration
        as_conf = AutosubmitConfig(self.experiment_id, self.basic_config, YAMLParserFactory())
        as_conf.check_conf_files(False)
        # Load migrate
        #Find migrate file
        pickup_data = as_conf.misc_data.get("PLATFORMS",{})
        if not pickup_data:
            raise AutosubmitCritical("No migrate information found", 7014)

        # Merge platform keys with migrate keys that should be the old credentials
        # Migrate file consist of:
        # platform_name: must match the platform name in the platforms configuration file, must have the old user
        #  USER: user
        #  PROJECT: project
        #  Host ( optional ) : host of the machine if using alias
        #  TEMP_DIR: temp dir for current platform, because can be different for each of the

        platforms_to_test = self.load_platforms_in_use(as_conf)
        Log.info('Migrating experiment {0}'.format(self.experiment_id))
        Log.info("Checking remote platforms")
        self.check_migrate_config(as_conf, platforms_to_test, pickup_data)
        # establish the connection to all platforms on use
        restore_platforms(platforms_to_test)
        platforms_with_issues = list()
        for p in platforms_to_test:
            if p.temp_dir == "":
                p.temp_dir = pickup_data.get(p.name, {}).get("TEMP_DIR", "")
            Log.info(f"Using temp dir: {p.temp_dir}")
            if p.root_dir != p.temp_dir and len(p.temp_dir) > 0:
                try:
                    Log.info(f"Converting the absolute symlinks into relatives on platform [{p.name}] ")
                    command = f"cd {p.remote_log_dir} ; find {p.root_dir} -type l -lname '/*' -printf 'var=\"$(realpath -s --relative-to=\"%p\" \"$(readlink \"%p\")\")\" && var=${{var:3}} && ln -sf $var \"%p\" \\n' > convertLink.sh"
                    try:
                        p.check_absolute_file_exists(p.temp_dir)
                    except Exception:
                        exit_with_errors = True
                        Log.printlog(f'{p.temp_dir} does not exist on platform [{p.name}]', 7014)
                        platforms_with_issues.append(p.name)
                        continue
                    thread = p.send_command_non_blocking(f"{command} ", True)
                    # has thread end?
                    start_time = time.time()
                    Log.info(f"Waiting for the absolute symlinks conversion to finish on platform [{p.name}]")
                    while thread.is_alive():
                        current_time = time.time()
                        elapsed_time = current_time - start_time
                        if elapsed_time >= 10:
                            Log.info(f"Waiting for the absolute symlinks conversion to finish on platform [{p.name}]")
                            start_time = time.time()  # reset the start time
                        time.sleep(1)
                    p.send_command(f"cd {p.remote_log_dir} ; cat convertLink.sh", True)
                    ssh_output = p.get_ssh_output()
                    if ssh_output.startswith("var="):
                        command = f"cd {p.remote_log_dir} ; chmod +x convertLink.sh ; ./convertLink.sh ; rm convertLink.sh"
                        p.send_command(command, True)
                        Log.result(f"Absolute symlinks converted on platform [{p.name}]")
                    else:
                        Log.result(f"No absolute symlinks found in [{p.root_dir}] for platform [{p.name}]")
                except IOError:
                    Log.result(f"No absolute symlinks found in [{p.root_dir}] for platform [{p.name}]")
                except AutosubmitError:
                    raise
                except AutosubmitCritical:
                    raise
                except BaseException as e:
                    exit_with_errors = True
                    error = str(e) + "\n" + p.get_ssh_output_err()
                    Log.printlog(f"Absolute symlinks failed to convert due to [{str(error)}] on platform [{p.name}]",
                                 7014)
                    platforms_with_issues.append(p.name)

                    break
                # If there are no errors in the conversion of the absolute symlinks, then move the files of this platform
                try:
                    Log.info(f"Moving remote files/dirs on platform [{p.name}] to [{p.temp_dir}]")
                    p.send_command(f"chmod 777 -R {p.root_dir}")
                    p.send_command(f"mkdir -p {p.temp_dir}")
                    p.send_command(f"chmod 777 -R {p.temp_dir}")
                    if p.check_absolute_file_exists(os.path.join(p.root_dir, self.experiment_id)):
                        if p.check_absolute_file_exists(os.path.join(p.temp_dir, self.experiment_id)):
                            Log.printlog(f"Directory [{os.path.join(p.temp_dir, self.experiment_id)}] already exists. New data won't be moved until you move the old data", 6000)
                            platforms_with_issues.append(p.name)
                            break
                    if not p.move_file(p.root_dir, os.path.join(p.temp_dir, self.experiment_id), False):
                        Log.result(f"No data found in [{p.root_dir}] for platform [{p.name}]")
                    else:
                        Log.result(
                            f"Remote files/dirs on platform [{p.name}] have been successfully moved to [{p.temp_dir}]")
                except BaseException as e:
                    exit_with_errors = True
                    Log.printlog(
                        f"Cant move files/dirs on platform [{p.name}] to [{p.temp_dir}] due to [{str(e)}]",
                        6000)
                    platforms_with_issues.append(p.name)
                    break
                Log.result(f"Platform [{p.name}] has been successfully migrated")
        if exit_with_errors:
            raise AutosubmitCritical(f'Platforms with issues: {platforms_with_issues}', 7014)

