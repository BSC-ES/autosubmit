from typing import List, Union, Any
from autosubmit.platforms.slurmplatform import SlurmPlatform
from log.log import AutosubmitCritical, AutosubmitError, Log
import os


class MaestroPlatform(SlurmPlatform):
    """Class to manage slurm jobs"""
    def __init__(self, expid: str, name: str, config: dict, auth_password: str=None):
        """ Initialization of the Class MaestroPlatform

        :param expid: Id of the experiment.
        :type expid: str
        :param name: Name of the platform.
        :type name: str
        :param config: A dictionary containing all the Experiment parameters.
        :type config: dict
        """
        self.job_names = {}
        Log.result(f"[MaestroPlatform] Init: {name}")
        SlurmPlatform.__init__(self, expid, name, config, auth_password = auth_password)

    def submit_job(self, job, script_name: str, hold: bool=False, export: str="none") -> Union[int, None]:
        """
        Submit a job from a given job object.

        :param job: Job object
        :type job: autosubmit.job.job.Job
        :param script_name: Name of the script of the job.
        :type script_name: str
        :param hold: Send job hold.
        :type hold: bool
        :param export: Set within the jobs.yaml, used to export environment script to use before the job is launched.
        :type export: str

        :return: job id for the submitted job.
        :rtype: int
        """
        Log.result(f"[MaestroPlatform] Job: {job.name}")
        # XXX Still not clear what this function does, if `submit_Script` does the `sbatch`?
        if not job.name in self.job_names.keys():
            self.job_names[job.name] = {}
            self.job_names[job.name]["id"] = ''.join(str(x) for x in bytes(job.name.encode("UTF-8")) )
            Log.result("[MaestroPlatform] assigning to `" + str(job.name) + "` id: " + str(self.job_names[job.name]["id"]))
            self.job_names[job.name]["status"] = "PENDING"
        else: 
            Log.result("[MaestroPlatform] second times same job")
        return SlurmPlatform.submit_job(self, job, script_name, hold, export)

    def submit_Script(self, hold: bool=False) -> Union[List[int], int]:
        """
        Sends a Submit file Script with sbatch instructions, execute it in the platform and
        retrieves the Jobs_ID of all jobs at once.

        :param hold: Submit a job in held status. Held jobs will only earn priority status if the
            remote machine allows it.
        :type hold: bool
        :return: job id for submitted jobs.
        :rtype: Union[List[int], int]
        """
        try:
            self.send_file(self.get_submit_script(), False)
            cmd = os.path.join(self.get_files_path(),
                               os.path.basename(self._submit_script_path))
            Log.result(f"[MaestroPlatform] Trying to fake submit job cmd: {cmd}")
#            # remove file after submission
#            cmd = f"{cmd} ; rm {cmd}"
#            try:
#                self.send_command(cmd)
#            except Exception:
#                raise
#            jobs_id = self.get_submitted_job_id(self.get_ssh_output())
            jobs_id = []
            for key,x in self.job_names.items():
                for k, v in self.job_names[key].items():
                    if k == "status" and v == "PENDING":
                        jobs_id.append(int(self.job_names[key]["id"]))
                        self.job_names[key]["status"] = "RUNNING"
#            Log.result("[MaestroPlatform] [submitScript] jobs_id str: " + str(jobs_id))
            return jobs_id

        except IOError as e:
            raise AutosubmitError("Submit script is not found, retry again in next AS iteration", 6008, str(e)) from e
        except AutosubmitError:
            raise
        except AutosubmitCritical:
            raise
        except Exception as e:
            raise AutosubmitError("Submit script is not found, retry again in next AS iteration", 6008, str(e)) from e


    def get_checkjob_cmd(self, job_id: str) -> str: # noqa
        Log.result("[MaestroPlatform] [get_checkjob_cmd] job_id: " +  str(job_id))

        return "echo \"[MaestroPlatform] [get_checkjob_cmd] Faking sacct on WFM (MDN role) \""


    def get_checkAlljobs_cmd(self, jobs_id: str): # noqa
# FIXME We need to set the status as COMPLETED when we see the COMPLETED file, we don't want to add extra steps (yet at least) of retrieving and checking slurm status per job name

        #[MaestroPlatform] [get_checkAlljobs_cmd] Faking salloc on WFM (MDN role)

        Log.result("[MaestroPlatform] [get_checkAlljobs_cmd] checking files...")
        files = self.check_completed_files()
        Log.result("[MaestroPlatform] [get_checkAlljobs_cmd] files: " + str(files))

        Log.result("[MaestroPlatform] [get_checkAlljobs_cmd] jobs_id: " + str(jobs_id))
        ret = "echo " 
        tmp = ""
        for key,x in self.job_names.items():
            if tmp:
                tmp += ", "
            if files.find(key) > -1:
                Log.result("[MaestroPlatform] [get_checkAlljobs_cmd] found key in files: " + str(key))
                self.job_names[key]["status"] = "COMPLETED"
            tmp += str(self.job_names[key]["id"]) + " " + str(self.job_names[key]["status"])
        ret += tmp
        Log.result("[MaestroPlatform] [get_checkAlljobs_cmd] fake slurm status: " + str(ret))
        return ret
#       return "echo \"12345     RUNNING\""

    def get_submit_cmd_x11(self, args: str, script_name: str) -> str:

        return "echo \"[MaestroPlatform] [get_submit_cmd_x11] Faking salloc on WFM (MDN role) \""


