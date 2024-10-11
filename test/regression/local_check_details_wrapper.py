"""
This test took the now ordered by name -d option of autosubmit create and checks that the workflow of 4.1 and 4.0 match.
Works under local_computer TODO introduce in CI
"""

import os
import subprocess
BIN_PATH = '../../bin'
VERSION = 4.1

def check_cmd(command, path=BIN_PATH):
    try:
        output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
        error = False
    except subprocess.CalledProcessError as e:
        output = e.output
        error = True
    return output, error

def run_test(expid):
    #check_cmd(f"rm -r /home/dbeltran/new_autosubmit/{expid}/tmp/LOG_{expid}/*")
    output = check_cmd(f"../../bin/autosubmit create {expid} -np -v -d -cw;")
    return output
def perform_test(expids):
    to_exclude = []

    for expid in expids:
        try:
            output,error = run_test(expid)
            # output to str
            output = output.decode("UTF-8")
            output = output.split("Job list created successfully[0m[39m")[1]
            output = expid + output
            # put it in a single file
            with open(f"{VERSION}_multi_test.txt", "a") as myfile:
                myfile.write(output)
        except Exception:
            raise Exception(f"Error in {expid}")

    # print to_exclude in format ["a001","a002"]
    print(to_exclude)


open(f"{VERSION}_multi_test.txt", "w").close()

# list all experiments under ~/new_autosubmit.
# except the excluded ones, which are not run
expids = []
excluded = ['a01y', 'a00j', 'a020', 'a01t', 'a00q', 'a00f', 'a01h', 'a00o', 'a01c', 'a00z', 't008', 'a00y', 'a00r', 't009', 'a000', 'a01e', 'a01i', 'a002', 'a008', 'a010', 'a003', 't007', 'a01d', 'autosubmit.db', 'a021', 'a00h', 'as_times.db', 'a04d', 'a02v']
for experiment in os.listdir("/home/dbeltran/new_autosubmit"):
    if experiment.startswith("a") or experiment.startswith("t") and len(experiment) == 4:
        if experiment not in excluded:
            expids.append(experiment)
perform_test(expids)