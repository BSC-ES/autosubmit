"""
This test took the now ordered by name -d option of autosubmit create and checks that the workflow of 4.1 and 4.0 match.
Works under local_computer TODO introduce in CI
"""

# Check: a014, a016


import os
import subprocess
BIN_PATH = '../../bin'
ACTIVE_DOCS = True # Use autosubmit_docs database
VERSION = 4.1 # 4.0 or 4.1

if ACTIVE_DOCS:
    EXPERIMENTS_PATH = '/home/dbeltran/autosubmit_docs'
    FILE_NAME = f"{VERSION}_docs_test.txt"
    BANNED_TESTS = []
else:
    EXPERIMENTS_PATH = '/home/dbeltran/new_autosubmit'
    FILE_NAME = f"{VERSION}_multi_test.txt"
    BANNED_TESTS = ["a02j","t002","a006","a00s","a029","a00z","a02l","a026","a012","a018","a02f","t000","a02d","a02i","a025","a02e","a02h","a02b","a023","a02k","a02c"]

def check_cmd(command, path=BIN_PATH):
    try:
        output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
        error = False
    except subprocess.CalledProcessError as e:
        output = e.output
        error = True
    return output, error

def run_test(expid):
    if VERSION == 4.0:
        output = check_cmd(f"../../bin/autosubmit create {expid} -np -v -d -cw;")
    else:
        output = check_cmd(f"../../bin/autosubmit create {expid} -np -v -d -cw -f;")
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
            with open(f"{FILE_NAME}", "a") as myfile:
                myfile.write(output)
        except Exception:
            to_exclude.append(expid)
    # print to_exclude in format ["a001","a002"]
    print(to_exclude)


open(f"{FILE_NAME}", "w").close()

# list all experiments under ~/new_autosubmit.
# except the excluded ones, which are not run
expids = []
#excluded = ['a026', 'a01y', 'a00j', 'a020', 'a01t', 'a00q', 'a00f', 'a01h', 'a00o', 'a01c', 'a00z', 't008', 'a00y', 'a00r', 't009', 'a000', 'a01e', 'a01i', 'a002', 'a008', 'a010', 'a003', 't007', 'a01d', 'autosubmit.db', 'a021', 'a00h', 'as_times.db', 'a04d', 'a02v']
excluded = []

for experiment in os.listdir(f"{EXPERIMENTS_PATH}"):
    if ( experiment.startswith("a") or experiment.startswith("t") ) and len(experiment) == 4:
        if experiment not in BANNED_TESTS:
            expids.append(experiment)
# Force
# expids = ["a001"]
perform_test(expids)