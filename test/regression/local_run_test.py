"""
This test checks that the autosubmit report command works as expected.
It is a regression test, so it is not run by default.
It only run within my home desktop computer. It is not run in the CI. Eventually it will be included TODO
Just to be sure that the autosubmitconfigparser work as expected if there are changes.
"""
import os
import subprocess
BIN_PATH = '../../bin'


def check_cmd(command, path=BIN_PATH):
    try:
        output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
        error = False
    except subprocess.CalledProcessError as e:
        output = e.output
        error = True
    return output, error

def run_test(expid):
    check_cmd(f"rm -r /home/dbeltran/new_autosubmit/{expid}/tmp/LOG_{expid}/*")
    check_cmd(f"../../bin/autosubmit create {expid} -np -v;")
    output = check_cmd(f"../../bin/autosubmit run {expid} -v")
    return output
def perform_test(expids):

    for expid in expids:
        output,error = run_test(expid)
        if "still retrieving outputs" in output.decode("UTF-8"):
            print(f"OK: autosubmit run command works as expected for {expid}")
        if error:
            for i in range(0,10):
                os.system('say ERROR ERROR ERROR PLEASE ERROR CHECK ERROR')
            print("ERR: autosubmit run command failed")
            print(output.decode("UTF-8"))
#Disabled one "t001","t002",
expids = ["t003","t004"]
perform_test(expids)