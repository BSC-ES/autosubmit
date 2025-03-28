
## How to Run These Tests

Running `test_workflow_dependencies.py` should be enough.

## How to Add New Tests

1. Create a new directory in this folder. The name can be whatever you choose. 
2. Create a folder named `conf` in the new directory. Add as many YAML files as you want.
3. The YAML files must be valid workflow configuration files. It is recommended to change the `PLATFORM` of the `jobs:` to `local`.
4. You don't have to modify the `expid` or anything else regarding the configuration. The test will take care of that.
5. Modify `test_workflow_dependencies.py` by adding the new folder name to the `@pytest.mark.parametrize` decorator in the `test_workflows_dependencies` function.

## Obtaining a Reference File

1. Run the workflow with the last known working Autosubmit version using the following command:
   ```sh
   module load autosubmit/4.1.11 # example
   autosubmit create $expid -np -f -d
   ```
2. Copy and paste the output of the command that starts with `## String representation of Job List`. 
3. Or the output of the `$expid/conf/ASLOGS/jobs_active_status.log`.
