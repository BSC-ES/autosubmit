
## How to Run These Tests

Running `test_workflow_dependencies.py` should be enough.

## How to Add New Tests

1. Download an $expid/conf/metadata/experiment_data.yml.
2. Use tests/resources/upload_workflow_config.py <input_experiment_data.yml> <tests/regression/workflows/<new_folder>/conf>
3. Enable add_new_tests in test_workflow_dependencies.py
4. Run test_workflow_dependencies.py
