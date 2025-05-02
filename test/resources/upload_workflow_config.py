"""
This script is used to upload a new reference configuration to the regression test suite. The script processes a YAML configuration file and extracts the relevant sections. It then modifies the extracted data to ensure that the configuration is compatible with the regression test suite. The modified configuration is saved to a new YAML file.
TODO: Make it an autosubmit command
"""
import sys
from ruamel.yaml import YAML
from pathlib import Path


def process_yaml(input_file_path, output_file_path):
    yaml = YAML()
    with open(input_file_path, 'r') as file:
        data = yaml.load(file)

    sections = ['DEFAULT', 'JOBS', 'EXPERIMENT', 'PROJECT', 'GIT']
    extracted_data = {key: data.get(key, {}) for key in sections}

    if 'JOBS' in extracted_data:
        for job in extracted_data['JOBS'].values():
            if 'PLATFORM' in job:
                job['PLATFORM'] = 'local'
            if 'ADDITIONAL_FILES' in job:
                del job['ADDITIONAL_FILES']

    if 'PROJECT' in extracted_data and 'PROJECT_TYPE' in extracted_data['PROJECT']:
        extracted_data['PROJECT']['PROJECT_TYPE'] = 'none'

    if 'DEFAULT' in extracted_data and 'HPCARCH' in extracted_data['DEFAULT']:
        extracted_data['DEFAULT']['HPCARCH'] = 'local'

    def substitute_paths(d):
        for key, value in d.items():
            if isinstance(value, dict):
                substitute_paths(value)
            elif isinstance(value, str) and '/' in value:
                d[key] = 'hidden'

    substitute_paths(extracted_data)
    Path(output_file_path).mkdir(parents=True, exist_ok=True)
    with open(f"{output_file_path}/conf.yml", 'w') as file:
        yaml.dump(extracted_data, file)


# TODO change output_yaml_file to conf_name and set the path to the test automatically
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_yaml_file> <output_yaml_file>")
        sys.exit(1)

    process_yaml(sys.argv[1], sys.argv[2])
