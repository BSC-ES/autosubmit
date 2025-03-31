import io
from github import Github
import os
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from log.log import Log
from ruamel.yaml import YAML
from pathlib import Path

# TODO prevent bot spam


def sanizate_file_content(file_path: str) -> str:
    """
    Sanitize the YAML file content to comply with the workflow licence.

    :param file_path: The content of the YAML file to be sanitized.
    :type file_path: str
    :return: The sanitized YAML content.
    :rtype: str
    """
    yaml = YAML()
    data = yaml.load(file_path)

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

    def substitute_paths(d: dict) -> None:
        for key, value in d.items():
            if isinstance(value, dict):
                substitute_paths(value)
            elif isinstance(value, str) and '/' in value:
                d[key] = 'hidden'

    substitute_paths(extracted_data)
    sanitized_content_stream = io.StringIO()
    yaml.dump(extracted_data, sanitized_content_stream)
    sanitized_content = sanitized_content_stream.getvalue()
    return sanitized_content


def upload_workflow(expid: str, conf_name: str) -> None:
    """
    Method to upload a workflow to the autosubmit repo.

    :param expid: The experiment ID.
    :type expid: str
    :param conf_name: The configuration name.
    :type conf_name: str
    :return: None
    """
    # Load configuration
    as_conf = AutosubmitConfig(expid)
    experiment_data = Path(as_conf.metadata_folder) / "experiment_data.yml"

    github_token = os.getenv('GITHUB_TOKEN')
    repo_name = "BSC-ES/autosubmit-config-parser"
    branch_name = f"automatic_workflow_conf_{conf_name}"
    base_branch = "master"
    pr_title = f"[Regression test] Add workflow configuration for the experiment {conf_name}"
    pr_body = f"Automatic regression test for workflow configuration for the experiment {conf_name}"
    file_path = f"test/regression/workflows/{conf_name}/conf"
    file_content = sanizate_file_content(experiment_data)

    # Authenticate to GitHub
    g = Github(github_token)
    repo = g.get_repo(repo_name)

    # Create a new branch
    ref = repo.get_git_ref(f"heads/{base_branch}")
    repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=ref.object.sha)

    # Create a new file
    repo.create_file(
        path=file_path,
        message=f"Add configuration file for {conf_name}",
        content=file_content,
        branch=branch_name
    )

    # Create a pull request
    pr = repo.create_pull(
        title=pr_title,
        body=pr_body,
        head=branch_name,
        base=base_branch
    )
    Log.result(f"Pull request created: {pr.html_url}")
