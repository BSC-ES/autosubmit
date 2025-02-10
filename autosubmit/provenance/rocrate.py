# Copyright 2015-2023 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

"""RO-Crate is a human and machine-readable format, widely used in the
workflow community with a wide variety of tools and use cases, built
focused on reproducibility.

For more about RO-Crate: https://www.researchobject.org/ro-crate/
"""

import datetime
import json
import mimetypes
import os
import subprocess
from pathlib import Path
from textwrap import dedent
from typing import List, Tuple, Union, Dict, Any

from rocrate.model.contextentity import ContextEntity
from rocrate.rocrate import ROCrate, File
from rocrate.utils import iso_now

from autosubmit.database.db_common import get_autosubmit_version
from autosubmit.database.db_common import get_experiment_descrip
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from log.log import Log, AutosubmitCritical

"""List of profiles used in our RO-Crate implementation, plus the one used
as graph context."""
PROFILES = [
    {
        "@id": "https://w3id.org/ro/wfrun/process/0.1",
        "@type": "CreativeWork",
        "name": "Process Run Crate",
        "version": "0.1"
    },
    {
        "@id": "https://w3id.org/ro/wfrun/workflow/0.1",
        "@type": "CreativeWork",
        "name": "Workflow Run Crate",
        "version": "0.1"
    },
    {
        "@id": "https://w3id.org/workflowhub/workflow-ro-crate/1.0",
        "@type": "CreativeWork",
        "name": "Workflow RO-Crate",
        "version": "1.0"
    }
]

# TODO: This could be a useful feature in ro-crate-py? Given a Python type,
#       give me the equivalent type in RO-Crate/JSON-LD.
# Some parameters in Autosubmit will contain dictionaries (like CUSTOM_CONFIG.PRE).
# We need to convert those to string in order to serialize into JSON-LD.
PARAMETER_TYPES_MAP = {
    'str': 'Text',
    'int': 'Integer',
    'float': 'Float',
    'bool': 'Boolean',
    'dict': str,
    'list': str
}

# These are the default keys exported as FormalParameters automatically.
# Others are added depending on the workflow configuration, and what the
# user has requested to export.
DEFAULT_EXPORTED_KEYS = [
    'DEFAULT',
    'EXPERIMENT',
    'CONFIG',
    'PROJECT'
]


def _add_dir_and_files(crate: ROCrate, base_path: str, relative_path: str, encoding_format: str = None) -> None:
    """Add a directory and its files into the RO-Crate.

    :param crate: the RO-Crate instance.
    :param base_path: the base path for the files being added.
    :param relative_path: the relative path (to the ``base_path``).
    :param encoding_format: the encoding format (if any).
    """
    folder = Path(base_path, relative_path)
    for root, dirs, files in os.walk(folder, topdown=True):
        for file in files:
            file_path = Path(root, file)
            _add_file(crate, base_path, file_path, encoding_format)
    crate.add_dataset(
        source=folder,
        dest_path=folder.relative_to(base_path)
    )


def _add_file(crate: ROCrate, base_path: Union[str, None], file_path: Path, encoding_format: str = None, use_uri: bool = False, **args: Any) -> Any:
    """Add a file into the RO-Crate.

    :param crate: the RO-Crate instance.
    :param base_path: the base path for the files being added. Optional.
    :param file_path: the path for the file being added.
    :param encoding_format: the encoding format (if any).
    :param use_uri: whether to use the Path as a URI or as a source directly. Defaults to ``False``.
    :return: the object returned by ro-crate-py
    :rtype: Any
    """
    properties = {
        "name": file_path.name,
        "sdDatePublished": iso_now(),
        "dateModified": datetime.datetime.utcfromtimestamp(file_path.stat().st_mtime).replace(
            microsecond=0).isoformat(),
        "contentSize": file_path.stat().st_size,
        **args
    }
    encoding_format = encoding_format if encoding_format is not None else mimetypes.guess_type(file_path)[0]
    if encoding_format is not None:
        # N.B.: We must not write ``None``'s or other missing or empty values
        #       to the encoding format if none found.
        properties['encodingFormat'] = encoding_format

    source = file_path if not use_uri else file_path.as_uri()

    dest_path = None
    if base_path:
        dest_path = file_path.relative_to(base_path)
    file = File(crate=crate,
        source=source,
        dest_path=dest_path,
        fetch_remote=False,
        validate_url=False,
        properties=properties)
    # This is to prevent ``metadata/experiment_data.yml`` to be added twice.
    # Once as the workflow main file, and twice when scanning the experiment
    # ``conf`` folder for YAML files.
    # See: https://github.com/ResearchObject/ro-crate-py/issues/165
    if file.id not in [x['@id'] for x in crate.data_entities]:
        return crate.add_file(
            source=source,
            dest_path=dest_path,
            fetch_remote=False,
            validate_url=False,
            properties=properties
        )
    return None


def _get_action_status(jobs: List[Job]) -> str:
    """Get the status of the workflow action.

    :param jobs: list of jobs, used to infer the current workflow/action status.
    :type jobs: List[str]
    :return: a valid RO-Crate and Schema.org action status.
    :rtype: str
    """
    if not jobs:
        return 'PotentialActionStatus'
    if all([job.status == Status.COMPLETED for job in jobs]):
        return 'CompletedActionStatus'
    failed_statuses = [
        Status.FAILED
    ]
    if any([job.status in failed_statuses for job in jobs]):
        return 'FailedActionStatus'
    return 'PotentialActionStatus'


def _get_git_branch_and_commit(project_path: str) -> Tuple[str, str]:
    """FIXME: workaround for: https://earth.bsc.es/gitlab/ces/autosubmit4-config-parser/-/merge_requests/2/diffs.

    :param project_path: the complete path for the Git project path.
    :type project_path: str
    :return: a tuple where the first element is the branch, and the second the commit hash
    :rtype: Tuple[str, str]
    """
    try:
        output = subprocess.check_output(
            "cd {0}; git rev-parse --abbrev-ref HEAD".format(project_path),
            shell=True, text=True)
    except subprocess.CalledProcessError as e:
        raise AutosubmitCritical("Failed to retrieve project branch...", 7014, str(e))

    project_branch = output.strip()
    Log.debug("Project branch is: " + project_branch)
    try:
        output = subprocess.check_output("cd {0}; git rev-parse HEAD".format(project_path), shell=True, text=True)
    except subprocess.CalledProcessError as e:
        raise AutosubmitCritical("Failed to retrieve project commit SHA...", 7014, str(e))
    project_sha = output.strip()
    Log.debug("Project commit SHA is: " + project_sha)
    return project_branch, project_sha


# Add Autosubmit Project to the RO-Crate.
def _get_project_entity(as_configuration: AutosubmitConfig, crate: ROCrate) -> Union[ContextEntity, None]:
    """Return a ``SoftwareSourceCode``, a specialized object from
    ``CreativeEntity`` that contains a ``codeRepository`` property
    that points to the location of files used by the Autosubmit
    workflow. Ref: https://schema.org/SoftwareSourceCode

    :param as_configuration: Autosubmit configuration object
    :type as_configuration: AutosubmitConfig
    :param crate: RO-Crate object
    :type crate: ROCrate
    :return: an entity that can be added into the RO-Crate.
    :rtype: Union[ContextEntity, None]
    """
    project = as_configuration.experiment_data['PROJECT']
    project_type = project['PROJECT_TYPE'].upper()
    project_values = as_configuration.experiment_data.get(project_type, {})
    project_path = as_configuration.get_project_dir()

    project_url = None
    project_version = None  # version is the commit/revision/etc., as per schema.org
    if project_type == 'NONE':
        project_url = ''
        project_version = ''
    elif project_type == 'SUBVERSION':
        # TODO: Maybe AutosubmitConfig needs a function to persist the subversion revision?
        raise AutosubmitCritical('Only Git and local projects are supported for RO-Crate.', 7014)
    elif project_type == 'GIT':
        project_url = project_values['PROJECT_ORIGIN']
        # TBD: Maybe the branch should be archived in the RO-Crate somehow too?
        _, project_version = _get_git_branch_and_commit(project_path)
    elif project_type == 'LOCAL':
        project_url = f'file://{project_values["PROJECT_PATH"]}'
        project_version = ''
    else:
        raise AutosubmitCritical(f'Project type {project_type} is not supported for RO-Crate.', 7014)

    parameter_value = {
        '@id': project_url,
        '@type': 'SoftwareSourceCode',
        'name': project_url,
        'sdDatePublished': iso_now(),
        'codeRepository': project_url,
        'version': project_version,
        'programmingLanguage': 'Any',
        'codeSampleType': 'template',
        'targetProduct': 'Autosubmit',
        'runtimePlatform': f'Autosubmit {as_configuration.get_version()}',
        'abstract': dedent('''\
The Autosubmit project. It contains the templates used
by Autosubmit for the scripts used in the workflow, as well as any other
source code used by the scripts (i.e. any files sourced, or other source
code compiled or executed in the workflow).''')
    }

    return ContextEntity(crate, properties=parameter_value)


def _create_formal_parameter(crate, parameter_name, name=None, **kwargs) -> Any:
    """Create a ``FormalParameter``.

    The ID's of ``FormalParameter``s must start with `#` since these
    are "internal" contextual entities.
    """
    properties = {
        '@id': f'#{parameter_name}-param',
        '@type': 'FormalParameter',
        'name': name or parameter_name,
        **kwargs
    }
    return crate.add(ContextEntity(crate, properties=properties))


def _create_parameter(crate, parameter_name, parameter_value, formal_parameter, type='PropertyValue', **kwargs) -> Any:
    properties = {
        '@id': f'#{parameter_name}-pv',
        '@type': type,
        'exampleOfWork': {
            '@id': formal_parameter['@id']
        },
        'name': parameter_name,
        'value': parameter_value,
        **kwargs
    }
    return crate.add(ContextEntity(crate, properties=properties))


def create_rocrate_archive(
        as_conf: AutosubmitConfig,
        rocrate_json: Dict[str, Any],
        jobs: List[Job],
        start_time: Union[str, None],
        end_time: Union[str, None],
        path: Path) -> ROCrate:
    """Create an RO-Crate archive using the ro-crate-py library.

    It uses the Autosubmit configuration for the prospective provenance, and also
    to locate the directories with perspective provenance.

    :param as_conf: Autosubmit configuration
    :type as_conf: AutosubmitConfig
    :param rocrate_json: RO-Crate JSON patch provided by the user
    :type rocrate_json: Dict[str, Any]
    :param jobs: List of Autosubmit jobs
    :type jobs: List[Job]
    :param start_time: Workflow run start time
    :type start_time: Union[str, None]
    :param end_time: Workflow run end time
    :type end_time: Union[str, None]
    :param path: path to save the RO-Crate in
    :type path: Path
    :return: ``True`` is the archive was created successful, ``False`` otherwise
    :rtype: object()bool
    """
    workflow_configuration = as_conf.experiment_data
    expid = workflow_configuration['DEFAULT']['EXPID']
    as_version = get_autosubmit_version(expid)
    experiment_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid)
    unified_yaml_configuration = Path(experiment_path, "conf/metadata/experiment_data.yml")

    root_profiles = [
        {"@id": profile["@id"]} for profile in PROFILES
    ]
    rocrate_metadata_json_profiles = [
        # Graph context.
        {
            "@id": "https://w3id.org/ro/crate/1.1"
        },
        {
            "@id": "https://w3id.org/workflowhub/workflow-ro-crate/1.0"
        }
    ]

    mimetypes.init()

    crate = ROCrate()
    crate.root_dataset.properties().update({
        'conformsTo': root_profiles
    })
    for profile in PROFILES:
        crate.add(ContextEntity(crate, properties=profile))

    Log.info('Creating RO-Crate archive...')

    # Create workflow configuration (prospective provenance)
    main_entity = crate.add_workflow(
        source=unified_yaml_configuration,
        dest_path=unified_yaml_configuration.relative_to(experiment_path),
        main=True,
        lang="Autosubmit",
        lang_version=as_version,
        gen_cwl=False
    )
    crate.metadata.properties().update({
        'conformsTo': rocrate_metadata_json_profiles
    })

    # Fetch the experiment description from the main database
    crate.description = get_experiment_descrip(expid)[0][0]

    # Add files generated after its execution (retrospective provenance)

    # Add original YAML configuration.
    _add_dir_and_files(crate, experiment_path, "conf")
    # Some external files could have been loaded too. That's why we use the
    # ``as_conf.current_loaded_files`` dictionary instead (name: mtime).
    experiment_configuration_path = Path(experiment_path, "conf")
    for config_entry in as_conf.current_loaded_files.keys():
        config_entry_path = Path(config_entry)
        # We do not want to add the entries under <EXPID>/conf/ again.
        if experiment_configuration_path in config_entry_path.parents:
            continue

        # Everything else is added as absolute URI, as it might be
        # a file like ``/etc/fstab``, or a private configuration from
        # the project.
        if config_entry_path.is_dir():
            crate.add_dataset(source=config_entry_path.as_uri())
        else:
            _add_file(crate, None, config_entry_path, encoding_format=None, use_uri=True)
    # Add log files.
    _add_dir_and_files(crate, experiment_path, BasicConfig.LOCAL_TMP_DIR, "text/plain")
    # Add plots files.
    _add_dir_and_files(crate, experiment_path, "plot")
    # Add status files.
    _add_dir_and_files(crate, experiment_path, "status")
    # Add SQLite DB and pickle files.
    _add_dir_and_files(crate, experiment_path, "pkl", "application/binary")

    # Register Workflow Run RO-Crate (WRROC) profile. This code was adapted from COMPSs and StreamFlow.
    #
    # See: https://gitlab.bsc.es/wdc/compss/framework/-/blob/9cc5a8a5ba76457cf9b71d698bb77b8fa0aa0c9c/compss/runtime/scripts/system/provenance/generate_COMPSs_RO-Crate.py
    #      https://github.com/alpha-unito/streamflow/blob/c04089b0c16d74f50c4380c8648f271dfd702b9d/streamflow/provenance/run_crate.py
    #      https://www.researchobject.org/workflow-run-crate/
    #      https://about.workflowhub.eu/Workflow-RO-Crate/
    # NOTE: A ``CreateAction`` can have an agent, pointing to the author
    #       of the RO-Crate or to another user. However, since we do not
    #       store that information in Autosubmit. Users wanting to use it
    #       have to add the ``PATCH`` to have an agent with the right
    #       ``@id``.
    create_action_properties = {
        "@type": "CreateAction",
        "actionStatus": {"@id": f"http://schema.org/{_get_action_status(jobs)}"},
        "description": crate.description
    }
    if start_time is not None:
        create_action_properties['startTime'] = start_time
    if end_time is not None:
        create_action_properties['endTime'] = end_time
    create_action = crate.add(
        ContextEntity(crate, '#create-action', create_action_properties)
    )
    crate.root_dataset.properties().update({
        'mentions': {'@id': create_action.id}
    })

    # Here we add the Autosubmit project as ``SoftwareCode``, and as part (``isPartOf``)
    # of the RO-Crate main ``SoftwareCode`` entity.
    try:
        project_entity = _get_project_entity(as_conf, crate)
        crate.add(project_entity)
        main_entity.append_to('hasPart', {'@id': project_entity['@id']})
    except ValueError as e:
        raise AutosubmitCritical("Failed to read the Autosubmit Project for RO-Crate...", 7014, str(e))

    # inputs and outputs
    # FIXME: Blocked by: https://earth.bsc.es/gitlab/es/autosubmit/-/issues/1045
    # TODO: Need to add input and output to ``main_entity``.
    #       "input": [ { "@id": "#id-param" }, {}, ... ]
    #       Oh, and "output" in the same way.
    #       Each input and output has the following format:
    #       { "@id": "#id-param", "@type": "FormalParameter", "additionalType": "File",
    #         "name": "input_file", "valueRequired": True }
    #       (note, outputs won't have valueRequired).
    #       The actual value of the FormalParameter goes into another entity:
    #       { "@id": "#id-pv", "@type": "PropertyValue", "exampleOfWork": {"@id": "id-param"},
    #         "name": id", "value": 42 }
    #
    # How the code will look like once we have fixed the issue linked above:
    #
    # for item in ins:
    #     formal_parameter = get_formal_parameter(item, type='in')
    #     property_value = get_parameter_value(item, parameter=formal_parameter)
    #     crate.add(formal_parameter)
    #     crate.add(property_value)
    #     if formal_parameter['@type'] == 'File':
    #         create_action.append_to('hasPart', {'@id': property_value.id})
    #     create_action.append_to('input', {'@id': formal_parameter.id})
    # for item in outs:
    #     formal_parameter = get_formal_parameter(item, type='out')
    #     property_value = get_parameter_value(item, parameter=formal_parameter)
    #     crate.add(formal_parameter)
    #     crate.add(property_value)
    #     if formal_parameter['@type'] == 'File':
    #         create_action.append_to('hasPart', {'@id': property_value.id})
    #     create_action.append_to('output', {'@id': formal_parameter.id})

    project_type = as_conf.experiment_data['PROJECT']['PROJECT_TYPE'].upper()
    exported_keys = DEFAULT_EXPORTED_KEYS.copy()
    if project_type == 'LOCAL':
        exported_keys.append('LOCAL')
    elif project_type == 'GIT':
        exported_keys.append('GIT')
    # N.B.: Subversion is not supported at the moment. See ``_get_project_entity``.
    # elif project_type == 'SUBVERSION':
    #     exported_keys.append('SUBVERSION')
    else:
        # Dummy?
        pass

    ins = []
    outs = []
    # TODO: Modify when we manage to have dicts/objects in YAML,
    #       https://earth.bsc.es/gitlab/es/autosubmit/-/issues/1045
    if 'INPUTS' in rocrate_json and rocrate_json['INPUTS']:
        ins.extend(rocrate_json['INPUTS'])
    if 'OUTPUTS' in rocrate_json and rocrate_json['OUTPUTS']:
        outs.extend(rocrate_json['OUTPUTS'])
    # Add the extra keys defined by the user in the ``ROCRATE.INPUT``.
    if ins:
        exported_keys.extend(ins)

    # Inputs.
    for exported_key in exported_keys:
        for e_k, e_v in workflow_configuration[exported_key].items():
            param_name = '.'.join([exported_key, e_k])
            Log.debug(f'Create input parameter for {param_name} = {str(e_v)}'.replace('{', '{{').replace('}', '}}'))
            python_type = type(e_v).__name__
            if python_type not in PARAMETER_TYPES_MAP:
                raise AutosubmitCritical(
                    f"Could not locate a type in RO-Crate for parameter {param_name} type {python_type}", 7014)
            # The formal parameters are added to the workflow (main entity).
            additional_type = PARAMETER_TYPES_MAP[python_type]
            if type(additional_type) is not str:
                additional_type = PARAMETER_TYPES_MAP[python_type](additional_type)
            formal_parameter = _create_formal_parameter(
                crate,
                param_name,
                additionalType=additional_type,
                valueRequired='True'
            )
            main_entity.append_to('input', {'@id': formal_parameter['@id']})
            # The parameter values are added to the CrateAction.
            parameter_value = _create_parameter(
                crate,
                param_name,
                e_v,
                formal_parameter,
                type='PropertyValue'
            )

            create_action.append_to('object', {'@id': parameter_value['@id']})

    # Outputs.
    project_path = Path(workflow_configuration['ROOTDIR'], 'proj',
                        workflow_configuration['PROJECT']['PROJECT_DESTINATION'])
    # NOTE: Do **NOT** pass ``source=project_path`` or ro-crate-py will copy the whole
    #       proj folder into the exported RO-Crate (which can have several GB's).
    crate.add_dataset(
        dest_path=project_path.relative_to(experiment_path)
    )
    for output_pattern in outs:
        for output_file in project_path.rglob(output_pattern):
            Log.debug(f'Create output parameter for {output_file}')
            # The formal parameters are added to the workflow (main entity).
            formal_parameter = _create_formal_parameter(
                crate,
                output_file.relative_to(experiment_path),
                name=output_file.name,
                additionalType='File',
                valueRequired='True'
            )
            main_entity.append_to('output', {'@id': formal_parameter['@id']})
            # The file, added to the ``CreateAction.result``, and an example
            # of the file above.
            file_entity = _add_file(
                crate,
                base_path=experiment_path,
                file_path=output_file,
                encoding_format=None,
                exampleOfWork={'@id': formal_parameter['@id']})
            create_action.append_to('result', {'@id': file_entity['@id']})

    # Merge with user provided values.
    # NOTE: It is important that this call happens after the JSON-LD has
    #       been constructed by ro-crate-py, as methods like ``add`` will
    #       replace entries (i.e. if we added before ro-crate-py, then we
    #       could have our values replaced by newly added values).
    if 'PATCH' in rocrate_json and '@graph' in rocrate_json['PATCH']:
        patch = json.loads(rocrate_json['PATCH'])
        for jsonld_node in patch['@graph']:
            crate.add_or_update_jsonld(jsonld_node)

    # Write RO-Crate ZIP.
    date = datetime.datetime.today().strftime('%Y%m%d-%H%M%S')
    crate.write_zip(Path(path, f"{expid}-{date}.zip"))
    Log.info(f'RO-Crate archive written to {experiment_path}')
    return crate
