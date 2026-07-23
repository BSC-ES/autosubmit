Wrapper engines
===============

A *wrapper engine* is a tool that enables the *in situ* management of tasks within wrappers on HPC platforms. These tools are intended to abstract both
developers and users from the complexity of managing wrappers in HPC resource allocations. Due to their nature, these wrappers are referred to as "delegated".

The integration of these tools into Autosubmit allows for the creation of aggregation types with dependencies more complex than those supported by default,
such as vertical, horizontal, or hybrid aggregations. That is, one or more sections can be grouped under a single delegated wrapper regardless of the
dependencies that exist between them. Ultimately, this provides greater flexibility in configuration and better adaptability at runtime.

This guide presents the interface that Autosubmit provides for communicating with the wrapper engines.

.. hint:: Autosubmit has already integrated Flux as its primary wrapper engine. At this time, it is the only one available.

Configuring a delegated wrapper
-------------------------------

A delegated wrapper can be configured in a similar way to a traditional wrapper, with the following changes:

.. code-block:: yaml

    WRAPPERS:
        WRAPPER_POST:
            POLICY: flexible
            MIN_WRAPPED: 2
            MAX_WRAPPED: 18
            TYPE: delegated
            JOBS_IN_WRAPPER: POST1 POST2
            METHOD: flux
        CUSTOM_ENV_SETUP: |
            module load miniconda
            conda activate flux-0.84-0.50

A new ``TYPE`` of wrapper, the ``delegated`` wrapper, is provided in addition to the existing ones. The ``METHOD`` directive is used to specify the wrapper
engine to be used. In this example, it is Flux.

Since a wrapper engine may not be directly accessible from a resource allocation---for example, because it is inside a virtual environment or requires
initializing a container---a new directive specific to this type of wrapper, ``CUSTOM_ENV_SETUP``, is provided, allowing the workflow developers to specify
the instructions needed to make the environment accessible. In the example, a Conda environment containing the Flux installation is enabled.

The remaining directives, such as ``POLICY``, ``MIN_WRAPPED``, ``MAX_WRAPPED``, or ``JOBS_IN_WRAPPER``, among others, behave exactly as you would expect
from standard wrapper types, except ``EXTEND_WALLCLOCK``, which is disabled.

Integration of a wrapper engine
-------------------------------

Autosubmit provides an intuitive interface that makes it easy to interact with wrapper engines. This section details each of the steps required to
successfully integrate a new tool.

Requirements for integrating a wrapper engine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before integrating a wrapper engine, it is important to ensure that the tool meets the necessary requirements for integration:

- A specific tool for managing wrappers *on-site* does not necessarily have to be available on the cluster. In such cases, the user is responsible for
  deploying it. Therefore, such a tool should be easy to deploy. This entails supporting a variety of architectures (x86, RISC-V, ARM), working on a variety
  of schedulers (Slurm, PBS, Flux, etc.) and regardless of the underlying technologies configured, as the MPI implementation (Open MPI, Intel MPI).
- Must be capable of running any kind of task application (MPI, Bash, Python, containerized, etc.).
- Should provide a simple way to define a workflow.
- The *in situ* scheduler must be able to deal with dependencies between tasks. The dependency definition should be explicit and straightforward, following
  a task flow basis, rather than a data flow where tasks are executed as their inputs become available.
- Must be capable of performing precise resource management for each task, also on multi-node configurations.
- It should be fault-tolerant.
- Must be currently maintained and should be recognized within the workflow or HPC communities.

The Interface
~~~~~~~~~~~~~

For each delegated wrapper, Autosubmit generates a pair of files, ``subworkflow_***.json`` and ``<EXPID>_WRAPPER_***.cmd``, using the ``DelegatedWrapper``
and ``DelegatedWrapperBuilder`` classes, respectively. Both are sent to the HPC platform along with the individual scripts for each task.

The first file contains the structured definition of the subworkflow that the wrapper engine must execute. This JSON structure contains a common ``cwd``
(working directory) for the entire wrapper, and the definition of the nodes and edges of the DAG (Directed Acyclic Graph) representing the subworkflow.

The ``tasks`` section contain information about each of the wrapper's tasks: their ``id`` (which matches their name), along with a representation of the
resource request according to the Autosubmit configuration, these being ``nodes``, ``threads``, ``processors``, ``wallclock``, and ``exclusive``.

The edges, defined as ``dependencies``, represent the dependencies between tasks. They contain a ``from`` ``to`` pair indicating the source and the
target nodes of the dependency, along with the link's ``weight``, which corresponds to the *wallclock* of the source task.

The second file is the Bash script that Autosubmit sends to the HPC platform to perform resource allocation and generate the *wrapper script*. This
*wrapper script* is responsible for executing the wrapper via the wrapper engine's API, based on the information in the JSON file.

Example of a JSON specification for a delegated wrapper
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This section includes a JSON example for a simple wrapper consisting of two tasks: ``POST1`` and ``POST2``, with a dependency between them.

.. code-block:: json

    {
        "directed": true,
        "multigraph": false,
        "graph": {
            "wrapper_defaults": {
                "cwd": "/***/a00i/LOG_a00i"
            }
        },
        "tasks": [
            {
                "nodes": null,
                "threads": 2,
                "processors": 10,
                "wallclock": 600,
                "exclusive": true,
                "id": "a00i_20250101_fc0_1_POST1"
            },
            {
                "nodes": null,
                "threads": 2,
                "processors": 10,
                "wallclock": 600,
                "exclusive": true,
                "id": "a00i_20250101_fc0_1_POST2"
            }
        ],
        "dependencies": [
            {
                "weight": 600,
                "from": "a00i_20250101_fc0_1_POST1",
                "to": "a00i_20250101_fc0_1_POST2"
            }
        ]
    }

The wrapper script
^^^^^^^^^^^^^^^^^^

The wrapper script is the key component for interaction between Autosubmit and the wrapper engine. This script is generated on the platform itself using the
script that Autosubmit sends to request the allocation and initialize the wrapper engine environment. It is responsible for parsing the JSON file,
reconstructing the task and dependency graph, and executing the tasks in an orderly manner on the resources specified for each one.

The wrapper script can be written in any language, depending on the availability of wrapper engine bindings. Naturally, its complexity and size will depend on
the complexity of the interaction with the wrapper engine, and the sending and monitoring of tasks would require specific knowledge of it.

The graph can be manually reconstructed from the JSON or by using the Python `networkx` library, which was originally used to build it. It is important to note
that this library may not be available in the remote environment.

Creating a new wrapping method
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``METHOD`` directive in the wrapper configuration is used to specify the wrapper engine to be used. The default engine is Flux, although other ones can
be supported. To support new methods, modifications must be made to the ``WrapperFactory`` classes for each supported platform. For example, if you want to
integrate a new engine called ``newengine`` that is compatible with Slurm platforms, you must edit its wrapper factory appropriately to point to the
corresponding ``DelegatedWrapperBuilder`` class:

.. code-block:: python

    class SlurmWrapperFactory(WrapperFactory):

        def delegated_wrapper(self, **kwargs):
            if kwargs["method"] == 'flux':
                return FluxWrapperBuilder(**kwargs)
            elif kwargs["method"] == 'newengine':
                return NewEngineWrapperBuilder(**kwargs)
            else:
                raise NotImplementedError(self.exception_method(kwargs["method"]))

``DelegatedWrapperBuilder`` is an abstract base class that defines the common interface for all wrapper engines. This class is responsible for generating
both the script sent to the platform and the wrapper script that coordinates the execution of tasks. To do this, it provides a configurable general structure
with specific elements dependent on each wrapper engine, such as the *wrapper script* and the command to execute it within the allocation.

The ``NewEngineWrapperBuilder`` class in the following example implements ``DelegatedWrapperBuilder``. ``NewEngineWrapperBuilder`` is responsible for
implementing the function that generates the wrapper script and for filling in some properties of the class.

.. code-block:: python

    class NewEngineWrapperBuilder(DelegatedWrapperBuilder):

        @property
        def name(self):
            return "newengine"
        
        @property
        def command(self) -> str:
            return f"srun <call_to_the_wrapper_engine> python {self._delegated_script_name}"

        def _generate_delegated_script(self) -> str:
            return textwrap.dedent(f"""
                #!/usr/bin/env python
                import json

                def main():
                    with open(f"subworkflow_{self._unique_part}.json", "r") as f:
                        dag_json = f.read()
                    
                    # Other logic to reconstruct the graph and execute the tasks through the wrapper engine API

                if __name__ == "__main__":
                    main()
                """)

Literature
----------
- Goitia González, P., et al. (2026). *In Situ Workflow Orchestration in HPC to Optimize Extreme Climate Simulation Workflows*. Universidad de Cantabria. https://earth.bsc.es/wiki/lib/exe/fetch.php?media=library:external:pablo_goitia_tfm.pdf
- Giménez de Castro Marciani, M., et al. (2026). *Accelerating Earth System workflows with in situ workflow task management*. EGU General Assembly 2026, Vienna, Austria, 3-8 May 2026. EGU26-11759. https://doi.org/10.5194/egusphere-egu26-11759
- Goitia González, P., et al. (2026). *Optimizing the Destination Earth workflow with in situ HPC task orchestration*. EGU General Assembly 2026, Vienna, Austria, 3-8 May 2026. EGU26-12058. https://doi.org/10.5194/egusphere-egu26-12058
