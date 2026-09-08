##########
Provenance
##########

Autosubmit manages experiments following the `FAIR data`_ principles,
findability, accessibility, interoperability, and reusability. It
supports and uses open standards such as YAML, RO-Crate, as well as
other standards such as ISO-8601.

Each Autosubmit experiment is assigned a :doc:`unique experiment ID <expids>`
(also called expid). It also provides a central database and utilities
that permit experiments to be referenced.

The commands issued by users generate log files, and become part of the
Autosubmit experiment, as explained in the :doc:`Traceability section <traceability>`.

Users can :ref:`archive Autosubmit experiments <archive>`. These archives contain the complete
logs and other files in the experiment directory, and can be later unarchived
and executed again. Supported archival formats are ZIP and **RO-Crate**.

RO-Crate
--------

RO-Crate is a community standard adopted by other workflow managers
to package research data with their metadata. It is extensible, and contains
profiles to package computational workflows. From the `RO-Crate`_ website,
“What is RO-Crate?”:

.. pull-quote::
  RO-Crate is a community effort to establish a lightweight approach to
  packaging research data with their metadata. It is based on schema.org
  annotations in JSON-LD, and aims to make best-practice in formal
  metadata description accessible and practical for use in a wider variety
  of situations, from an individual researcher working with a folder of
  data, to large data-intensive computational research environments.

Autosubmit `conforms`_ to the following RO-Crate profiles:

* `RO-Crate 1.1`_

* `Process Run Crate 0.5`_

* `Workflow Run Crate 0.5`_

* `Workflow RO-Crate 1.0`_

Experiments archived as RO-Crate can also be uploaded to `Zenodo`_ and
to `WorkflowHub`_. The Autosubmit team worked with the WorkflowHub team
to add Autosubmit as a supported language for workflows. Both Zenodo
and WorkflowHub are issuers of `DOI`_'s (digital object identifiers),
which can be used as persistent identifiers to resolve Autosubmit
experiments referenced in papers and other documents.

RO-Crate configuration
----------------------

The content of the generated RO-Crate can be customized using the
``ROCRATE.INPUTS``, ``ROCRATE.OUTPUTS``, and ``ROCRATE.PATCH`` options.

Additional workflow inputs
~~~~~~~~~~~~~~~~~~~~~~~~~~

``ROCRATE.INPUTS`` can be used to export additional Autosubmit configuration
sections as workflow inputs.

For example:

.. code-block:: yaml

    ROCRATE:
      INPUTS:
        - PLATFORMS
      OUTPUTS:

Each entry under ``INPUTS`` refers to a top-level section of the resolved
Autosubmit configuration. The direct keys contained in that section are exported
as workflow inputs.

For example, given the following platforms configuration:

.. code-block:: yaml

    PLATFORMS:
      PLATFORM_A:
        ...
      PLATFORM_B:
        ...

exporting ``PLATFORMS`` results in ``PLATFORM_A`` and ``PLATFORM_B`` being
represented as separate workflow inputs, with identifiers such as
``#PLATFORMS.PLATFORM_A-param`` and ``#PLATFORMS.PLATFORM_B-param``.

Only the direct children of the selected section are expanded. Nested mappings
are not recursively converted into separate workflow inputs. Instead, nested
``dict`` and ``list`` values are represented as text values.

Automatically exported inputs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following sections are exported automatically and do not need to be added
under ``ROCRATE.INPUTS``:

* ``DEFAULT``
* ``EXPERIMENT``
* ``CONFIG``
* ``PROJECT``

In addition, one project-specific section is exported automatically depending on
``PROJECT.PROJECT_TYPE``:

* ``PROJECT_TYPE: GIT`` automatically exports ``GIT``.
* ``PROJECT_TYPE: LOCAL`` automatically exports ``LOCAL``.

These sections should not be added manually to ``ROCRATE.INPUTS``. Adding an
already automatically exported section results in duplicate input references and
duplicate corresponding metadata entities in the generated RO-Crate.

Available input sections
^^^^^^^^^^^^^^^^^^^^^^^^

A name used under ``ROCRATE.INPUTS`` must correspond to a top-level section that
exists in the resolved Autosubmit configuration.

The selected section must be a mapping whose direct values use supported types.

The supported direct value types are:

* string
* integer
* float
* boolean
* dictionary
* list

Dictionaries and lists are serialized as text in the RO-Crate.

If the requested top-level section does not exist, RO-Crate generation fails.

Likewise, if the selected top-level value is not a mapping, or one of its direct
values has an unsupported type, RO-Crate generation fails.

Additional workflow outputs
~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ROCRATE.OUTPUTS`` can be used to add files from the Autosubmit project directory
as workflow outputs.

For example:

.. code-block:: yaml

    ROCRATE:
      INPUTS:
      OUTPUTS:
        - "results/*.nc"

Output entries are file-name patterns evaluated recursively inside the project
directory.

A single pattern can therefore match:

* one file;
* multiple files;
* files located in nested subdirectories.

Every matched file is:

* represented as a workflow output;
* referenced from the RO-Crate ``CreateAction``;
* described as a ``File`` entity in the JSON-LD metadata;
* physically included in the RO-Crate ZIP file.

Patterns without matches
^^^^^^^^^^^^^^^^^^^^^^^^

If an output pattern does not match any file, it is silently ignored.

RO-Crate generation continues successfully and no workflow output is added for
that pattern.

Duplicate and overlapping outputs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each file selected through ``OUTPUTS`` must only be selected once.

If the same file is matched more than once, for example because:

* the same pattern is specified twice;
* two different patterns overlap;
* the file was already added to the RO-Crate through another mechanism;

RO-Crate generation currently fails.

Output patterns should therefore be defined so that the same file is not selected
multiple times.

Automatically included files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``ROCRATE.OUTPUTS`` does not define the complete contents of the generated ZIP.

The RO-Crate generator also includes files required to describe the experiment,
including experiment configuration and Autosubmit runtime information.

Therefore, ``OUTPUTS`` should be understood as a mechanism for declaring
additional workflow output files rather than as a complete list of files that will
be present in the archive.

Patching the JSON-LD metadata
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ROCRATE.PATCH`` can be used to modify or extend the JSON-LD graph generated by
the RO-Crate generator.

The patch must contain an ``@graph`` array.

For example:

.. code-block:: yaml

    ROCRATE:
      INPUTS:
      OUTPUTS:
      PATCH: |
        {
          "@graph": [
            {
              "@id": "./",
              "license": "Confidential"
            }
          ]
        }

Updating an existing entity
^^^^^^^^^^^^^^^^^^^^^^^^^^^

If a node in the patch uses an ``@id`` that already exists in the generated
RO-Crate, the existing entity is updated.

Properties not included in the patch are preserved.

For example:

.. code-block:: json

    {
      "@id": "./",
      "license": "Confidential"
    }

adds the ``license`` property to the root RO-Crate entity without replacing the
rest of its metadata.

If the patch defines a property that already exists, its existing value is
replaced by the value provided in the patch.

Adding a new entity
^^^^^^^^^^^^^^^^^^^

If the ``@id`` provided in a patch node does not already exist in the RO-Crate,
a new JSON-LD entity is added.

For example:

.. code-block:: yaml

    PATCH: |
      {
        "@graph": [
          {
            "@id": "https://example.org/organization",
            "@type": "Organization",
            "name": "Example Organization"
          }
        ]
      }

adds a new ``Organization`` entity to the JSON-LD graph.

PATCH limitations
^^^^^^^^^^^^^^^^^

The content of ``PATCH`` must be valid JSON.

If ``@graph`` is present but the patch contains invalid JSON, RO-Crate generation
fails.

If the ``PATCH`` content does not contain ``@graph``, the patch is ignored and
RO-Crate generation continues normally without applying it.

Using INPUTS, OUTPUTS, and PATCH together
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``INPUTS``, ``OUTPUTS``, and ``PATCH`` can be used simultaneously.

For example:

.. code-block:: yaml

    ROCRATE:
      INPUTS:
        - PLATFORMS

      OUTPUTS:
        - "results/*.nc"

      PATCH: |
        {
          "@graph": [
            {
              "@id": "./",
              "license": "Confidential"
            }
          ]
        }

In this case:

* the additional configuration section is exported as workflow inputs;
* matching files are registered and included as workflow outputs;
* the JSON-LD patch is applied to the generated metadata.

The three mechanisms are processed independently and can be combined in the same
RO-Crate configuration.


.. _FAIR data: https://en.wikipedia.org/wiki/FAIR_data

.. _RO-Crate: https://www.researchobject.org/ro-crate/

.. _conforms: https://github.com/ResearchObject/workflow-run-crate/pull/61

.. _Zenodo: https://zenodo.org/

.. _WorkflowHub: https://workflowhub.eu/

.. _DOI: https://en.wikipedia.org/wiki/Digital_object_identifier

.. _RO-Crate 1.1: https://www.researchobject.org/ro-crate/specification/1.1/index.html

.. _Process Run Crate 0.5: https://www.researchobject.org/workflow-run-crate/profiles/0.5/process_run_crate/

.. _Workflow Run Crate 0.5: https://www.researchobject.org/workflow-run-crate/profiles/0.5/workflow_run_crate/

.. _Workflow RO-Crate 1.0: https://about.workflowhub.eu/Workflow-RO-Crate/
