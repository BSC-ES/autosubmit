################
Script templates
################

Autosubmit jobs require a ``FILE`` property that points to a
script template. Script templates can be written in Bash shell,
R, or Python. By default, the ``TYPE`` property of a job is set
to ``bash``. Template scripts can have any file extension,
the generated script will have it replaced by ``.cmd``.

.. code-block:: yaml
  :emphasize-lines: 3
  :caption: Job ``JOB_1`` with template script ``print_expid.sh``

  JOBS:
    JOB_1:
      FILE: print_expid.sh
      PLATFORM: LOCAL
      RUNNING: once
      TYPE: bash # default

In the example above, the script template ``print_expid.sh``
file must exist in the Autosubmit Project. When you run
``autosubmit create`` or ``autosubmit refresh``, Autosubmit
will copy the Project files, including template scripts, to the
experiment folder `proj`. The template scripts are then
preprocessed and the final script is generated when an Autosubmit
experiment is :ref:`inspected <inspect_cmd>` or
:doc:`created </userguide/run/index>`.

When Autosubmit preprocesses the template script, it replaces
*placeholders* by configuration values. A placeholder is any
configuration key wrapped by ``%%``. For example, ``%DEFAULT.EXPID%``
shown below refers to the Autosubmit configuration value
found in the ``DEFAULT.EXPID`` YAML configuration key.

Assuming that one of the Autosubmit experiment configuration files
contains the following:

.. code-block:: yaml
  :caption: Autosubmit configuration

  DEFAULT:
    EXPID: a000
    # ... other settings

and a template script used in that experiment contains the following
code:

.. code-block:: bash
  :caption: A template script

  #!/bin/bash

  echo "The experiment ID is %DEFAULT.EXPID%"

then after the template script is preprocessed by Autosubmit, the
generated script will be written in the experiment folder with the
extension ``.cmd`` and it will look like this:

.. code-block:: bash
  :caption: Generated script

  #!/bin/bash

  # Autosubmit Header
  # ...

  echo "The experiment ID is a000"

  # Autosubmit Tailer

Header and Tailer
=================

Autosubmit does not require users to modify the header and
tailer used. The header contains code to set the correct locale,
create supporting files for Autosubmit (e.g. ``<EXPID>_TEST_STAT``),
and functions used internally by Autosubmit (e.g. for checkpointing).

The tailer creates other supporting files (e.g. ``<EXPID>_TEST_COMPLETED``
and update ``<EXPID>_TEST_STAT``).

For certain platforms, like Slurm for instance, Autosubmit will populate
the header with options for the job scheduler. These options come from platform
and job configuration values (e.g. ``TASKS``, ``MEMORY``, etc.) and are
automatically translated by Autosubmit into the format expected by the
job scheduler.

If necessary, Autosubmit allows users to customize the header
and the tailer. The configuration keys ``EXTENDED_HEADER_PATH`` and
``EXTENDED_TAILER_PATH`` can be used to indicate the location
of, respectively, the header and tailer scripts used by Autosubmit.
The location is relative to the project root folder, and the scripts
are appended after the default header and tailer scripts.
