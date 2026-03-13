Inspect & Debug Tools
=====================

.. contents::
   :local:
   :depth: 2


Inspect ( dry-run )
-------------------

Generate all ``.cmd`` files for a given experiment without submitting jobs to
any remote platform. This is the primary tool for debugging configuration and
template issues offline.

.. code-block:: text

   autosubmit inspect <expid> [options]

**Positional arguments**

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Argument
     - Description
   * - ``expid``
     - Experiment identifier (e.g. ``a000``).

**Optional arguments**

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Option
     - Description
   * - ``-f``, ``--force``
     - Overwrite all existing ``.cmd`` files. Required when re-running inspect
       on an experiment that has already been inspected.
   * - ``-cw``, ``--check_wrapper``
     - Generate possible wrappers in the current workflow.
   * - ``-q``, ``--quick``
     - Only check one job per section.

Overview
--------


Autosubmit provides debugging tools to help diagnose configuration and parameter
substitution issues **before** submitting jobs to a platform. The primary tool
for this is the ``inspect`` command, also known as **dry-run mode**.

Dry-run mode generates job scripts locally without connecting to any remote
platform, making it fast and safe for iterative debugging.

The ``inspect`` command renders the final job scripts as Autosubmit would
submit them. When combined with the ``--quick`` flag, it only processes one job
per section, allowing you to verify all your ``.cmd`` templates inexpensively.


What inspect checks
-------------------


Parameter Substitution
^^^^^^^^^^^^^^^^^^^^^^

Autosubmit replaces ``%VARIABLE%`` placeholders in templates with values
from your experiment configuration. The ``inspect`` command renders
the final script so you can verify every substitution resolved correctly.

Common substitution issues:

- **Wrong value substituted** — the variable resolves but with an unexpected
  value, often caused by a typo in the key name or an override in the wrong
  section.
- **Empty substitutions** — the variable resolves to an empty string because
  the value is not defined.

Example:

Given a job template containing:

.. code-block:: bash

   echo "Running on %HPCARCH%"

If ``HPCARCH`` is not defined in your experiment config, the rendered output
will show:

.. code-block:: bash

   echo "Running on "

Use ``inspect`` to catch this before submission:

.. code-block:: bash

   autosubmit inspect a000 --quick


CMD Syntax Validation
^^^^^^^^^^^^^^^^^^^^^

When ``VALIDATE: True`` is set under a job in your configuration, the
``inspect`` command will also check the rendered script for syntax errors.
This is particularly useful when combined with ``--quick`` to validate one
job per section efficiently.

To enable syntax validation for a job:

.. code-block:: yaml

   JOBS:
       JOB:
           VALIDATE: True

After running:

.. code-block:: bash

   autosubmit inspect a000 --quick

Any syntax issues detected in the rendered ``.cmd`` files will be reported
in the command output.

.. note::

   Syntax validation checks for common issues like unmatched quotes, missing
   variables, and invalid scheduler directives. It does not execute the script,
   so it cannot catch runtime errors. This validation can also be enabled with
   ``autosubmit run``.



Common Debugging Workflow
-------------------------

The recommended workflow to debug parameter issues before submission:

1. **Edit** your jobs configuration (``conf/*.yml``). Add ``VALIDATE: True``
   to enable syntax checks in rendered scripts.

   .. code-block:: yaml

      JOBS:
          JOB:
              VALIDATE: True

2. **Run** ``autosubmit inspect <expid> --quick`` to render the first
   chunk/split of each section. Use ``-f`` to overwrite any previously
   generated ``.cmd`` files.

   .. code-block:: bash

      autosubmit inspect a000 --quick -f

3. **Search** the output for any empty substitutions.

4. **Verify** scheduler directives, module paths, and executable arguments
   look correct.

5. **Iterate** fix configuration and re-run inspect until the output is
   clean.

6. **Submit** using ``autosubmit run <expid>``.


.. seealso::

   :doc:`/userguide/configure/index`
      Configuration file reference.

   :doc:`/userguide/run/index`
      Running an experiment after verification.

   :doc:`/troubleshooting/index`
      Common issues and solutions.
