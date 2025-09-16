Create an Experiment
====================

Create new experiment
---------------------

To create a new experiment, run the following command:

.. code-block:: bash

    autosubmit expid -H <HPCname> -d "<Description>"

Where:

* ``HPCname`` - The name of the main HPC platform for the experiment (will be the default platform for all tasks)
* ``Description`` - A brief description of the experiment

Options:

.. runcmd:: autosubmit expid -h

Examples:

.. code-block:: bash

    # Basic experiment creation
    autosubmit expid --HPC marenostrum4 --description "experiment is about..."

    # Create from repository with minimal configuration
    autosubmit expid -min -repo https://earth.bsc.es/gitlab/ces/auto-advanced_config_example \
                     -b main -conf as_conf -d "minimal config example"

    # Create dummy experiment for testing
    autosubmit expid -dm -d "dummy test"

Configuring Default Platforms
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you have an ``autosubmitrc`` or ``.autosubmitrc`` file in your home directory, you can configure a default platforms file that will be used as a template for new experiments.

Add the following to your autosubmitrc file:

.. code-block:: ini

    [conf]
    custom_platforms = /home/Earth/user/custom.yml

.. note::
   The path must be absolute (like the output of ``pwd``) and include the filename of your custom platforms configuration.

Copy another experiment
-----------------------

To copy an existing experiment with a new unique identifier:

.. code-block:: bash

    # Copy experiment with default configuration
    autosubmit expid -y <EXPID> -H <HPCname> -d "<Description>"

    # Copy experiment with custom configuration path
    autosubmit expid -y <EXPID> -c <PATH> -H <HPCname> -d "<Description>"

Where:

* ``EXPID`` - The experiment identifier to copy from
* ``HPCname`` - The name of the main HPC platform for the new experiment
* ``Description`` - A brief description of the new experiment
* ``PATH`` - (Optional) Path to custom configuration directory

Examples:

.. code-block:: bash

    # Copy experiment to Ithaca platform
    autosubmit expid -y a0b1 -H ithaca -d "Copy of experiment a0b1"

    # Copy with custom configuration path
    autosubmit expid -y a0b1 -p "/esarchive/autosubmit/genericFiles/conf" \
                     -H marenostrum4 -d "Modified copy of a0b1"

.. warning:: You can only copy experiments created with Autosubmit 3.11 or above.

.. tip::
   You can configure default platforms in your ``autosubmitrc`` file:

   .. code-block:: ini

       [conf]
       custom_platforms = /home/Earth/user/custom.yml

Create a dummy experiment
-------------------------

Dummy experiments are useful for testing your Autosubmit configuration without expensive computations. They behave like regular experiments but only submit sleep jobs to the HPC platform.

To create a dummy experiment:

.. code-block:: bash

    autosubmit expid -H <HPCname> -dm -d "<Description>"

Where:

* ``HPCname`` - The HPC platform to test
* ``Description`` - A brief description

Example:

.. code-block:: bash

    autosubmit expid -H ithaca -dm -d "Testing Autosubmit configuration"

Create a test case experiment
------------------------------

Test case experiments use a reserved "t" prefix in their experiment ID to distinguish testing suites from production runs. They allow you to create experiments with specific configurations for testing purposes.

To create a test case experiment:

.. code-block:: bash

    autosubmit testcase

Options:

.. runcmd:: autosubmit testcase -h

Example:

.. code-block:: bash

    autosubmit testcase -d "TEST CASE cca-intel auto-ecearth3 layer 0" \
                        -H cca-intel -b 3.2.0b_develop -y a09n

.. _create_profiling:

Profiling experiment creation
------------------------------

You can profile the experiment creation process to analyze performance. To enable profiling, add the ``--profile`` (or ``-p``) flag to your ``autosubmit create`` command:

.. code-block:: bash

    autosubmit create --profile <EXPID>

.. include:: ../../_include/profiler_common.rst