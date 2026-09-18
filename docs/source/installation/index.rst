############
Installation
############

Autosubmit is free software, licensed under |license|.

.. admonition:: Prerequisites
   :class: tip

   **Python** |python_min| – |python_max| — enforced by the installed package, and ``pip`` ≥ 24.0 (check with ``pip -V``).

   These must be available on the host machine:

   .. system_dependencies::

.. note:: The ``dot -v`` command should list ``dot``, ``pdf``, ``png``, ``SVG`` and ``Xlib`` in the device section.

.. important:: The host machine must be able to reach HPCs and clusters via password-less SSH. Generate a PEM-format key with ``ssh-keygen -t rsa -b 4096 -C "email@email.com" -m PEM``.

Install Autosubmit
==================

Autosubmit is distributed via `PyPI`_ and as source on `GitHub`_. Choose one of the methods below.
The commands use ``apt`` and assume Ubuntu 22.04 LTS or 24.04 LTS, whose default ``python3`` falls within the supported range; adjust for other distributions.

.. _install-with-pip:

Install with pip
----------------

Install system packages and the Autosubmit Python package:

.. code-block:: bash

    # Update repositories
    apt update

    # Avoid interactive stuff
    export DEBIAN_FRONTEND=noninteractive

    # System dependencies
    apt install curl git graphviz python3 python3-pip python3-tk rsync sqlite3 subversion dialog -y -q

    # Install Autosubmit from PyPI
    pip3 install autosubmit

    # Verify the CLI works
    autosubmit -h

Then proceed to :ref:`quick-setup` or :ref:`full-setup`.

Build from source
-----------------

Clone the source from `GitHub`_ and install with ``pip`` from a working copy.

.. code-block:: bash

    git clone https://github.com/BSC-ES/autosubmit.git
    cd autosubmit
    pip install .

Then proceed to :ref:`quick-setup` or :ref:`full-setup`.

Verify the install
------------------

.. runcmd:: autosubmit -v

.. _quick-setup:

Quick setup
===========

For a personal test with a user-level database. This creates ``$HOME/.autosubmitrc`` and puts everything under ``$HOME/autosubmit/``.

.. code-block:: bash

    # Quick-configure (user-level database)
    autosubmit configure

    # Create Autosubmit directories and database
    autosubmit install

    # Generate a new <EXPID>
    autosubmit expid -H "local" -d "Test exp in local."

    # Create the experiment structure
    # Since it was a new install, the <EXPID> will be a000
    autosubmit create a000

    # Run the experiment
    autosubmit run a000

.. hint::
    The ``dialog`` (GUI) library is optional. Without it, ``autosubmit configure`` prompts on the CLI. Use ``autosubmit configure -h`` to see all options.

You can pass ``--advanced`` to ``autosubmit configure`` to choose different paths:

* Experiments path (``$HOME/autosubmit/`` by default) and database filename (``autosubmit.db`` by default).
* Global logs — those not belonging to any experiment (default ``$HOME/autosubmit/logs``).
* Autosubmit metadata (default ``$HOME/autosubmit/metadata/``).

You can also configure an SMTP server and email address for notifications.

To run on a remote platform, first set up password-less SSH:

.. code-block:: bash

    # Generate a key pair for password-less ssh. PEM format is recommended as others can cause problems
    ssh-keygen -t rsa -b 4096 -C "email@email.com" -m PEM

    # Copy the public key to the remote machine
    ssh-copy-id -i ~/.ssh/id_rsa.pub user@remotehost

    # Add your key to the ssh-agent (if encrypted)
    # If not initialized, initialize it
    eval `ssh-agent -s`
    # Add the key
    ssh-add ~/.ssh/id_rsa
    # Where ~/.ssh/id_rsa is the path to your private key

Example: user-level ``.autosubmitrc``
-------------------------------------

.. code-block:: ini

   [database]
   path = /home/<user>/autosubmit
   filename = autosubmit.db

   [local]
   path = /home/<user>/autosubmit

   [globallogs]
   path = /home/<user>/autosubmit/logs

   [structures]
   path = /home/<user>/autosubmit/metadata/structures

   [historicdb]
   path = /home/<user>/autosubmit/metadata/data

   [historiclog]
   path = /home/<user>/autosubmit/metadata/logs

.. _full-setup:

.. _Shared-Filesystem:

Full setup
==========

Use this setup for production. Configuration goes in ``/etc/autosubmitrc``, which points every user on the machine at the same experiment database, so they can see and work with each other's experiments.

Priority order of configuration files:

``AUTOSUBMIT_CONFIGURATION`` > ``$HOME/.autosubmitrc`` > ``/etc/autosubmitrc``

Set the ``AUTOSUBMIT_CONFIGURATION`` environment variable to the path of an ``autosubmitrc`` file to override everything else.

.. warning:: If you already have ``$HOME/.autosubmitrc`` from :ref:`quick-setup`, delete or rename it before doing the full setup, or it will shadow ``/etc/autosubmitrc``.

.. _configure-autosubmit:

``autosubmit configure``
------------------------

Create ``/etc/autosubmitrc`` (or move ``$HOME/.autosubmitrc`` to ``/etc/autosubmitrc``) with the sections below.

Mandatory parameters
~~~~~~~~~~~~~~~~~~~~

.. code-block:: ini

    [database]
    # Accessible for all users of the filesystem
    path = <database_path>
    # Experiment database name can be whatever.
    filename = autosubmit.db

    # Accessible for all users of the filesystem, can be the same as database_path
    [local]
    path = <experiment_path>

    # Global logs, logs without <EXPID> associated.
    [globallogs]
    path = /home/<user>/autosubmit/logs

    # This depends on your email server and can be left empty if not applicable
    [mail]
    smtp_server = mail.bsc.es
    mail_from = automail@bsc.es

Recommended parameters
~~~~~~~~~~~~~~~~~~~~~~

The following parameters are the Autosubmit metadata. They are not mandatory, but it is recommended to have them set up, as some of them can positively affect the Autosubmit performance.

.. code-block:: ini

   [structures]
   path = /home/<user>/autosubmit/metadata/structures

   [historicdb]
   path = /home/<user>/autosubmit/metadata/data

   [historiclog]
   path = /home/<user>/autosubmit/metadata/logs

Optional parameters
~~~~~~~~~~~~~~~~~~~

These parameters provide extra functionalities to Autosubmit.

.. code-block:: ini

    [conf]
    # Allows using a different jobs_<EXPID>.yml default template on `autosubmit expid`
    jobs = <path_jobs>/jobs_<EXPID>.yml
    # Allows using a different platforms_<EXPID>.yml default template on `autosubmit expid`
    platforms = <path_platforms>/platforms_<EXPID>.yml

    # Autosubmit API includes extra information for some Autosubmit functions. It is optional to have access to it to use Autosubmit.
    [autosubmitapi]
    # Autosubmit API (The API is right now only provided inside the BSC network), which enables extra features for the Autosubmit GUI
    url = <url of the Autosubmit API>:<port>

    # Used for controlling the traffic that comes from Autosubmit.
    [hosts]
    authorized = [<command1,commandN> <machine1,machineN>]
    forbidden = [<command1,commandN> <machine1,machineN>]

About hosts parameters:

From ``3.14+`` onwards, the users can tailor Autosubmit commands to run on specific machines. Previously, only the run was affected by the deprecated ``whitelist`` parameter.

* ``authorized = [<command1,commandN> <machine1,machineN>]`` list of machines that can run given autosubmit commands. If the list is empty, all machines are allowed.
* ``forbidden = [<command1,commandN> <machine1,machineN>]`` list of machines that cannot run given autosubmit commands. If the list is empty, no machine is forbidden.

Example: BSC ``/etc/autosubmitrc``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: ini

   [database]
   path = /esarchive/autosubmit
   filename = ecearth.db

   [local]
   path = /esarchive/autosubmit

   [conf]
   jobs = /esarchive/autosubmit/default
   platforms = /esarchive/autosubmit/default

   [mail]
   smtp_server = mail.bsc.es
   mail_from = automail@bsc.es

   [hosts]
   authorized = [run bscearth000,bscesautosubmit01,bscesautosubmit02] [stats, clean, describe, check, report,dbfix,pklfix, upgrade,updateversion all]
   forbidden = [expid, create, recovery, delete, inspect, monitor, recovery, configure,setstatus,testcase, test, refresh, archive, unarchive bscearth000,bscesautosubmit01,bscesautosubmit02]

``autosubmit install``
----------------------

Finally, create the directories defined in ``/etc/autosubmitrc`` and initialise a blank database:

.. code-block:: bash

    autosubmit install

Dependencies and licenses
=========================

.. dropdown:: Python dependencies and licenses
   :icon: package

   Generated at build time from ``pyproject.toml``.

   .. dependencies_licenses::

.. _PyPI: https://pypi.org/project/autosubmit/
.. _GitHub: https://github.com/BSC-ES/autosubmit
