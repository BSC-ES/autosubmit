############
Installation
############

How to install
==============

The Autosubmit code is hosted in GitHub. The Autosubmit Python package is available through PyPI, the primary source for Python packages.

- Pre-requisites: bash, python >= 3.9, sqlite3, git-scm > 1.8.2, subversion, pip >= 24.0, dialog, curl, python-tk(tkinter in centOS), graphviz >= 2.41, rsync

.. important:: (SYSTEM) Python version has to be at least 3.9 and at most 3.12. 
.. important:: (SYSTEM) Graphviz version must be >= 2.38 except 2.40(not working). You can check the version using dot -v.
.. important:: (SYSTEM) Pip version must be >= 24.0. You can check the version using pip -V.

- Python dependencies: configobj>=5.0.6, argparse>=1.4.0 , python-dateutil>=2.8.2, matplotlib==3.4.3, numpy==1.21.6, py3dotplus>=1.1.0, pyparsing>=3.0.7, paramiko>=2.9.2, mock>=4.0.3, six>=1.10, portalocker>=2.3.2, networkx==2.6.3, requests>=2.27.1, bscearth.utils>=0.5.2, cryptography>=36.0.1, setuptools>=60.8.2, xlib>=0.21, pip>=22.0.3, ruamel.yaml, pythondialog, pytest, nose, coverage, PyNaCl==1.4.0, six>=1.10.0, requests, xlib, Pygments, packaging==19, typing>=3.7, autosubmitconfigparser

.. important:: ``dot -v`` command should contain "dot", pdf, png, SVG, Xlib in the device section.

.. important:: The host machine has to be able to access HPCs/Clusters via password-less ssh. Ensure that the ssh key is in PEM format ``ssh-keygen -t rsa -b 4096 -C "email@email.com" -m PEM``.

To install autosubmit, execute the following:
::

    pip install autosubmit

Or download, unpack and:
::

    pip install .

.. hint::
    To check if Autosubmit is installed, run ``autosubmit -v`` This command will print Autosubmit's current version

.. runcmd:: autosubmit -v

.. hint::
    To read Autosubmit's readme file, run ``autosubmit readme``

.. hint::
    To see the changelog, use ``autosubmit changelog``

The sequence of instructions to install Autosubmit and its dependencies with pip.
---------------------------------------------------------------------------------

.. warning:: The following instructions are for Ubuntu 20.04 LTS. The instructions may vary for other UNIX distributions.

.. code-block:: bash

    # Update repositories
    apt update

    # Avoid interactive stuff
    export DEBIAN_FRONTEND=noninteractive

    # Dependencies
    apt install wget curl python3.9 python3.9-tk python3.9-dev graphviz -y -q

    # Additional dependencies related with pycrypto
    apt install build-essential libssl-dev libffi-dev -y -q

    # Install Autosubmit using pip
    pip3 install autosubmit

    # Check that we can execute autosubmit commands
    autosubmit -h

For a very quick test, you can follow the next instructions to configure and run Autosubmit at the user level. Otherwise, please go directly to `How to configure Autosubmit <https://autosubmit.readthedocs.io/en/master/installation/index.html#how-to-configure-autosubmit>`_ .

.. code-block:: bash

    # Quick-configure ( user-level database)
    autosubmit configure

    # Install
    autosubmit install

    # Quick-start

    # Get <EXPID>
    autosubmit a000 -H "local" -d "Test exp in local."

    # Create with
    # Since it was a new install, the <EXPID> will be a000
    autosubmit create a000

    # In case you want to use a remote platform

    # Generate a key pair for password-less ssh. PEM format is recommended as others can cause problems
    ssh-keygen -t rsa -b 4096 -C "email@email.com" -m PEM

    # Copy the public key to the remote machine
    ssh-copy-id -i ~/.ssh/id_rsa.pub user@remotehost


    # Add your key to the ssh-agent ( if encrypted )

    # If not initialized, initialize it
    eval `ssh-agent -s`

    # Add the key
    ssh-add ~/.ssh/id_rsa
    # Where ~/.ssh/id_rsa is the path to your private key

    # run
    autosubmit run a000


The sequence of instructions to install Autosubmit and its dependencies with conda.
-----------------------------------------------------------------------------------

.. warning:: The following instructions are for Ubuntu 20.04 LTS. The instructions may vary for other UNIX distributions.

.. warning:: This procedure is still WIP. You can follow the process at `issue #864 <https://earth.bsc.es/gitlab/es/autosubmit/-/issues/886>`_. We strongly recommend using the pip procedure.

If you don't have conda installed yet, we recommend following `Installing Miniconda <https://docs.conda.io/projects/miniconda/en/latest/index.html>`_.

.. code-block:: bash

    # Download git
    apt install git -y -q

    # Download autosubmit
    git clone https://github.com/BSC-ES/autosubmit.git -b v4.0.0b
    cd autosubmit

    # Create a Conda environment from YAML with autosubmit dependencies
    conda env create -f environment.yml -n autosubmitenv

    # Activate env
    conda activate autosubmitenv

    # Install autosubmit
    pip install autosubmit

    # Test autosubmit
    autosubmit -v

For a very quick test, you can follow the next instructions to configure and run Autosubmit at the user level. Otherwise, please go directly to `How to configure Autosubmit <https://autosubmit.readthedocs.io/en/master/installation/index.html#how-to-configure-autosubmit>`_

.. code-block:: bash

    # Quick-configure ( user-level database)
    autosubmit configure

    # Install
    autosubmit install

    # Quick-start
    # Get <EXPID>
    autosubmit <EXPID> -H "local" -d "Test exp in local."

    # Create with
    # Since it was a new install, the <EXPID> will be a000
    autosubmit create a000

    # In case you want to use a remote platform

    # Generate a key pair for password-less ssh. PEM format is recommended as others can cause problems
    ssh-keygen -t rsa -b 4096 -C "email@email.com" -m PEM

    # Copy the public key to the remote machine
    ssh-copy-id -i ~/.ssh/id_rsa.pub user@remotehost

    # Add your key to ssh agent ( if encrypted )
    # If not initialized, initialize it
    eval `ssh-agent -s`
    # Add the key
    ssh-add ~/.ssh/id_rsa
    # Where ~/.ssh/id_rsa is the path to your private key

    # run
    autosubmit run a000

.. hint::
    After installing the Conda, you may need to close the terminal and re-open it so the installation takes effect.


How to configure Autosubmit
===========================

There are two methods of configuring the Autosubmit main paths.

* ``autosubmit configure`` is suited for a personal/single user who wants to test Autosubmit in the scope of ``$HOME``. It will generate an ``$HOME/.autosubmitrc`` file that overrides the machine configuration.

Manually generate an ``autosubmitrc`` file in one of these locations, which is the recommended method for a production environment with a shared database in a manner that multiple users can share and view others' experiments.

* ``/etc/autosubmitrc``, System level configuration.

* Set the environment variable ``AUTOSUBMIT_CONFIGURATION`` to the path of the ``autosubmitrc`` file. This will override all other configuration files.

.. important::  `.autosubmitrc` user level precedes system configuration unless the environment variable is set. `AUTOSUBMIT_CONFIGURATION` > `$HOME/.autosubmitrc > /etc/autosubmitrc`

Quick Installation - Non-shared database (user level)
------------------------------------------------------

After the package installation, you have to configure at least the database and path for Autosubmit.

To use the default settings, create a directory called ``autosubmit`` (``mkdir $HOME/autosubmit``) in your home directory before running the ``configure`` command.

::

    autosubmit configure

``autosubmit generate`` will always generate a file called ``.autosubmitrc`` in your ``$HOME``.

You can add ``--advanced`` to the configure command for advanced options.

::

    autosubmit configure --advanced

It will allow you to choose different directories:

* Experiments path and database name ( ``$HOME/autosubmit/`` by default ) and database name ( ``$HOME/autosubmit/autosubmit.db``  by default )
* Path for the global logs (those not belonging to any experiment). Default is ``$HOME/autosubmit/logs``.
* Autosubmit metadata. Default is ``$HOME/autosubmit/metadata/``

Additionally, it also provides the possibility of configuring an SMTP server and an email account to use the email notifications feature.

.. hint::
    The ``dialog`` (GUI) library is optional. Otherwise, the configuration parameters will be prompted (CLI). Use ``autosubmit configure -h`` to see all the allowed options.

Example - Local - .autosubmitrc skeleton
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: ini

   [database]
   path = /home/dbeltran/autosubmit
   filename = autosubmit.db

   [local]
   path = /home/dbeltran/autosubmit

   [globallogs]
   path = /home/dbeltran/autosubmit/logs

   [structures]
   path = /home/dbeltran/autosubmit/metadata/structures

   [historicdb]
   path = /home/dbeltran/autosubmit/metadata/data

   [historiclog]
   path = /home/dbeltran/autosubmit/metadata/logs


Production environment installation - Shared-Filesystem database
----------------------------------------------------------------

.. _Shared-Filesystem:

.. warning:: Keep in mind the .autosubmitrc precedence. If you, as a user, have a .autosubmitrc generated in the quick-installation, you have to delete or rename it before using the production environment installation.

Create an ``/etc/autosubmitrc`` file or move it from ``$HOME/.autosubmitrc`` to ``/etc/autosubmitrc`` with the information as follows:

Mandatory parameters of /etc/autosubmit
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
    path = /home/dbeltran/autosubmit/logs

    # This depends on your email server and can be left empty if not applicable
    [mail]
    smtp_server = mail.bsc.es
    mail_from = automail@bsc.es

Recommendable parameters of /etc/autosubmit
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following parameters are the Autosubmit metadata, it is not mandatory, but it is recommendable to have them set up as some of them can positively affect the Autosubmit performance.

.. code-block:: ini

   [structures]
   path = /home/dbeltran/autosubmit/metadata/structures

   [historicdb]
   path = /home/dbeltran/autosubmit/metadata/data

   [historiclog]
   path = /home/dbeltran/autosubmit/metadata/logs

Optional parameters of /etc/autosubmit
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These parameters provide extra functionalities to Autosubmit.

.. code-block:: ini

    [conf]
    # Allows using a different jobs_<EXPID>.yml default template on `autosubmit expid ``
    jobs = <path_jobs>/jobs_<EXPID>.yml
    # Allows using a different platforms_<EXPID>.yml default template on `autosubmit expid `
    platforms = <path_platforms>platforms_<EXPID>.yml> path to any jobs.yml

    # Autosubmit API includes extra information for some Autosubmit functions. It is optional to have access to it to use Autosubmit.
    [autosubmitapi]
    # Autosubmit API (The API is right now only provided inside the BSC network), which enables extra features for the Autosubmit GUI
    url = <url of the Autosubmit API>:<port>

    # Used for controlling the traffic that comes from Autosubmit.
    [hosts]
    authorized =  [<command1,commandN> <machine1,machineN>]
    forbidden =   [<command1,commandN> <machine1,machineN>]

About hosts parameters:

From 3.14+ onwards, the users can tailor Autosubmit commands to run on specific machines. Previously, only the run was affected by the deprecated whitelist parameter.

* authorized =  [<command1,commandN> <machine1,machineN>] list of machines that can run given autosubmit commands. If the list is empty, all machines are allowed.
* forbidden =   [<command1,commandN> <machine1,machineN>] list of machines that cannot run given autosubmit commands. If the list is empty, no machine is forbidden.

Example - BSC - /etc/autosubmitrc skeleton
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
        authorized =  [run bscearth000,bscesautosubmit01,bscesautosubmit02] [stats, clean, describe, check, report,dbfix,pklfix, upgrade,updateversion all]
        forbidden =  [expìd, create, recovery, delete, inspect, monitor, recovery, migrate, configure,setstatus,testcase, test, refresh, archive, unarchive bscearth000,bscesautosubmit01,bscesautosubmit02]

Experiments database installation
=================================

As the last step, ensure to install the Autosubmit database. To do so, execute  ``autosubmit install``.

.. code-block:: bash

    autosubmit install

This command will generate a blank database in the specified configuration path.
