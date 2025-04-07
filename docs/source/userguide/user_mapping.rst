############
User Mapping
############

About
-----

For Autosubmit, user mapping means associating selected personal user accounts with a shared account.

The personal user account is used to access each remote platform, while the shared account is used to run the experiments on the machine where Autosubmit is deployed.

When to use
-------------

When to use: When you want to run a set of shared experiments using different HPC users.

More specifically, this can be useful for launching something like an experiment testing suite on a shared machine without having to create redundant experiments for each user who wants to run the tests.

Prerequisites
--------------

* The sysadmin of the machine where Autosubmit is deployed must have created a shared user account that will be used to run the experiments.

* The sysadmin is responsible for securing the remote keys used so that the personal user accounts are not compromised.

* The user is responsible for keeping their personal user account details (e.g., SSH keys) secure, including not sharing them with others.

* Someone has to create the ``platform_${SUDO_USER}.yml`` file for each user with access to the shared account.

* Someone has to create the ``ssh_config_${SUDO_USER}`` file for each user with access to the shared account.

How it works
--------------

The idea is to map two different things depending on the user logged in to the shared account to ensure the correct Autosubmit behavior.

* Platform_<EXPID>.yml file that contains the personal user for each platform.

(Personal user action): The user must set the environment variable "AS_ENV_PLATFORMS_PATH" to point to the file that contains the personal platforms_<EXPID>.yml file.

Defaults to: None

(One time, all shared experiments): Has to have this defined in the $autosubmit_data/$expid/conf

.. code-block:: yaml

    ...
    DEFAULT:
        ...
        CUSTOM_CONFIG:
            ...
            POST: "%AS_ENV_PLATFORMS_PATH%"
        ...
    ...


* (OPTIONAL) ssh_config file that contains the ssh config for each platform

(Personal user action): The user must set the environment variable "AS_ENV_SSH_CONFIG_PATH" to point to a file that contains the personal ~/.ssh/config file.

Defaults to: "~/.ssh/config" or "~/.ssh/config_${SUDO_USER}" if the env variable: "AS_ENV_SSH_CONFIG_PATH" is set.


How to activate it with examples
----------------------------------

* (once) Generate the platform_${SUDO_USER}.yml

.. code-block:: yaml

    Platforms:
        Platform:
            User: bscXXXXX

* (once) Generate the ssh_config_${SUDO_USER}.yml

.. code-block:: ini

    Host marenostrum5
        Hostname glogin1.bsc.es
        User bscXXXXX
    Host marenostrum5.2
        Hostname glogin2.bsc.es
        User bscXXXXX

1) Set the environment variable "AS_ENV_PLATFORMS_PATH".

.. code-block:: bash

    export AS_ENV_PLATFORMS_PATH="~/platforms/platform_${SUDO_USER}.yml"

Tip: Add it to the shared account .bashrc file.

2) Set the environment variable "AS_ENV_SSH_CONFIG_PATH" (OPTIONAL).

.. code-block:: bash

    export AS_ENV_SSH_CONFIG_PATH="~/ssh/config_${SUDO_USER}.yml"

Tip: Add it to the shared account .bashrc file.

3) Ensure that the experiments have set the %CUSTOM_CONFIG.POST% to the "AS_ENV_PLATFORMS_PATH" variable.

.. code-block:: bash

    cat $autosubmit_data/$expid/conf/minimal_<EXPID>.yml

.. code-block:: yaml

    ...
    DEFAULT:
        ...
        CUSTOM_CONFIG:
            ...
            POST: "%AS_ENV_PLATFORMS_PATH%"
        ...
    ...

4) Run the experiments.

.. code-block:: bash

    autosubmit run $expid
