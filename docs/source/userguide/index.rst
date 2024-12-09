##########
User Guide
##########

.. toctree::
   /userguide/create/index
   /userguide/configure/index
   /userguide/defining_workflows/index
   /userguide/wrappers/index
   /userguide/run/index
   /userguide/modifying_workflow/index
   /userguide/manage/index
   /userguide/monitor_and_check/index
   /userguide/set_and_share_the_configuration/index
   /userguide/variables
   /userguide/expids
   /userguide/provenance
   /userguide/traceability
   /userguide/templates

Command list
============

* expid  Create a new experiment
* create  Create specified experiment workflow
* check  Check configuration for specified experiment
* describe  Show details for specified experiments
* run  Run specified experiment
* inspect  Generate cmd files
* test  Test experiment
* testcase  Test case experiment
* monitor  Plot specified experiment
* stats  Plot statistics and summaries for specified experiment
* setstatus  Sets job status for an experiment
* recovery  Recover specified experiment
* clean  Clean specified experiment
* refresh  Refresh project directory for an experiment
* delete  Delete specified experiment
* configure  Configure database and path for autosubmit
* install  Install database for Autosubmit on the configured folder
* archive  Clean, compress and remove from the experiments' folder a finalized experiment
* unarchive  Restores an archived experiment
* migrate_exp  Migrates an experiment from one user to another
* report  extract experiment parameters
* updateversion  Updates the Autosubmit version of your experiment with the current version of the module you are using
* dbfix  Fixes the database malformed error in the historical database of your experiment
* pklfix  Fixed the blank pkl error of your experiment
* updatedescrip  Updates the description of your experiment (See: :ref:`updateDescrip`)


Tutorials (How to)
------------------

* :doc:`create/index`

* :doc:`configure/index`

* :ref:`run_modes`

* :ref:`workflow_recovery`

TODO add ``workflow_validation``.

..
  * :ref:`workflow_validation`

* :ref:`autoStatistics`

* :ref:`archive`

* :ref:`advanced_features`



