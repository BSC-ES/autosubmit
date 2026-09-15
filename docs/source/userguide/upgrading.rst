.. _upgrading_from_4_1:

Upgrading
=========

.. important::
   Stop the Autosubmit instance related to the experiment before starting. Stopping Autosubmit does
   not stop jobs that are already running on the platform. Cancel them or wait for them to finish
   with the scheduler. Otherwise the upgrade can submit them again.

Autosubmit 4.2 stores the workflow in a database and does not read the ``job_list.pkl`` file
written by <4.2. The workflow is regenerated from the YAML configuration, so an experiment created,
per example, with 4.1 must be created again after upgrading.

Upgrade procedure
-----------------

1. Stop the Autosubmit instance and make sure no job is still running or queued on the platform.
2. Install or load Autosubmit 4.2.
3. Update the start conditions in ``<EXPID>/conf/*.yml``. ``STATUS`` is now a set of exact
   statuses. See `Start conditions`_ below.
4. Register the new version and regenerate the workflow:

   .. code-block:: bash

       autosubmit create <EXPID> -f -v

5. Recover the job statuses from the platform and the experiment history. Run the dry-run first,
   then save the result:

   .. code-block:: bash

       autosubmit recovery <EXPID> --all -f
       autosubmit recovery <EXPID> -s --all -f

   Add ``--offline`` if the platform is not reachable and you only need the local history.

7. Resume the experiment:

   .. code-block:: bash

       autosubmit run <EXPID>

The old ``job_list.pkl`` and its backup are no longer used. Keep them for reference or archive them
with the rest of the experiment.

Start conditions
----------------

In <4.2, ``STATUS`` accepted a single status and the statuses formed an ordered scale. A parent
satisfied the edge when its status was at or after the configured one. The generic waiting-job path
also marked a job ready when all its parents were ``COMPLETED`` or ``SKIPPED``, even when the
dependency declared a different ``STATUS``.

Autosubmit 4.2 compares the parent status against an exact set. Add every status you want to
accept. ``SUCCEEDED`` expands to ``COMPLETED`` and ``SKIPPED``, and ``FINISHED`` expands to
``COMPLETED``, ``SKIPPED`` and ``FAILED``.

.. list-table::
    :widths: 35 65
    :header-rows: 1

    * - 4.1 configuration
      - 4.2 migration
    * - ``STATUS`` omitted
      - No change. The default is ``SUCCEEDED`` (``COMPLETED`` or ``SKIPPED``).
    * - ``STATUS: COMPLETED``
      - Changed. In 4.1 a ``SKIPPED`` parent also satisfied it. Use ``SUCCEEDED`` to keep that
        behavior. ``COMPLETED`` now means exactly ``COMPLETED``.
    * - ``STATUS: SKIPPED``
      - Changed. In 4.1 a ``COMPLETED`` or ``FAILED`` parent could also satisfy it. Use
        ``SKIPPED,SUCCEEDED`` for successful outcomes, or ``FINISHED`` to accept any outcome.
    * - ``STATUS: FAILED``
      - Changed. In 4.1 a ``COMPLETED`` parent also satisfied it. Use ``FINISHED`` to run after any
        outcome.
    * - ``STATUS: RUNNING``
      - Changed. In 4.1 a parent that moved past ``RUNNING`` also satisfied it. Use
        ``RUNNING,SUCCEEDED`` for successful endings, or ``RUNNING,FINISHED`` to accept any outcome.
    * - ``STATUS: QUEUING``, ``READY``, ``HELD``, etc.
      - Changed. In 4.1 the parent could move past the configured status. List every accepted
        status explicitly, for example ``QUEUING,RUNNING,FINISHED``.
    * - ``UNKNOWN``, ``SUSPENDED``
      - Not valid start conditions. Remove them from ``STATUS``.
    * - ``DEPENDENCIES: JOB?``
      - Removed. Use ``DEPENDENCIES: JOB`` with ``STATUS: FINISHED``.
    * - ``STATUS: X?``
      - Removed. Use ``STATUS: FINISHED`` when any outcome is acceptable, or an explicit list such
        as ``RUNNING,SUCCEEDED`` for intermediate statuses.
    * - ``FAIL_OK: true``, ``OPTIONAL: true``
      - Removed. Use an explicit ``STATUS`` list, or ``FINISHED`` when any outcome is acceptable.
        Enabled flags raise ``AutosubmitCritical``.
    * - ``FAIL_OK: false``, ``OPTIONAL: false``
      - No behavioural change. They are removed during configuration normalization.

If ``autosubmit create`` raises ``Invalid STATUS``, the message lists the accepted values. Fix the
configuration and run ``autosubmit create <EXPID> -f`` again.
