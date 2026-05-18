###########
Performance
###########

Autosubmit includes built-in support for **CPMIP** (Computational Performance Model Intercomparison Project)
metrics, a set of metrics that can be used for the study of computational performance of climate (and Earth system) models.
These metrics provide a quick and comparable view of how efficiently a simulation runs on a given platform, making them a useful first indicator 
when investigating for inefficiencies — undersized partitions, oversubscribed nodes, I/O bottlenecks, or regressions after a code change.

CPMIP metrics target the **simulation job** of an experiment — the job that
advances model time chunk by chunk (commonly named ``SIM``). SYPD and CHSY are
defined in terms of simulated years, so they rely on the chunk calendar
(``CHUNK``, ``CHUNKSIZE``, ``CHUNKSIZEUNIT``) that only chunk-based simulation
jobs carry; CORE_HOURS can still be computed on any job.

CPMIP metrics are not computed automatically: today they are evaluated only
as part of the notification workflow. 
A job’s CPMIP metrics are computed only when both of the following conditions are met:

* ``MAIL.NOTIFICATIONS: True`` is set in the experiment configuration, and
* the job declares a ``CPMIP_THRESHOLDS`` block in ``jobs_<EXPID>.yml``.

Without that, the metric values are not recorded anywhere — there is no
silent computation in the background. See
:ref:`cpmip-notifications-config` for how to enable both.

Available CPMIP metrics
=======================

.. list-table::
   :header-rows: 1
   :widths: 18 50 32

   * - Metric
     - Definition
     - Formula
   * - **SYPD**
     - Simulated Years Per Day — how much model time the job advances per
       wall-clock day. Higher is better.
     - ``simulated_years * 24 / runtime_hours``
   * - **CHSY**
     - Core-Hours per Simulated Year — total CPU time consumed per year of
       simulation. Lower is better.
     - ``ncpus * runtime_hours / simulated_years``
   * - **CORE_HOURS**
     - Total core-hours billed by the run. Useful as a budget signal.
     - ``ncpus * runtime_hours``

Inputs come from the job stat file (``start_time``, ``end_time``) and the
experiment configuration (``PROCESSORS`` plus the chunk calendar). If a required
input is missing, the metric is silently skipped — for example, ``CORE_HOURS``
and ``CHSY`` are skipped when ``PROCESSORS`` is not set on the job.
