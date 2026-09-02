:orphan:

Measuring the Autosubmit I/O footprint
======================================

This page describes how to measure the process, file and network I/O that
Autosubmit performs during a run. It reproduces the numbers shown in
:ref:`platform_connections` and can be pointed at any workflow, not just
the dummy one.

The measurement is not part of the test suite. It is a manual procedure a
user runs on their own machine when they want concrete numbers for their
own workflow shape, or when profiling the effect of a change to
Autosubmit itself.

Prerequisites
-------------

* ``autosubmit`` installed and on ``PATH``.
* Either ``bpftrace`` (preferred; low overhead, needs ``CAP_BPF`` or
  root) or ``strace`` (unprivileged, but slows the traced process
  substantially).

  * Debian/Ubuntu: ``sudo apt install bpftrace`` or ``sudo apt install strace``
  * RHEL/Fedora: ``sudo dnf install bpftrace`` or ``sudo dnf install strace``

* Passwordless SSH to the target host. ``localhost`` is sufficient for
  the ``local`` platform: add your public key to
  ``~/.ssh/authorized_keys`` and confirm ``ssh localhost hostname``
  returns without prompting.

Step 1 - Create an experiment
-----------------------------

.. code-block:: bash

    autosubmit expid -H local -d "io measurement" --dummy
    autosubmit create <expid>

The dummy workflow contains a handful of short jobs and exercises the
main-loop paths (submission, status polling, log retrieval) without
requiring a real HPC. To measure against a real HPC, replace ``local``
with the name of a Slurm, PJM or PBS platform from your configuration.

Step 2 - Run the measurement
----------------------------

.. code-block:: bash

    ./scripts/measure_platform_io.sh <expid>

The script picks ``bpftrace`` when it can run it and falls back to
``strace`` otherwise. Results are written under the experiment
directory, to ``<LOCAL_ROOT_DIR>/<expid>/tmp/measure_<timestamp>/``,
where ``LOCAL_ROOT_DIR`` is the Autosubmit root configured in
``.autosubmitrc`` (``~/autosubmit`` by default):

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - File
     - Contents
   * - ``strace.log``
     - The raw trace. Tens of megabytes is normal for a short workflow.
   * - ``autosubmit.stdout``
     - The workflow's own output, kept separate from the summary.
   * - ``summary.txt``
     - The metric table described below.

To re-summarise an existing trace without re-running the workflow:

.. code-block:: bash

    ./scripts/measure_platform_io.sh --summarise <path-to>/strace.log

Step 3 - Read the summary
-------------------------

.. code-block:: text

    ====================================================================
    Autosubmit platform I/O measurement
    ====================================================================
    Experiment : a004
    Timestamp  : 20260902_135720
    Trace file : /home/user/autosubmit/a004/tmp/measure_20260902_135720/strace.log (20M)

      Metric                                                       Count
      --------------------------------------------------------------------------------
      SSH connections opened                                       0 (no SSH platform in use)
      Subprocess launches (total)                                  437 across all binaries
      Job-execution invocations (bash + timeout + printenv)        bash 48, timeout 48, printenv 48 (~7 per job)
      Job-status check subprocesses (ps + grep)                    ps 40, grep 40
      Completion-marker sweeps (find)                              24
      Opens of local SQLite DBs                                    1,121 (682 job_list.db + 439 job_data_a004.db)
      Bytes written to disk                                        ~1 MB
      Bytes read from disk (includes subprocess-startup overhead)  ~67 MB
      Total syscalls                                               ~180,000
      --------------------------------------------------------------------------------

      Jobs detected in trace: 7

How to read each row:

* **SSH connections opened** - one entry per ``connect()`` to port 22. A
  Paramiko-backed platform opens two per platform at startup (the main
  session and the log-recovery subprocess) plus any reconnects. The
  ``local`` platform opens none, so a zero here is expected rather than
  an error.
* **Subprocess launches** - every external command Autosubmit runs.
  Dominated by shell wrappers on the ``local`` platform; on a Slurm
  platform the scheduler commands appear here instead.
* **Job-execution invocations** - each job launch appears as a
  ``printenv`` + ``timeout`` + ``bash`` triple, so these three counts
  should track each other. The per-job figure is derived from the number
  of distinct ``STAT`` files seen in the trace.
* **Job-status check subprocesses** - the ``local`` platform polls job
  state by shelling out to ``ps`` and filtering with ``grep``, once per
  status-check cycle per active job.
* **Completion-marker sweeps** - one ``find`` per platform per
  status-check cycle, looking for ``_COMPLETED`` markers.
* **Opens of local SQLite DBs** - Autosubmit persists state after every
  job transition, so this count scales with job count multiplied by the
  number of state changes per job rather than with job count alone.
* **Bytes written to disk** - the closest single figure to Autosubmit's
  real filesystem footprint.
* **Bytes read from disk** - inflated by subprocess startup. Every
  process launched loads libc, the linker cache and locale data, so this
  figure scales with subprocess count and overstates Autosubmit's own
  work.

Caveats
-------

The numbers are **workflow-specific** and **setup-specific**. They are
useful for:

* Establishing an order-of-magnitude baseline for a given workflow.
* Comparing two configurations against each other, for example with and
  without wrappers, or with different chunk counts.
* Confirming that a change to Autosubmit moved the numbers in the
  expected direction.

They are not:

* A general statement of what Autosubmit does per run. The dummy
  workflow is small and unrepresentative of operational workloads.
* A performance benchmark when collected under ``strace``, which imposes
  significant overhead. Use the ``bpftrace`` path for timing work.
* A substitute for measuring on the target HPC. Filesystem behaviour on
  GPFS, Lustre or BeeGFS differs from a local disk, and figures from a
  ``localhost`` run will not reflect that.