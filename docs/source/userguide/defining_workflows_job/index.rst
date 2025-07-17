Job frequency
~~~~~~~~~~~~~

Some times you just don't need a job to be run on every chunk or member. For example, you may want to launch the postprocessing
job after various chunks have completed. This behaviour can be achieved using the ``FREQUENCY`` attribute. You can specify
an integer I for this attribute and the job will run only once for each I iterations on the running level.

.. hint::
   You don't need to adjust the frequency to be a divisor of the total jobs. A job will always execute at the last
   iteration of its running level

.. runcmd:: cat ./userguide/defining_workflows_job/code/job_frequency.yml

.. runcmd:: cat ./userguide/defining_workflows_job/code/exp_frequency.yml

.. runcmd:: mv ./userguide/defining_workflows_job/code/job_frequency.yml /home/docs/autosubmit/a000/conf/jobs_a000.yml

.. runcmd:: mv ./userguide/defining_workflows_job/code/exp_frequency.yml /home/docs/autosubmit/a000/conf/expdef_a000.yml

.. runcmd:: cat /home/docs/autosubmit/a000/conf/jobs_a000.yml

.. runcmd:: cat /home/docs/autosubmit/a000/conf/expdef_a000.yml

.. code-block:: yaml

    JOBS:
      INI:
        FILE: ini.sh
        RUNNING: member

      SIM:
        FILE: sim.sh
        DEPENDENCIES: ini sim-1
        RUNNING: chunk

      POSTPROCESS:
        FILE: postprocess.sh
        DEPENDENCIES: sim
        RUNNING: chunk
        FREQUENCY: 3

      COMBINE:
        FILE: combine.sh
        DEPENDENCIES: postprocess
        RUNNING: member


.. runcmd:: autosubmit create a000 --hide -o png

.. runcmd:: find /home/docs/autosubmit/a000/plot/ -type f -iname "a000_*.png" -exec mv -- {} ./userguide/defining_workflows_job/fig/frequency.png \;

The resulting workflow can be seen in Figure :numref:`frequency`

.. figure:: fig/frequency.png
   :name: frequency
   :width: 100%
   :align: center
   :alt: simple workflow plot

   Example showing dependencies between jobs running at different frequencies.

Job synchronize
~~~~~~~~~~~~~~~

For jobs running at chunk level, and this job has dependencies, you could want
not to run a job for each experiment chunk, but to run once for all member/date dependencies, maintaining
the chunk granularity. In this cases you can use the ``SYNCHRONIZE`` job parameter to determine which kind
of synchronization do you want. See the below examples with and without this parameter.

.. hint::
   This job parameter works with jobs with ``RUNNING`` parameter equals to 'chunk'.


.. runcmd:: mv ./userguide/defining_workflows_job/code/job_no_synchronize.yml /home/docs/autosubmit/a000/conf/jobs_a000.yml

.. runcmd:: mv ./userguide/defining_workflows_job/code/exp_no_synchronize.yml /home/docs/autosubmit/a000/conf/expdef_a000.yml

.. code-block:: yaml

    EXPERIMENT:
      DATELIST: 20000101 20010101
      MEMBERS: Member1 Member2
      CHUNKSIZEUNIT: month
      CHUNKSIZE: 1
      NUMCHUNKS: 3
      CHUNKINI: ''
      CALENDAR: standard

    JOBS:
      INI:
        FILE: ini.sh
        RUNNING: member

      SIM:
        FILE: sim.sh
        DEPENDENCIES: INI SIM-1
        RUNNING: chunk

      ASIM:
        FILE: asim.sh
        DEPENDENCIES: SIM
        RUNNING: chunk


.. runcmd:: autosubmit monitor a000 --hide -o png

.. runcmd:: find /home/docs/autosubmit/a000/plot/ -type f -iname "a000_*.png" -exec mv -- {} ./userguide/defining_workflows_job/fig/no-synchronize.png \;

The resulting workflow can be seen in Figure :numref:`nosync`

.. figure:: fig/no-synchronize.png
   :name: nosync
   :width: 100%
   :align: center
   :alt: simple workflow plot

   Example showing dependencies between chunk jobs running without synchronize.

.. runcmd:: mv -v ./userguide/defining_workflows_job/code/exp_synchronize.yml /home/docs/autosubmit/a000/conf/expdef_a000.yml

.. code-block:: yaml

    ASIM:
      FILE: asim.sh
      DEPENDENCIES: SIM
      RUNNING: chunk
      SYNCHRONIZE: member


.. runcmd:: autosubmit monitor a000 --hide -o png

.. runcmd:: find /home/docs/autosubmit/a000/plot/ -type f -iname "a000_*.png" -exec mv -- {} ./userguide/defining_workflows_job/fig/member-synchronize.png \;

The resulting workflow of setting ``SYNCHRONIZE`` parameter to 'member' can be seen in Figure :numref:`msynchronize`

.. figure:: fig/member-synchronize.png
   :name: msynchronize
   :width: 100%
   :align: center
   :alt: simple workflow plot

   Example showing dependencies between chunk jobs running with member synchronize.

.. code-block:: yaml

    ASIM:
        FILE: asim.sh
        DEPENDENCIES: SIM
        RUNNING: chunk
        SYNCHRONIZE: date

The resulting workflow of setting ``SYNCHRONIZE`` parameter to 'date' can be seen in Figure :numref:`dsynchronize`

.. figure:: fig/date-synchronize.png
   :name: dsynchronize
   :width: 100%
   :align: center
   :alt: simple workflow plot

   Example showing dependencies between chunk jobs running with date synchronize.

Job split
~~~~~~~~~

For jobs running at any level, it may be useful to split each task into different parts.
This behaviour can be achieved using the ``SPLITS`` attribute to specify the number of parts.

It is also possible to specify the splits for each task using the ``SPLITS_FROM`` and ``SPLITS_TO`` attributes.

There is also an special character '*' that can be used to specify that the split is 1-to-1 dependency. In order to use this character, you have to specify both SPLITS_FROM and SPLITS_TO attributes.

.. code-block:: yaml

    JOBS:
      ini:
          FILE: ini.sh
          RUNNING: once

      sim:
          FILE: sim.sh
          DEPENDENCIES: ini sim-1
          RUNNING: once

      asim:
          FILE: asim.sh
          DEPENDENCIES: sim
          RUNNING: once
          SPLITS: 3

      post:
          FILE: post.sh
          RUNNING: once
          DEPENDENCIES:
              asim:
                  SPLITS_FROM:
                      2,3: # [2:3] is also valid
                          splits_to: 1,2*,3* # 1,[2:3]* is also valid, you can also specify the step with [2:3:step]
          SPLITS: 3

In this example:

Post job will be split into 2 parts.
Each part will depend on the 1st part of the asim job.
The 2nd part of the post job will depend on the 2nd part of the asim job.
The 3rd part of the post job will depend on the 3rd part of the asim job.

.. figure:: fig/splits_job.png
   :name: splits_job
   :width: 100%
   :align: center
   :alt: splits_job

Example 1: 1-to-1 dependency

.. code-block:: yaml

  EXPERIMENT:
    DATELIST: 19600101
    MEMBERS: "00"
    CHUNKSIZEUNIT: day
    CHUNKSIZE: '1'
    NUMCHUNKS: '2'
    CALENDAR: standard

  JOBS:
    TEST:
      FILE: TEST.sh
      RUNNING: chunk
      SPLITS: 1
      WALLCLOCK: 00:30
    TEST2:
      FILE: TEST2.sh
      DEPENDENCIES:
        TEST:
          SPLITS_FROM:
            all:
              SPLITS_TO: '[1:auto]*\1'
      RUNNING: chunk
      SPLITS: 1
      WALLCLOCK: 00:30


.. figure:: fig/splits_1_to_1.png
   :name: split_1_to_1
   :width: 100%
   :align: center
   :alt: 1-to-1

Example 2: N-to-1 dependency

.. code-block:: yaml

  JOBS:
    TEST:
      FILE: TEST.sh
      RUNNING: once
      SPLITS: '4'
    TEST2:
      FILE: TEST2.sh
      DEPENDENCIES:
        TEST:
          SPLITS_FROM:
            "[1:2]":
              SPLITS_TO: "[1:4]*\\2"
      RUNNING: once
      SPLITS: '2'

.. figure:: fig/splits_n_to_1.png
   :name: N_to_1
   :width: 100%
   :align: center
   :alt: N_to_1

Example 3: 1-to-N dependency

.. code-block:: yaml

  JOBS:
    TEST:
      FILE: TEST.sh
      RUNNING: once
      SPLITS: '2'
    TEST2:
      FILE: TEST2.sh
      DEPENDENCIES:
        TEST:
          SPLITS_FROM:
            "[1:4]":
              SPLITS_TO: "[1:2]*\\2"
      RUNNING: once
      SPLITS: '4'

.. figure:: fig/splits_1_to_n.png
   :name: 1_to_N
   :width: 100%
   :align: center
   :alt: 1_to_N

Job Splits with calendar
~~~~~~~~~~~~~~~~~~~~~~~~

For jobs running at any level, it may be useful to split each task into different parts based on the calendar.
This behaviour can be achieved setting the ``SPLITS: auto`` and using the ``%EXPERIMENT.SPLITSIZE%`` and ``%EXPERIMENT.SPLITSIZEUNIT%`` variables.

Example4: Auto split

.. code-block:: yaml

    EXPERIMENT:
      DATELIST: 19900101
      MEMBERS: fc0
      CHUNKSIZEUNIT: day
      SPLITSIZEUNIT: day
      CHUNKSIZE: 3
      SPLITSIZE: 15
      SPLITPOLICY: flexible
      NUMCHUNKS: 2
      CALENDAR: standard

    JOBS:
      APP:
        FILE: app.sh
        FOR:
          DEPENDENCIES:
          - APP_ENERGY_ONSHORE:
              SPLITS_FROM:
                all:
                  SPLITS_TO: previous
            OPA_ENERGY_ONSHORE_1:
              SPLITS_FROM:
                all:
                  SPLITS_TO: all
            OPA_ENERGY_ONSHORE_2:
              SPLITS_FROM:
                all:
                  SPLITS_TO: all
          NAME: '%RUN.APP_NAMES%'
          SPLITS: '1'
        PLATFORM: 'local'
        RUNNING: chunk
        WALLCLOCK: 00:05
      DN:
        DEPENDENCIES:
          APP_ENERGY_ONSHORE-1:
            SPLITS_TO: '1'
          DN:
            SPLITS_FROM:
              all:
                SPLITS_TO: previous
        FILE: dn.sh
        PLATFORM: 'local'
        RUNNING: chunk
        SPLITS: auto
        WALLCLOCK: 00:05
      OPA:
        CHECK: on_submission
        FILE: opa.sh
        FOR:
          DEPENDENCIES:
          - DN:
              SPLITS_FROM:
                all:
                  SPLITS_TO: "[1:%JOBS.DN.SPLITS%]*\\1"
            OPA_ENERGY_ONSHORE_1:
              SPLITS_FROM:
                all:
                  SPLITS_TO: previous
          - DN:
              SPLITS_FROM:
                all:
                  SPLITS_TO: "[1:%JOBS.DN.SPLITS%]*\\1"
            OPA_ENERGY_ONSHORE_2:
              SPLITS_FROM:
                all:
                  SPLITS_TO: previous
          NAME: '%RUN.OPA_NAMES%'
          SPLITS: '[auto, auto]'
        PLATFORM: 'local'
        RUNNING: chunk
        WALLCLOCK: 00:05
    RUN:
      APP_NAMES:
      - ENERGY_ONSHORE
      OPA_NAMES:
      - energy_onshore_1
      - energy_onshore_2



.. figure:: fig/splits_auto.png
   :name: auto
   :width: 100%
   :align: center
   :alt: auto

Job delay
~~~~~~~~~

Some times you need a job to be run after a certain number of chunks. For example, you may want to launch the asim
job after various chunks have completed. This behaviour can be achieved using the ``DELAY`` attribute. You can specify
an integer N for this attribute and the job will run only after N chunks.

.. hint::
   This job parameter works with jobs with RUNNING parameter equals to 'chunk'.

.. code-block:: yaml

    EXPERIMENT:
      DATELIST: 20000101 20010101
      MEMBERS: fc0
      CHUNKSIZEUNIT: month
      SPLITSIZEUNIT: day
      CHUNKSIZE: 1
      SPLITSIZE: 1
      SPLITPOLICY: flexible
      NUMCHUNKS: 4
      CALENDAR: standard

    JOBS:
      INI:
          FILE: ini.sh
          RUNNING: member

      SIM:
          FILE: sim.sh
          DEPENDENCIES: ini sim-1
          RUNNING: chunk

      ASIM:
          FILE: asim.sh
          DEPENDENCIES: sim asim-1
          RUNNING: chunk
          DELAY:  2

      POST:
          FILE:  post.sh
          DEPENDENCIES:  sim asim
          RUNNING:  chunk

The resulting workflow can be seen in Figure :numref:`delay`

.. figure:: fig/experiment_delay_doc.png
   :name: delay
   :width: 100%
   :align: center
   :alt: simple workflow with delay option

   Example showing the asim job starting only from chunk 3.
