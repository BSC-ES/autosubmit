Workflow examples:
------------------

Example 1: How to select an specific chunk
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. WARNING::
   This example illustrates the old select_chunk.

.. runcmd:: mv ./userguide/defining_workflows_example/code/job_frequency.yml /home/docs/autosubmit/a000/conf/jobs_a000.yml
    :silent-output: 1
    :prompt:

.. runcmd:: mv -v ./userguide/defining_workflows_example/code/exp_startdate.yml /home/docs/autosubmit/a000/conf/expdef_a000.yml
    :silent-output: 1
    :prompt:

.. code-block:: yaml

    JOBS:
      SIM:
          FILE: templates/sim.tmpl.sh
          DEPENDENCIES: INI SIM-1 POST-1 CLEAN-5
              INI:
              SIM-1:
              POST-1:
                CHUNKS_FROM:
                  all:
                      chunks_to: 1
              CLEAN-5:
          RUNNING: chunk
          WALLCLOCK: 0:30
          PROCESSORS: 768


.. runcmd:: autosubmit monitor a000 --hide -o png
    :silent-output: 1
    :prompt:

.. runcmd:: find /home/docs/autosubmit/a001/plot/ -type f -iname "a000_*.png" -exec mv -- {} ./userguide/defining_workflows_example/fig/select_chunks.png \;
    :silent-output: 1
    :prompt:

.. figure:: fig/select_chunks.png
   :name: select_chunks
   :width: 100%
   :align: center
   :alt: select_chunks_workflow

Example 2: SKIPPABLE
~~~~~~~~~~~~~~~~~~~~

In this workflow you can see an illustrated example of ``SKIPPABLE`` parameter used in an dummy workflow.

.. code-block:: yaml

    EXPERIMENT:
      DATELIST: 19600101 19650101 19700101
      MEMBERS: fc0 fc1
      CHUNKSIZEUNIT: month
      SPLITSIZEUNIT: day
      CHUNKSIZE: 1
      SPLITSIZE: 1
      SPLITPOLICY: flexible
      NUMCHUNKS: 4
      CALENDAR: standard

    JOBS:
        SIM:
            FILE: sim.sh
            DEPENDENCIES: INI POST-1
            WALLCLOCK: 00:15
            RUNNING: chunk
            QUEUE: debug
            SKIPPABLE: TRUE

        POST:
            FILE: post.sh
            DEPENDENCIES: SIM
            WALLCLOCK: 00:05
            RUNNING: member
            #QUEUE: debug

.. figure:: fig/skip.png
   :name: skip
   :width: 100%
   :align: center
   :alt: skip_workflow

Example 3: Weak dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In this workflow you can see an illustrated example of weak dependencies.

Weak dependencies, work like this way:

* X job only has one parent. X job parent can have ``COMPLETED`` or ``FAILED`` as status for current job to run.
* X job has more than one parent. One of the X job parent must have ``COMPLETED`` as status while the rest can be  ``FAILED`` or ``COMPLETED``.

.. code-block:: yaml

    EXPERIMENT:
      DATELIST: 2021102412
      MEMBERS: MONARCH SILAM CAMS
      CHUNKSIZEUNIT: month
      SPLITSIZEUNIT: day
      CHUNKSIZE: 1
      SPLITSIZE: 1
      SPLITPOLICY: flexible
      NUMCHUNKS: 1
      CALENDAR: standard

    JOBS:
        GET_FILES:
            FILE: templates/fail.sh
            RUNNING: chunk

        IT:
            FILE: templates/work.sh
            RUNNING: chunk
            QUEUE: debug

        CALC_STATS:
            FILE: templates/work.sh
            DEPENDENCIES: IT GET_FILES ?
            RUNNING: chunk
            SYNCHRONIZE: member

.. figure:: fig/dashed.png
   :name: dashed
   :width: 100%
   :align: center
   :alt: dashed_workflow

Example 4: Select Member
~~~~~~~~~~~~~~~~~~~~~~~~

In this workflow you can see an illustrated example of select member. Using 4 members 1 datelist and 4 different job sections.

.. code-block:: yaml

    EXPERIMENT:
        DATELIST: 19600101
        MEMBERS: "00 01 02 03"
        CHUNKSIZE: 1
        NUMCHUNKS: 2

    JOBS:
        SIM:
            RUNNING: chunk
            QUEUE: debug

        DA:
            DEPENDENCIES:
                SIM:
                    members_from:
                        all:
                            members_to: 00,01,02
            RUNNING: chunk
            SYNCHRONIZE: member

        REDUCE:
            DEPENDENCIES: SIM
            RUNNING: member
            FREQUENCY: 4

        REDUCE_AN:
            FILE: templates/05b_sim.sh
            DEPENDENCIES: DA
            RUNNING: chunk
            SYNCHRONIZE: member

.. figure:: fig/select_members.png
   :name: select_members
   :width: 100%
   :align: center
   :alt: select_members

Loops definition
~~~~~~~~~~~~~~~~

You need to use the ``FOR`` and ``NAME`` keys to define a loop.

To generate the following jobs:

.. code-block:: yaml

    EXPERIMENT:
      DATELIST: 19600101
      MEMBERS: "00"
      CHUNKSIZEUNIT: day
      CHUNKSIZE: '1'
      NUMCHUNKS: '2'
      CALENDAR: standard

    JOBS:
      POST_20:
        DEPENDENCIES:
          POST_20:
          SIM_20:
        FILE: POST.sh
        PROCESSORS: '20'
        RUNNING: chunk
        THREADS: '1'
        WALLCLOCK: 00:05

      POST_40:
        DEPENDENCIES:
          POST_40:
          SIM_40:
        FILE: POST.sh
        PROCESSORS: '40'
        RUNNING: chunk
        THREADS: '1'
        WALLCLOCK: 00:05

      POST_80:
        DEPENDENCIES:
          POST_80:
          SIM_80:
        FILE: POST.sh
        PROCESSORS: '80'
        RUNNING: chunk
        THREADS: '1'
        WALLCLOCK: 00:05

      SIM_20:
        DEPENDENCIES:
          SIM_20-1:
        FILE: POST.sh
        PROCESSORS: '20'
        RUNNING: chunk
        THREADS: '1'
        WALLCLOCK: 00:05

      SIM_40:
        DEPENDENCIES:
          SIM_40-1:
        FILE: POST.sh
        PROCESSORS: '40'
        RUNNING: chunk
        THREADS: '1'
        WALLCLOCK: 00:05

      SIM_80:
        DEPENDENCIES:
          SIM_80-1:
        FILE: POST.sh
        PROCESSORS: '80'
        RUNNING: chunk
        THREADS: '1'
        WALLCLOCK: 00:05

One can use now the following configuration:

.. code-block:: yaml

    JOBS:
      SIM:
        FOR:
          NAME: [ 20,40,80 ]
          PROCESSORS: [ 20,40,80 ]
          THREADS: [ 1,1,1 ]
          DEPENDENCIES: [ SIM_20-1,SIM_40-1,SIM_80-1 ]
        FILE: POST.sh
        RUNNING: chunk
        WALLCLOCK: '00:05'

      POST:
          FOR:
            NAME: [ 20,40,80 ]
            PROCESSORS: [ 20,40,80 ]
            THREADS: [ 1,1,1 ]
            DEPENDENCIES: [ SIM_20 POST_20,SIM_40 POST_40,SIM_80 POST_80 ]
          FILE: POST.sh
          RUNNING: chunk
          WALLCLOCK: '00:05'


.. warning:: The mutable parameters must be inside the ``FOR`` key.

.. figure:: fig/for.png
   :name: for
   :width: 100%
   :align: center
   :alt: for
