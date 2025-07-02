:orphan:

..
   The :orphan: section tells Sphinx not to include this page in any contents list

.. _advanced_profiling:

autosubmit.profiler
===================

.. important:: If you only want to use the profiler built into the Autosubmit commands, simpler 
      user-oriented guides are available for :ref:`run<run_profiling>`, 
      :ref:`create<create_profiling>` and :ref:`monitor<monitor_profiling>`.

######################################
The Autosubmit's profiler
######################################

Autosubmit integrates a profiler that allows developers to easily measure the performance of entire 
functions or specific code fragments.

The profiler generates a comprehensive report with enough information to detect bottleneck functions 
during the execution of experiments, as well as information about the total memory consumed.

It mainly uses the ``cProfile`` library to make the report, so this module inherits its deterministic 
profiling and reasonable overhead features. However, it also limits profiling to a single thread, so 
please, do not use it on concurrent code. For memory profiling, it uses ``psutil``.

.. caution::
      This profiler was originally designed to be used in the ``autosubmit run`` command, so using 
      it in other functions may produce unexpected results or errors. Now, its usage have been 
      extended to ``autosubmit create`` and ``monitor``.
      
      The profiler instantiation requires an ``<EXPID>``, and not all the functions in Autosubmit use it.
      This can be bypassed using another string, but keep in mind that there is no error handling in
      this case.

How to profile a function or a specific code fragment?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Depending on the Autosubmit function you want to profile, you must add a ``--profile`` argument to the
parser. The ``autosubmit run``, ``create`` and ``monitor`` subcommands already support it. It is
recommended that the default value of this flag always be ``False``, to ensure that the profiler does
not interfere with the normal execution in an unwanted way. You will need to add something like this to
your parser:

.. code-block:: python

      subparser.add_argument('-p', '--profile', action='store_true', default=False, required=False,
      help='Prints performance parameters of the execution of this experiment.')

The function must receive the flag as argument to control the execution of the profiler. If the flag
has value ``True``, you should proceed as follows:

1. Instantiate a **Profiler(EXPID)** object. Specifying the ``<EXPID>`` is mandatory.

2. Run the profiler by calling the **start()** function of the instantiated object, at the beginning
   of the function or code fragment you want to evaluate. The measurement will start instantly.

3. Execute the **stop()** function of the profiler at the end of the function or code fragment to be
   evaluated. The process of taking measurements will stop instantly. The report will be generated
   automatically and the files will be stored in the ``<EXPID>/tmp/profile`` directory.

.. important:: Make sure, if necessary, that the call to `stop()` is always made, even if the
      Autosubmit code fails, in order to get the performance report.


The most relevant functions of the profiler, in detail
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. digraph:: foo
   :name: status_diagram
   :align: center
   :alt: Status diagram of the profiler

   bgcolor="transparent"
   graph [nodesep=0.5]

   // Extreme nodes (start/end)
   node [label="",color="black",style=filled,fixedsize=true];

   node [shape=circle,height=0.25,width=0.25] ENTRY;
   node [shape=doublecircle,height=0.2,width=0.2] EXIT;

   // Status nodes
   node [shape=rect,style=rounded,height=0.5,width=1.2,fixedsize=true,fontsize=12,fontname=arial];

   node [label="__init__"] init;
   node [label="start"] start;
   node [label="stop"] stop;
   node [label="report"] report;

   // Relations
   ENTRY -> init;
   init -> start [label="when\nstarted = False",fontsize=7,fontname=arial];
   start -> stop [label="     when\n     started = True",fontsize=7,fontname=arial];
   stop -> report [label="automatically",fontsize=7,fontname=arial];
   report -> EXIT;

   { rank = same; ENTRY init start }
   { rank = same; stop report EXIT }

* The **start()** function: Starts taking measures, both of execution times thanks to ``cProfile``, and
  memory thanks to ``psutil``. It also manages errors to avoid illegal transitions between states.

* The **stop()** function: Same as the previous function, but terminating the taking of measurements.
  It will call the report function automatically.

* The **_report()** function: It is private, and its purpose is to generate the final performance
  report and storing it properly. It will print the report to the console output and log it at the same time.
  In addition, it will generate two files in the directory chosen when instantiating the Profiler
  object, a ``.txt`` file with the same report shown on screen, and a ``.prof`` file with the report
  generated by ``pstats``. The ``.prof`` file can be manipulated with the appropriate tools. Our
  recommendation is to open it with `SnakeViz <https://jiffyclub.github.io/snakeviz/>`_, a graphical
  library that will interpret the data for you and display it in an interactive web interface.