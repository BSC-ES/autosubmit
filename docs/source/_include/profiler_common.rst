.. note:: Remember that the purpose of this profiler is to measure the performance of Autosubmit, 
  not the jobs it runs.

This profiler uses Python's ``cProfile`` and ``psutil`` modules to generate a report with simple CPU and 
memory metrics which will be displayed in your console after the command finishes, as in the example below:

.. figure:: /_include/fig/profiler_output.png
   :name: profiler_head_output
   :align: center
   :alt: Screenshot of the header of the profiler's output

The profiler output is also saved in ``<EXPID>/tmp/profile``. There you will find two files, the
report in plain text format and a ``.prof`` binary which contains the CPU metrics. We highly recommend 
using `SnakeViz <https://jiffyclub.github.io/snakeviz/>`_ to visualize this file, as follows:

.. figure:: /_include/fig/profiler_snakeviz.png
   :name: profiler_snakeviz
   :align: center
   :alt: The .prof file represented by the graphical library SnakeViz

For more detailed documentation about the profiler, please visit this :ref:`page<advanced_profiling>`.