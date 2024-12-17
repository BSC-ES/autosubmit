#!/bin/bash

###############################################################################
#                   NODES_PJM t000 EXPERIMENT
###############################################################################
#
#PJM -N t000_NODES_PJM
#PJM -L elapse=00:01:00
#PJM -L rscgrp=dummy
#PJM -g whatever
#

#
#PJM -L node=1
#PJM -o /tmp/pytest-of-dbeltran/pytest-2/scheduler_tests1/scratch/whatever/dbeltran/t000/LOG_t000/t000_NODES_PJM.cmd.out.0
#PJM -e /tmp/pytest-of-dbeltran/pytest-2/scheduler_tests1/scratch/whatever/dbeltran/t000/LOG_t000/t000_NODES_PJM.cmd.err.0
#
export OMP_NUM_THREADS=40
###############################################################################
###################
# Autosubmit header
