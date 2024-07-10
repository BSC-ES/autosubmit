#!/bin/bash

###############################################################################
#                   BASE_PJM t000 EXPERIMENT
###############################################################################
#
#PJM -N t000_BASE_PJM
#PJM -L elapse=00:01:00
#PJM -L rscgrp=dummy
#PJM -g whatever
#

export OMP_NUM_THREADS=1
#
#

#PJM -o /tmp/pytest-of-dbeltran/pytest-0/scheduler_tests0/scratch/whatever/dbeltran/t000/LOG_t000/t000_BASE_PJM.cmd.out.0
#PJM -e /tmp/pytest-of-dbeltran/pytest-0/scheduler_tests0/scratch/whatever/dbeltran/t000/LOG_t000/t000_BASE_PJM.cmd.err.0
#
#
###############################################################################
###################
# Autosubmit header