#!/bin/bash

###############################################################################
#                   BASE_SLURM t000 EXPERIMENT
###############################################################################
#
#SBATCH --qos=gp_debug
#
#
#SBATCH -A whatever
#
#SBATCH --cpus-per-task=1
#
#
#SBATCH -n 1
#
#SBATCH -t 00:01:00
#SBATCH -J t000_BASE_SLURM
#SBATCH --output=/tmp/pytest-of-dbeltran/pytest-0/scheduler_tests0/scratch/whatever/dbeltran/t000/LOG_t000/t000_BASE_SLURM.cmd.out.0
#SBATCH --error=/tmp/pytest-of-dbeltran/pytest-0/scheduler_tests0/scratch/whatever/dbeltran/t000/LOG_t000/t000_BASE_SLURM.cmd.err.0

#
#
###############################################################################
###################
# Autosubmit header
