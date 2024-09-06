#!/bin/bash

###############################################################################
#                   NODES_SLURM t000 EXPERIMENT
###############################################################################
#
#SBATCH --qos=gp_debug
#
#
#SBATCH -A whatever
#
#SBATCH --cpus-per-task=40
#SBATCH --ntasks-per-node=90
#SBATCH --nodes=1
#
#
#SBATCH -t 00:01:00
#SBATCH -J t000_NODES_SLURM
#SBATCH --output=/tmp/pytest-of-dbeltran/pytest-2/scheduler_tests1/scratch/whatever/dbeltran/t000/LOG_t000/t000_NODES_SLURM.cmd.out.0
#SBATCH --error=/tmp/pytest-of-dbeltran/pytest-2/scheduler_tests1/scratch/whatever/dbeltran/t000/LOG_t000/t000_NODES_SLURM.cmd.err.0

#
#
###############################################################################
###################
# Autosubmit header