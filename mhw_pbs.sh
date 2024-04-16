#!/bin/bash
#PBS -P v45
#PBS -q normal
#PBS -M john.reilly@utas.edu.au
#PBS -m abe
#PBS -l ncpus=16
#PBS -l mem=64GB
#PBS -l jobfs=10GB
#PBS -l walltime=10:00:00
#PBS -l software=python
#PBS -l wd
#PBS -l storage=gdata/v45+gdata/hh5+gdata/cj50+gdata/ua8
#PBS -j oe

# Load conda environment
module unload conda
module use /g/data/hh5/public/modules
module load conda/analysis3

# set filename of main code
filecode=mhw_pbs

# run python application
python3 -u $filecode.py > $PBS_JOBID.log

