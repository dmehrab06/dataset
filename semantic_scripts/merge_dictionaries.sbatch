#!/bin/bash
#SBATCH -N 1
#SBATCH -n 1
#SBATCH --job-name=merge_d
#SBATCH -t 01:00:00
#SBATCH --mem=250000
#SBATCH -p bii
#SBATCH -A nssac_covid19

source activate skmob

python merge_dictionaries.py $1 $2 $3 $4 $5
