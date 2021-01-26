#!/bin/bash
#SBATCH -N 1
#SBATCH -n 1
#SBATCH --job-name=df_to_dict
#SBATCH --cpus-per-task=10
#SBATCH -t 1:00:00
#SBATCH -p bii
#SBATCH -A nssac_covid19

source activate skmob

python df_to_semant_dict.py $1 $2 $3
