#!/bin/bash
#SBATCH -N 1
#SBATCH -n 1
#SBATCH --job-name=get_seman_traj
#SBATCH --cpus-per-task=20
#SBATCH --mem=200000
#SBATCH -t 01:00:00
#SBATCH -p bii
#SBATCH -A nssac_covid19

source activate skmob

python seman_traj_map_points.py $1 $2 $3
