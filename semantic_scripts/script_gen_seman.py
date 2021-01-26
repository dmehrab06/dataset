import os
import sys

month = sys.argv[1]
day = sys.argv[2]
input_dir = '/project/biocomplexity/data/XMode/evaluation/US_data/'+month+'/'+day+'/'
out_dir = '/project/biocomplexity/data/XMode/evaluation/US_data_semantic/'+month+'/'+day+'/raw_csv/'

for file in os.listdir(input_dir):
    if file.endswith('csv'):
        if not os.path.isfile(out_dir+file):
            print('sbatch seman_traj_map_points.sbatch '+month+' '+day+' '+file)
