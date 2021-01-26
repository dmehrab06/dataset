import os
import sys

month = sys.argv[1]
day = sys.argv[2]
input_dir = '/project/biocomplexity/data/XMode/evaluation/US_data_semantic/'+month+'/'+day+'/raw_csv/'
out_dir = '/project/biocomplexity/data/XMode/evaluation/US_data_semantic/'+month+'/'+day+'/raw_dict/'

for file in os.listdir(input_dir):
    if file.endswith('csv'):
        if not os.path.isfile(out_dir+file+"_dict.dict"):
            print('sbatch df_to_semant_dict.sbatch '+month+' '+day+' '+file)
