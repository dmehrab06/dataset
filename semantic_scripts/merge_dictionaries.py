import os
import pandas as pd
from datetime import datetime
import argparse
import logging
import glob
import csv
import multiprocessing
import geopandas as gpd
import skmob
from skmob.preprocessing import detection
from scipy.spatial import distance
import numpy as np
import warnings
import sys
warnings.filterwarnings("ignore")
import logging

#{key:value}
#{user:list of t_lists}
#t_list = [time,poi_type,lat,lon]

def merge_seman_traj(traj1,traj2):
    new_traj = []
    traj1_it = 0
    traj2_it = 0
    while (traj1_it<len(traj1)) and (traj2_it<len(traj2)):
        if traj1[traj1_it][0]<traj2[traj2_it][0]:
            new_traj.append(traj1[traj1_it])
            traj1_it = traj1_it+1
        else:
            new_traj.append(traj2[traj2_it])
            traj2_it = traj2_it+1
            
    while(traj1_it<len(traj1)):
        new_traj.append(traj1[traj1_it])
        traj1_it = traj1_it + 1
        
    while(traj2_it<len(traj2)):
        new_traj.append(traj2[traj2_it])
        traj2_it = traj2_it + 1
    return new_traj

def merge_user_dict(dict1,dict2):
    
    for user in dict2:
        if user in dict1:
            traj1 = dict1[user]
            traj2 = dict2[user]
            
            new_traj = merge_seman_traj(traj1,traj2)
            
            dict1[user] = new_traj
        else:
            dict1[user] = dict2[user]
    
    return dict1

import sys

#take month and day as input

dict1_file = sys.argv[1] #part-00196-80523996-d464-4425-9991-0b2462a666b0-c000.csv.af.csv_dict.dict
dict2_file = sys.argv[2] #part-00197-80523996-d464-4425-9991-0b2462a666b0-c000.csv.af.csv_dict.dict
out_file = sys.argv[3] #test_output.dict
month = sys.argv[4]
day = sys.argv[5]
in_dir = '/project/biocomplexity/data/XMode/evaluation/US_data_semantic/'+str(month)+'/'+str(day)+'/raw_dict/'
out_dir = '/project/biocomplexity/data/XMode/evaluation/US_data_semantic/'+str(month)+'/'+str(day)+'/processed_dict/'

# dummy_dict={'dummy':[['nowhere'],['nowhere']]}
# ff = open(out_dir+out_file,'w')
# ff.write(str(dummy_dict))

if not os.path.isdir(out_dir):
    os.makedirs(out_dir)

def get_dict_from_file(file_loc):
    
    if os.path.isfile(in_dir+file_loc):
        f = open(in_dir+file_loc,'r')
        try:
            my_dict = eval(f.read())
            return my_dict
        except:
            print('something happened')
    else:
        f = open(out_dir+file_loc,'r')
        try:
            my_dict = eval(f.read())
            return my_dict
        except:
            print('something happened')

def remove_file(file_loc):
    if os.path.isfile(out_dir+file_loc):
        os.remove(out_dir+file_loc)

starttime = datetime.now()
d1 = get_dict_from_file(dict1_file)
d2 = get_dict_from_file(dict2_file)

new_dict = merge_user_dict(d1,d2)

ff = open(out_dir+out_file,'w')
ff.write(str(new_dict))
endtime = datetime.now()

print('runtime: '+str(endtime-starttime))

remove_file(dict1_file)
remove_file(dict2_file)
