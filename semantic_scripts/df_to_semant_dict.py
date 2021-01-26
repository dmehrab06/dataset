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

def split_status(x):
    for i in range(0,len(x)):
        x[i] = x[i].split('#')
    return x

def get_semantic_df(df):
    #df['status'] = df['datetime']+"#"+df['fac_name']+"#"+df['POI_dist'].astype(str)+"#"+df['lat'].astype(str)+"#"+df['lng'].astype(str)+"#"+df['POI_lat'].astype(str)+"#"+df['POI_lon'].astype(str)
    df['status'] = df['datetime']+"#"+df['fac_name']+"#"+df['poi_name']+"#"+df['POI_dist'].astype(str)+"#"+df['lat'].astype(str)+"#"+df['lng'].astype(str)+"#"+df['POI_lat'].astype(str)+"#"+df['POI_lon'].astype(str)+"#"+df['Res_lat'].astype(str)+"#"+df['Res_lon'].astype(str)+"#"+df['Res_dist'].astype(str)
    df = df.sort_values(by=['uid','datetime'])
    semantic_df = df.groupby(['uid'])['status'].apply(lambda x: ';'.join(x)).reset_index()
    semantic_df['status'] = semantic_df['status'].apply(lambda x: x.split(';'))
    semantic_df['status'] = semantic_df['status'].apply(split_status)
    #semantic_df.head()
    return semantic_df

def get_semantic_dict(semantic_df):
    semantic_dict = pd.Series(semantic_df.status.values,index=semantic_df.uid).to_dict()
    return semantic_dict

def print_something_of_dict(semantic_dict):
    print({k: semantic_dict[k] for k in list(semantic_dict)[:5]})
    
month = sys.argv[1]
day = sys.argv[2]
file_part = sys.argv[3]

file_loc = '/project/biocomplexity/data/XMode/evaluation/US_data_semantic/'+month+'/'+day+'/raw_csv/'+file_part
out_file_loc = '/project/biocomplexity/data/XMode/evaluation/US_data_semantic/'+month+'/'+day+'/raw_dict/'+file_part

pivot_df = pd.read_csv(file_loc,dtype={'county_fips':str})
pivot_semantic_dict = get_semantic_dict(get_semantic_df(pivot_df))

ff = open(out_file_loc+"_dict.dict",'w')
ff.write(str(pivot_semantic_dict))
