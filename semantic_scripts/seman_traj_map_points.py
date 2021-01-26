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

from datetime import datetime

starttime = datetime.now()


#%%time
def haversine(lon1, lat1, lon2, lat2):
    KM = 6372.8 #Radius of earth in km instead of miles
    lat1, lon1, lat2, lon2 = map(np.deg2rad, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    total_km = KM * c
    return total_km

#%%time
#given file should have all the county names that we want to process
def process_county_info():
    
    #df = pd.read_csv('/home/zm8bh/Mobility_Codes/seperated codes/code/extract_other_counties/info_about_counties_all.csv',dtype={'fips':str})
    df = pd.read_csv('/project/biocomplexity/data/XMode/evaluation/semantic_extraction_scripts/info_about_counties_all.csv',dtype={'fips':str})
    df['fips'] = df['fips'].apply(lambda x: x.zfill(5))
    #print(df.head())
    return df

#this is supposed to work like a global input for every function
info_df = process_county_info()
#info_df.head()

#%%time
#read the largest chunk from the input part
month = sys.argv[1] #this will be passed as an argument
day = sys.argv[2] #this will be passed as an argument
file_part = sys.argv[3] #this will be passed as an argument
out_dir_csv = '/project/biocomplexity/data/XMode/evaluation/US_data_semantic/'+month+'/'+day+'/raw_csv/'
out_dir_dict = '/project/biocomplexity/data/XMode/evaluation/US_data_semantic/'+month+'/'+day+'/raw_dict/'
partname = "/project/biocomplexity/data/XMode/evaluation/US_data/"+month+"/"+day+"/"+file_part

# Configure the logger
# loggerConfigFileName: The name and path of your configuration file
#logging.config.fileConfig(path.normpath('seman_traj_map_points.log'))
logging.basicConfig(filename='log_files/seman_'+month+'_'+day+'_'+file_part[0:-4]+'.log', filemode='w', format='%(message)s', level=logging.INFO)
# Create the logger
# Admin_Client: The name of a logger defined in the config file
mylogger = logging.getLogger('Admin_Client')
mylogger.info('hello')


col_names = ['advertiser_id','platform','location_at','latitude','longitude','altitude','horizontal_accuracy','vertical_accuracy','heading','speed','ipv4','ipv6','final_country','user_agent','background','publisher_id','wifi_ssid','wifi_bssid','tech_signals','carrier','model']

part_df = pd.read_csv(partname, delimiter = ",", names = col_names, quotechar = '"', error_bad_lines = False, warn_bad_lines = True, low_memory=False)
part_df = part_df.drop(columns = ['heading','ipv4','ipv6','background','publisher_id','wifi_ssid','wifi_bssid','tech_signals','carrier','final_country'])

updated_col_names = part_df.columns.tolist()

#part_df.head()

#%%time
#supposed to work like a global input
whole_shape_file = "/project/biocomplexity/data/XMode/evaluation/zm8bh/Mobility_Codes/seperated codes/code/current_important_notebooks/_Notebooks/shapefiles/tl_2016_us_county.shp"
whole_shape_gdf = gpd.read_file(whole_shape_file)
whole_shape_gdf.shape

#%%time
# this function finds all the stop point and maps POI and residence for a specific county
# this function needs to be concurrent for different counties, they will all work on same input file but produce output differently
def process_for_county(curfips):
    #POI sanity check
    POI_location = '/project/biocomplexity/data/XMode/evaluation/POI_directory/'
    if not os.path.isfile(POI_location+curfips+'_POIS.csv'):
        empty_df = pd.DataFrame(columns=['uid'])
        mylogger.info("no nearby POI will be found in this part for "+str(curfips))
        return empty_df
    #Res sanity check
    Res_location = '/project/biocomplexity/data/XMode/evaluation/Residence_data/'
    if not os.path.isfile(Res_location+curfips+'_Res.csv'):
        empty_df = pd.DataFrame(columns=['uid'])
        mylogger.info("no nearby Res will be found in this part for "+str(curfips))
        return empty_df
    
    func_start = datetime.now()
    fips_df = info_df[info_df.fips==curfips]
    cur_min_lat,cur_max_lat,cur_min_lng,cur_max_lng = fips_df.iloc[0]['min_lat'],fips_df.iloc[0]['max_lat'],fips_df.iloc[0]['min_lng'],fips_df.iloc[0]['max_lng']
    #initial filtering based on bounding box, works very fast
    county_df = part_df[part_df['latitude'].ge(cur_min_lat) & part_df['latitude'].le(cur_max_lat) & part_df['longitude'].ge(cur_min_lng) & part_df['longitude'].le(cur_max_lng)]
    
    county_code = (str(curfips).zfill(5))[2:]
    county_prefix = (str(curfips).zfill(5))[0:2]
    #print(county_code)
    
    state_shape_gdf = whole_shape_gdf[(whole_shape_gdf['STATEFP']==county_prefix)]
    #state_shape_gdf = whole_shape_gdf
    county_shape_gdf = state_shape_gdf[(state_shape_gdf['COUNTYFP']==county_code)]
    
    #joining with shape file to get more accurate measurement, works slower, that is why initial filtering is done.
    if(len(county_df) > 0):
        
        county_df = gpd.GeoDataFrame(county_df, geometry = gpd.points_from_xy(county_df.longitude, county_df.latitude))

        county_df.crs = county_shape_gdf.crs
        county_gdf = gpd.sjoin(county_df, county_shape_gdf, how='inner')
        county_gdf = county_gdf[updated_col_names].copy(deep=True)
        spatial_join_time = datetime.now()
        mylogger.info("county pings separated for "+str(curfips)+", time taken: "+str(spatial_join_time-func_start))
        
    else:
        empty_df = pd.DataFrame(columns=['uid'])
        mylogger.info("no nearby pings found in this part for "+str(curfips))
        return empty_df
    
    #stop point detection
    county_gdf['location_at'] = pd.to_datetime(county_gdf['location_at'],unit='s')
    try:
        traj_df = skmob.TrajDataFrame(county_gdf, latitude = 'latitude', longitude = 'longitude', user_id = 'advertiser_id', datetime = 'location_at')
        county_stop_df = detection.stops(traj_df, stop_radius_factor=0.05, minutes_for_a_stop=5.0, spatial_radius_km=0.2, leaving_time=True)
        county_stop_df['county_fips'] = curfips
        stop_detect_time = datetime.now()
        mylogger.info("stop point detection complete for "+str(curfips)+", time taken: "+str(stop_detect_time-spatial_join_time))
    except:
        empty_df = pd.DataFrame(columns=['uid'])
        mylogger.info('stop detection failed for '+curfips)
        return empty_df
    
    #associate each point with its closest POI information
    #load POI data for corresponding county
    
    POI_df = pd.read_csv(POI_location+curfips+'_POIS.csv')
    POI_df = POI_df[['source_id','poi_name','st_name','st_num','designation','lat','lon','fac_name']]
    POI_df = POI_df.rename(columns={'lat':'POI_lat','lon':'POI_lon'})
    #find index of closest POI for each ping
    POI_dist_np = distance.cdist(county_stop_df[['lat','lng']], POI_df[['POI_lat','POI_lon']], metric='euclidean')
    POI_dist_df = pd.DataFrame(POI_dist_np,index=county_stop_df['uid'], columns=POI_df['source_id'].tolist())
    county_stop_df['POI_id'] = [i[i.astype(bool)][0] for i in np.where(POI_dist_df.values==POI_dist_df.min(axis=1)[:,None], POI_dist_df.columns, False)]
    #join with POI data based on previously found index, also calculate actual distance
    county_POI_df = county_stop_df.merge(POI_df, how='inner', left_on='POI_id', right_on='source_id')
    county_POI_df = county_POI_df.drop(columns=['source_id'])
    county_POI_df["POI_dist"] = haversine(county_POI_df["lng"], county_POI_df["lat"], county_POI_df["POI_lon"], county_POI_df["POI_lat"])
    POI_time = datetime.now()
    mylogger.info("pings matched with nearby POIs "+str(curfips)+", time taken: "+str(POI_time-stop_detect_time))
    
    try:
        Res_df = pd.read_csv(Res_location+curfips+'_Res.csv')
        #hack
        if Res_df.shape[0]>500000:
            Res_df = Res_df.sample(500000)
        if Res_df.shape[0]<2:
            return county_POI_df
        
        Res_df = Res_df[['blockgroup_id','urban_rural','lat','lon']]
        Res_df = Res_df.rename(columns={'lat':'Res_lat','lon':'Res_lon'})
        #find index of closest POI for each ping
        Res_dist_np = distance.cdist(county_stop_df[['lat','lng']], Res_df[['Res_lat','Res_lon']], metric='euclidean')
        Res_dist_df = pd.DataFrame(Res_dist_np,index=county_stop_df['uid'], columns=Res_df['blockgroup_id'].tolist())
        county_stop_df['Res_id'] = [i[i.astype(bool)][0] for i in np.where(Res_dist_df.values==Res_dist_df.min(axis=1)[:,None], Res_dist_df.columns, False)]
        #join with POI data based on previously found index, also calculate actual distance
        county_Res_df = county_stop_df.merge(Res_df, how='inner', left_on='Res_id', right_on='blockgroup_id')
        county_Res_df = county_Res_df.drop(columns=['blockgroup_id'])
        county_Res_df["Res_dist"] = haversine(county_Res_df["lng"], county_Res_df["lat"], county_Res_df["Res_lon"], county_Res_df["Res_lat"])
        Res_time = datetime.now()
        mylogger.info("pings matched with nearby Residences "+str(curfips)+", time taken: "+str(Res_time-stop_detect_time))
    except Exception as e:
        mylogger.info('something wrong happened while matching with '+str(curfips)+' Residences')
        mylogger.info(e)
        return county_POI_df
    
    county_POI_df['Res_dist'] = county_Res_df['Res_dist']
    county_POI_df['Res_lon'] =  county_Res_df['Res_lon']
    county_POI_df['Res_lat'] =  county_Res_df['Res_lat']
    county_POI_df['urban_rural'] =  county_Res_df['urban_rural']
    return county_POI_df
    #county_POI_df.to_csv(out_dir+file_part[0:-4]+"_"+curfips+".csv")#return county_POI_df

# fips = '15003' #this is just for testing purpose for checking the function working
# county_POI_df = process_for_county(fips)
# print(county_POI_df.shape)
# county_POI_df.sample(20)

def split_status(x):
    for i in range(0,len(x)):
        x[i] = x[i].split('#')
    return x

def get_semantic_df(df):
    df['status'] = df['datetime']+"#"+df['fac_name']+"#"+df['POI_dist'].astype(str)+"#"+df['lat'].astype(str)+"#"+df['lng'].astype(str)+"#"+df['POI_lat'].astype(str)+"#"+df['POI_lon'].astype(str)+"#"+df['Res_lat'].astype(str)+"#"+df['Res_lon'].astype(str)+"#"+df['Res_dist'].astype(str)
    df = df.sort_values(by=['uid','datetime'])
    semantic_df = df.groupby(['uid'])['status'].apply(lambda x: ';'.join(x)).reset_index()
    semantic_df['status'] = semantic_df['status'].apply(lambda x: x.split(';'))
    semantic_df['status'] = semantic_df['status'].apply(split_status)
    #semantic_df.head()
    return semantic_df

def get_semantic_dict(semantic_df):
    semantic_dict = pd.Series(semantic_df.status.values,index=semantic_df.uid).to_dict()
    return semantic_dict

#%%time
from multiprocessing import Pool 
#trying with parallelization    
county_list = info_df['fips'].tolist()
#print(county_list)
N = 40
pool = Pool(processes = N)
df_list = pool.map(process_for_county, county_list)
#pool.map(process_for_county, county_list)
final_reduced_df = pd.concat(df_list, ignore_index=True)
pool.close()

# out_dir = '/project/biocomplexity/data/XMode/evaluation/US_data_semantic/'+month+'/'+day+'/'
if not os.path.isdir(out_dir_csv):
    os.makedirs(out_dir_csv)

if not os.path.isdir(out_dir_dict):
    os.makedirs(out_dir_dict)
#out_dir+file_part
final_reduced_df.to_csv(out_dir_csv+file_part)

# cur_dict = get_semantic_dict(get_semantic_df(final_reduced_df))



# ff = open(out_dir_dict+file_part+"_dict.dict",'w')
# ff.write(str(cur_dict))

endtime = datetime.now()
mylogger.info("Start time: " + str(starttime))
mylogger.info("End time: " + str(endtime))
mylogger.info("Run time: " + str(endtime - starttime))

logging.shutdown()
