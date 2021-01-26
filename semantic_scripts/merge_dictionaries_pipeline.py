#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import sys
#change this to py file

month = sys.argv[1] # take as argument
day = sys.argv[2]

out_dir = '/project/biocomplexity/data/XMode/evaluation/US_data_semantic/'+month+'/'+day+'/raw_dict/'

args = []
dep = []

for file in os.listdir(out_dir):
    if file.endswith('dict'):# and file.startswith('part'):
        #if not os.path.isfile(out_dir+file):
        args.append(file)
        dep.append('none')

#args = args[0:11]
#dep = dep[0:11]
cursize = len(args)
curlevel = 1

# useful commands

# sbatch --dependency=afterany:$jobid00:$jobid01:$jobid02:$jobid03:$jobid04:$jobid05:$jobid06:$jobid07 ab_char.sbatch $1
# grep_ret=$(sbatch --dependency=afterany:$splitjobid grepDependency.sbatch $2)
# grep_arr=($grep_ret)
# grepjobid=${grep_arr[3]}
#echo $grepjobid
# In[2]:


def get_dependency_string(dependency_array):
    if len(dependency_array)==0:
        return ""
    else:
        prefix = "--dependency=afterany"
        for dep in dependency_array:
            prefix = prefix+":$"
            prefix = prefix+dep
        return prefix


# In[3]:


while(cursize>1):
    #print(cursize)
    #print(args[0:cursize])
    #print(dep[0:cursize])
    nextsize = 0
    for i in range(0,cursize,2):
       # print(i)
        dependencies = []
        if (i+1)<cursize:
            job_input_1 = args[i]
            job_input_2 = args[i+1]
            if dep[i]!='none':
                dependencies.append(dep[i])
            if dep[i+1]!='none':
                dependencies.append(dep[i+1])
            #add month and day to the commands#####################################################
            job_output = "dict_level_"+str(curlevel)+"_job_"+str(i)+".dict"
            sbatch_command = "sbatch "+get_dependency_string(dependencies)+" merge_dictionaries.sbatch "+job_input_1+" "+job_input_2+" "+job_output+" "+str(month)+" "+str(day)
            job_ret_variable = "job_level_"+str(curlevel)+"_idx_"+str(i)+"_ret"
            job_arr_variable = "job_level_"+str(curlevel)+"_idx_"+str(i)+"_arr"
            job_id_variable = "job_level_"+str(curlevel)+"_idx_"+str(i)+"_id"
            print(job_ret_variable+"=$("+sbatch_command+")")
            print(job_arr_variable+"=($"+job_ret_variable+")")
            print(job_id_variable+"=${"+job_arr_variable+"[3]}")
            
            args[i//2] = job_output
            dep[i//2] = job_id_variable
            #do something
        #args[i//2] = args[i]
        else:
            args[i//2] = args[i]
            dep[i//2] = 'none'
        nextsize = nextsize+1
    cursize = nextsize
    curlevel = curlevel+1
    


# In[ ]:




