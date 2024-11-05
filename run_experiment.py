import sys
import os
import subprocess
import multiprocessing
from typing import List, Tuple, Dict, Set
import json
import time

SCRATCHSPACE = "./scratchspace/cachesize"
PYTHON = "/mnt/nvme/umeshsum/cxl-net/venvpypy/bin/pypy3"
VENV_COMMAND = "source venvpypy/bin/activate"

def run_command(cmd: str, outfile:str="temp.txt"):
    
    cmd_args = cmd.split(' ')
    print(cmd_args)
    # print(cmd)
    with open(outfile,"w") as file:
        subprocess.run(cmd_args,stdout=file,stderr=file)
        # subprocess.run(cmd_args)
    # subprocess.run(cmd,shell=True)

def values_to_str(s: str):
    
    #Convert a nested list into a scalar array
    # print(s[1])
    # return str(s[0]) + '_' + '_'.join([str(x) for x in s[1]])

    return '_'.join([str(v) for v in s])

def generate_config_and_run(config_template:str,entries_to_change:List[str],values,tracefile:str,file_prefix:str):
    '''
    Generate a new config file and run the workload
    '''
    #Read the config template
    with open(config_template) as file:
        cfg = json.load(file)
        
    #Replace entries
    print(values)
    for entry,value in zip(entries_to_change,values):
        cfg[entry] = value

    #Change output filename
    cfg["Output json"] = os.path.join(SCRATCHSPACE,f"result_{file_prefix}_{values_to_str(values)}.json")

    new_cfg_filename = f"{file_prefix}_{values_to_str(values)}.json"
    
    #Write out the new config file
    cfg_file = os.path.join(SCRATCHSPACE,f"{new_cfg_filename}")
    with open(cfg_file,"w") as file:
        json.dump(cfg,file,indent=4)
        
    log_file = os.path.join(SCRATCHSPACE,f"log_{file_prefix}_{values_to_str(values)}.txt")
        
    #Ready the command we want to run
    cmd = f"{PYTHON} static_allocation.py {cfg_file} {tracefile}"
    print(cmd)
    #run command

    start = time.time()
    run_command(cmd,outfile=log_file)
    end = time.time()
    elapsed = end - start
    #Append execution time to the logfile
    with open(log_file,"a") as file:
        file.write(f"Time: {elapsed}s")
    
    #Remove the config file, we already have the config information in the log
    os.remove(cfg_file)
    
if __name__ == '__main__':
    
    #Config file template
    config_template = sys.argv[1]
    
    #List of tracefiles
    tracefiles = sys.argv[2:]
    
    os.environ["PYTHONPATH"]="/mnt/nvme/umeshsum/cxl-net/venvpypy/lib/pypy3.10/site-packages"
    
    #For each tracefile we need to generate multiple config files, each config representing a different switch set as directory
    #For our case, I am setting the following switch configurations
    #Config parameters to change
    entries_to_change = ["Host num lines","Switch num lines"]
    # entries_to_change = ["Host num lines"]
    #Possible combinations
    # values = [[23,[23]],[27,[27]],[31,[31]],[23,[23,27,31,16]],[31,[16]]]
    values = [[1024,1024],[4096,4096],[16384,16384],[65536,65536]]
    
    args = []
    
    for trace in tracefiles:
        file_prefix = os.path.basename(trace).replace('.trace','')
        for value in values:
            args.append((config_template,entries_to_change,value,trace,file_prefix))
            # generate_config_and_run(config_template,entries_to_change,value,trace,file_prefix)
    
    with multiprocessing.Pool(processes=32) as pool:
        pool.starmap(generate_config_and_run,args)