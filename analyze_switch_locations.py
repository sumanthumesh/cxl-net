import sys
from typing import Dict, List, Set
import matplotlib.pyplot as plt
import subprocess

def weighted_count(curr_set: Set[str], data: Dict[str, float]):
    
    for ele in curr_set:
        incr = 1/len(ele)
        if ele not in data.keys():
            data.update({ele:incr})
        else:
            data[ele] += incr

def count_set(key:int, curr_set: Set[str], data: Dict[int, Dict[str, int]]):
    
    for ele in curr_set:
        if ele in data[key].keys():
            data[key][ele] += 1
        else:
            data[key].update({ele:1})

def analyze_locations(filename):
    
    common_switch_count: Dict[int, int] = dict()
    per_line_per_switch_count: Dict[int, Dict[str, int]] = dict()
    weighted_per_switch_count: Dict[str, float] = dict()
    accesse_per_line: Dict[int, int] = dict()
    
    with open(filename) as file:
        addr = None
        first_line = False
        curr_set = set()
        last_set = set()
        while True:
            line = file.readline()
            if not line:
                break
            if line.startswith('0x'):
                addr = int(line.strip(),16)
                if addr not in common_switch_count.keys():
                    common_switch_count.update({addr:0})
                if addr not in per_line_per_switch_count.keys():
                    per_line_per_switch_count.update({addr:dict()})
                if addr not in accesse_per_line.keys():
                    accesse_per_line.update({addr:1})
                else:
                    accesse_per_line[addr] += 1
            else:
                curr_set = set([x.strip() for x in line.split(',')])
                count_set(addr,curr_set,per_line_per_switch_count)
                weighted_count(curr_set,weighted_per_switch_count)
                common = last_set.intersection(curr_set)
                # print(f"{hex(addr)},{last_set},{curr_set},{common}")
                if len(common) == 0:
                    common_switch_count[addr] += 1
                # print(f"{hex(addr)},{len(common)}")
                last_set = curr_set
        
    print(per_line_per_switch_count)
    print(weighted_per_switch_count)
    
    switch_names = ["S0","S1","S2","S3","S4","S5","S6","S7","S8"]
    lines_per_switch: Dict[str, float] = {s:0 for s in switch_names}
    for addr in per_line_per_switch_count.keys():
        weight = accesse_per_line[addr]
        for switchname,count in per_line_per_switch_count[addr].items():
            lines_per_switch[switchname] += weight * count/sum(per_line_per_switch_count[addr].values()) 
    
    # final_val = {key:val/sum(accesse_per_line.values()) for key,val in lines_per_switch.items()}               
    
    final_val = lines_per_switch
    print(final_val)
    print(sum(final_val.values()))
    
    # plt.figure(1)
    # x = range(len(weighted_per_switch_count))
    # y = [x/49771 for x in weighted_per_switch_count.values()]
    # x_label = weighted_per_switch_count.keys()
    # plt.figure(1)
    x = range(len(final_val))
    y = [x for x in final_val.values()]
    x_label = final_val.keys()
    plt.xlabel("Switches")
    plt.ylabel("Number of cachelines")
    plt.bar(x,y)
    plt.xticks(ticks=x,labels=x_label)
    plt.savefig(f"switch_distribution.png")
    
    
if __name__ == '__main__':
    
    switch_loc_file = sys.argv[1]
    
    analyze_locations(switch_loc_file)