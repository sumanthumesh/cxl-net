import sys
from typing import List, Set

def get_hosts(filename):
    
    hosts: Set[int] = set()
    
    with open(filename) as file:
        while True:
            line = file.readline()
            if not line:
                break
            hostid = int(line.split(' ')[2].strip())
            hosts.add(hostid)
    return hosts
            
if __name__ == "__main__":
    
    file = sys.argv[1]
    
    hosts = get_hosts(file)
    print(hosts)