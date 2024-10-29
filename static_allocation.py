import sys
import os
import networkx as nx
import matplotlib.pyplot as plt
from cache.cachesim import DirectoryEntry, HostCache, SnoopFilter, BaseCache, debug_print, OpType, DirectoryState
from cache import cachesim
from typing import List, Dict, Set, Tuple
import json

cachesim.DEBUG = True
cachesim.ADDR_WIDTH = 64

class CXLNet:

    def __init__(self,num_hosts:int,num_devices:int,num_switches:int):
        '''
        Initialize network. Specify number of nodes.
        Create networkx graph
        '''
        self.num_hosts = num_hosts
        self.num_devices = num_devices
        self.num_switches = num_switches

        self.host_ids = [i for i in range(self.num_hosts)]
        self.device_ids = [i for i in range(self.num_hosts,self.num_hosts+self.num_devices)]
        self.switch_ids = [i for i in range(self.num_hosts+self.num_devices,self.num_hosts+self.num_devices+self.num_switches)]

        self.nodeids = self.host_ids + self.device_ids + self.switch_ids
        print(self.nodeids)

        self.G = nx.Graph()
        #Add nodes
        self.G.add_nodes_from(self.nodeids)
        
        self.intermediate = None
        self.intermediate_path = []


    def connect(self,nodeA:str,nodeB:str):
        '''
        Add connections
        '''
        #Check if nodeid is valid
        if nodeA not in self.nodeids or nodeB not in self.nodeids:
            print(f"Invalid NodeID {nodeA},{nodeB}")
        #Check if connection is valid
        if nodeA[0] == 'H' and nodeB[0] == 'H' or \
           nodeA[0] == 'D' and nodeB[0] == 'D':
            print(f"Incorrect connection {nodeA}-{nodeB}")
            exit(2)
        
        self.G.add_edge(nodeA,nodeB)

    def draw(self):
        positions = nx.nx_pydot.graphviz_layout(self.G)
        # nx.draw(self.G,pos=positions,with_label)
        
        print(self.host_ids)
        print(self.device_ids)
        print(self.switch_ids)

        # print(list(self.G.nodes()))
        # print(list(self.G.edges()))

        node_colors = []
        for nodeid in self.nodeids:
            if nodeid in self.host_ids:
                node_colors.append('Red')
            elif nodeid in self.device_ids:
                node_colors.append('Yellow')
            elif nodeid in self.switch_ids:
                node_colors.append('Green')

        print(node_colors)
        
        nx.draw_networkx(self.G,pos=positions,nodelist=self.nodeids,node_color=node_colors,with_labels=True)
        plt.savefig('CXL_Topology.png')
        # nx.drawing.nx_pydot.graphviz_layout(self.G, "CXL_Topology.dot")

    def cost(self,nodeA,nodeB):
        '''
        Cost of traversing the path
        Right now I just count number of hops
        '''
        path_length = nx.shortest_path_length(self.G,source=nodeA,target=nodeB)
        if path_length == 0:
            print(f"Nothing to traverse between {nodeA} and {nodeB}")
            exit(1)
        return path_length

    def set_intermediate(self,nodeid:int,path:List[int]):
        '''
        Set an intermediate node. Any message from host to device will have to travel through here
        The path from intermediate to device is already known and fixed
        '''
        assert nodeid in self.switch_ids, f"Unknown node {nodeid}"
        self.intermediate = nodeid
        self.intermediate_path = path

    def host2dir_path(self,host:int,dir:int):
        '''
        Calculate path between host and dir location
        '''
        return 2*(self.cost(host,self.intermediate) + self.cost(self.intermediate,dir))
    
    def path_cost(self,*args: Tuple[int]):
        '''
        Given a set of nodes, this will give the path cost travelling along these nodes
        '''
        cost = 0
        nodes = args
        for window in zip(nodes,nodes[1:]):
            cost += nx.shortest_path_length(self.G,source=window[0],target=window[1])
        debug_print(f"Path: {nodes}, Cost: {cost}")
        return cost            
            
    def closest_node(self,source:int,dest:List[int]):
        '''
        Given one node and a list of nodes, find the node closest
        '''
        return min(dest, key=lambda node: nx.shortest_path_length(self.G, source, node))
    
    def furthest_node(self,source:int,dest:List[int]):
        '''
        Given one node and a list of nodes, find the node furthest away
        '''
        return max(dest, key=lambda node: nx.shortest_path_length(self.G, source, node))
        
class DirectoryEntryExtended(DirectoryEntry):

    def __init__(self):
        super().__init__()
        self.dir_location = None
    
    def __str__(self):
        return f"{self.state},{self.sharers},{self.owner} on {self.dir_location}"

class CXLHost(HostCache):
    
    def allocate(self,addr):
        '''
        Allocate an entry on the host
        '''
        #Split addr
        tag, setid, blk = self.split_addr(addr)
        
        replacement_addr = None
        
        #Check if there is space in the set
        if self.set_full(setid):
            #Find replacement candidate
            replacement_addr = self.replacement_candidate(addr)
            #Remove the line
            self.evict(replacement_addr)
        #Add the new line
        self.set_line(addr)
        return replacement_addr
    
class CXLSwitch(HostCache):
    
    def allocate(self,addr,data:DirectoryEntryExtended):
        '''
        Allocate entry on switch
        '''
        #Split addr
        tag, setid, blk = self.split_addr(addr)
        
        replacement_addr = None
        
        #Check if there is space in the set
        if self.set_full(setid):
            #Find replacement candidate
            replacement_addr = self.replacement_candidate(addr)
            #Remove the line
            self.evict(replacement_addr)
        #Add the new line
        self.set_line(addr,data)
        return replacement_addr
    
    def evict(self,addr):
        tag, setid, blk = self.split_addr(addr)
        assert tag in self.entries[setid].keys(), f"Entry {self.split_addr(addr)} not found in HostCache {self.id} during eviction"
        #We no longer need to track this for LRU
        self.del_from_lru(addr)
        debug_print(f"Evicted {hex(self.get_addr(addr))} from Switch {self.id} in set {setid}")
        self.delete_line(tag,setid)

class CXLDevice(SnoopFilter):
    
    def set_switches(self,switches:Dict[int,CXLSwitch]):
        '''
        Give device access to switches
        '''        
        self.switches = switches
        self.num_switches = len(switches)
    
    def allocate(self,addr,data):
        '''
        Make space for data and allocate on the device (not on switch)
        '''
        #Split addr
        tag, setid, blk = self.split_addr(addr)
        
        replacement_addr = None
        
        #Check if there is space in the set
        if self.set_full(setid):
            #Find replacement candidate
            replacement_addr = self.replacement_candidate(addr)
            #Remove the line
            self.evict(replacement_addr)
        #Add the new line
        self.set_line(addr,data)
        return replacement_addr
        
    def evict(self,addr):
        tag, setid, blk = self.split_addr(addr)
        assert tag in self.entries[setid].keys(), f"Entry {self.split_addr(addr)} not found in HostCache {self.id} during eviction"
        #We no longer need to track this for LRU
        self.del_from_lru(addr)
        debug_print(f"Evicted {hex(self.get_addr(addr))} from Device {self.id} in set {setid}")
        self.delete_line(tag,setid)
    
    def resolve_object(self,objid:int):
        '''
        Get object form the id
        '''
        if objid == self.id:
            obj = self
        elif objid in self.switches.keys():
            obj = self.switches[objid]
        else:
            print(f"Unknown directory destination {objid}")
            exit(2)
        return obj
    
    def migrate(self,addr:int,sourceid:int,targetid:int):
        '''
        Move directory entry from device to switch or vice versa
        '''
        #Resolve objects
        source = self.resolve_object(sourceid)        
        target = self.resolve_object(targetid)        
        
        #Copy data from source
        d = source.get_line(addr)
        #Allocate line in target
        
        replacement_addr = target.allocate(addr,d)
        
        #If there needs to be a replacement, pass the information to the calling function
        if replacement_addr != None:
            return replacement_addr
        else:
            return None
        
    def search_entry_device(self,addr):
        tag, setid, blk = self.split_addr(addr)
        return self.search_set(tag,setid)
    
    def search_entry_switch(self,addr):
        tag, setid, blk = next(iter(self.switches.values())).split_addr(addr)
        
        for switchid,switch in self.switches.items():
            if switch.search_set(tag,setid):
                return switchid

        return None
    
    def find_directory_entry(self,addr):
        '''
        Return directory entry by searching both device and switches
        '''
        
        if self.search_entry_device(addr):
            return self.get_line(addr)
        elif self.search_entry_switch(addr) != None:
            return self.switches[self.search_entry_switch(addr)].get_line(addr)
        else:
            return None
                
    
    def placement_policy(self,addr:int,optype:OpType,requestor:int,intermediate_path:List[int],reqid:int):
        '''
        Function implements the placement policy. It returns the node id where we need to allocate the entry for a new line that was previously in the invalid state
        '''
        #Randomly place on device or switches
        possible_dir_locations = intermediate_path + [self.id]
        #Choose based on modulus
        return possible_dir_locations[reqid % len(possible_dir_locations)]
        # #For now always place on the device
        # return self.id

class CoherenceEngine:
    
    def __init__(self, hosts: List[CXLHost], device: CXLDevice, switches: Dict[int,CXLSwitch]):
        self.hosts = hosts
        self.device = device
        self.switches = switches
        
        self.net: CXLNet = None
        
        self.reqid = 0
        
    def describe(self):
        '''
        Print out a system description with node ids
        '''
        for host in self.hosts:
            print(f"{host.id}: Host")
        print(f"{self.device.id}: Device")
        for switch in self.switches.values():
            print(f"{switch.id}: Switch")
        
    def add_network(self,net: nx.Graph):
        '''
        Assign topology info
        '''
        self.net=net

    def handle_host_eviction(self,addr:int,dentry:DirectoryEntry,evicting_host:int):
        '''
        When a host chooses to evict an entry, need to update device about it
        '''
        if addr == None:
            return
        
        i = self.net.intermediate
        
        #Remove from owner
        if dentry.owner != None:
            #Calculate path
            #owner -> device -> owner
            self.net.path_cost(dentry.owner,i,device,i,dentry.owner)
            self.hosts[dentry.owner].evict(addr)
            
        if len(dentry.sharers) != 0:
            #Calculate path
            #Evicting host -> device -> furthest sharer -> device
            furthest_sharer = self.net.furthest_node(device,dentry.sharers)
            self.net.path_cost(evicting_host,i,self.device.id,furthest_sharer,i,self.device.id)
            for hostid in dentry.sharers:
                self.hosts[hostid].evict(addr)
                
    def handle_directory_eviction(self,addr:int,dentry:DirectoryEntry):
        '''
        When a directory entry is evicted, all copies of the data in the hosts need to be invalidated
        '''
        
        if addr == None:
            return
        
        i = self.net.intermediate
        
        if dentry.state == DirectoryState.A:
            #Calculate path
            #device -> owner -> device
            self.net.path_cost(self.device.id,i,dentry.owner,i,self.device.id)
            #Evict from owner
            self.hosts[dentry.owner].evict(addr)
        elif dentry.state == DirectoryState.S:
            #Calculate path
            #device -> furthest sharer -> device
            furthest_sharer = self.net.furthest_node(device,dentry.sharers)
            self.net.path_cost(self.device.id,i,furthest_sharer,i,self.device.id)
            #Evict from all sharers
            for hostid in dentry.sharers:
                self.hosts[hostid].evict(addr)
    
    def migration_policy(self):
        '''
        This function is called every transaction and performs migration of directory entry
        '''
        #For now this is a dummy function
        pass
    
    def verify_line(self,addr):
        '''
        Invariants for a single line
        '''
        # Fetch the line
        dentry: DirectoryEntry = self.device.find_directory_entry(addr)
        # Invariants
        assert dentry != None, f"Line for {hex(addr)} does not exist"
        assert dentry.owner == None or len(dentry.sharers) == 0, f"{dentry} invalid state"
        assert (dentry.state == DirectoryState.A and dentry.owner != None) or\
               (dentry.state == DirectoryState.S and len(dentry.sharers) != 0), \
                f"Invalid combo, State {dentry.state}, Owner {dentry.owner}, Sharers {dentry.sharers}"
        if dentry.owner != None:
            assert self.hosts[dentry.owner].check_hit(addr), f"Owner {dentry.owner} does not have copy of line"
            #Verify that no other host has this line
            for host in self.hosts:
                if host.id != dentry.owner:
                    assert not host.check_hit(addr), f"Line found in {host.id} which is not owner {dentry.owner}"
        if len(dentry.sharers) > 0:
            for hostid in dentry.sharers:
                assert self.hosts[hostid].check_hit(addr), f"Sharer {hostid} does not have a copy of the line"
            #Verify that no other host has this line
            for host in self.hosts:
                if host.id not in dentry.sharers:
                    assert not host.check_hit(addr), f"Line found in {host.id} which is not a sharer {dentry.sharers}"
        #Make sure directory entry is in only one location
        num_dirs = 0
        if self.device.check_hit(addr):
            num_dirs += 1
        for switchid,switch in self.switches.items():
            if switch.check_hit(addr):
                num_dirs += 1
        assert num_dirs == 1, f"Line for {addr} found in {num_dirs} location instead of one"
    
    def verify_system_state(self):
        '''
        Perform lots of checks on the current system
        '''
        for cacheset in self.device.entries:
            for tag,line in cacheset.items():
                self.verify_line(line.addr)
        for switch in self.switches.values():
            for cacheset in switch.entries:
                for tag,line in cacheset:
                    self.verify_line(line.addr)
        debug_print(f"System state verified")
    
    def process_req(self, addr:int, optype: OpType, requestor: int):
        '''
        Process request one by one
        '''
        
        ########################
        #For debugging help
        if self.reqid == 800:
            pass
        ########################

        print(f"{self.reqid}: {hex(addr)} {optype} {requestor}")

        hit = False
        switchid = self.device.search_entry_switch(addr)
        dir_holder = None
        
        #Needed for path costs
        i = self.net.intermediate
        path_cost = 0
        
        #Check if entry exists on switch or device
        if self.device.search_entry_device(addr):
            hit = True
            print(f"Entry found in device")
            dir_holder = self.device
        elif switchid != None:
            hit = True
            print(f"Entry found on switch {switchid}")
            dir_holder = self.device.switches[switchid]
        else:
            print(f"Line {hex(addr)} not found")
            
        if hit:
            dentry: DirectoryEntry = dir_holder.get_line(addr)
            debug_print(f"Current state: {dentry}")         
            #Check state and process accordingly
            assert dentry.state != DirectoryState.I
            #If line is in Modified state
            if dentry.state == DirectoryState.A:
                old_owner = dentry.owner
                #If requestor is also owner, pass
                if requestor == dentry.owner:
                    #No cost, wont be a CXL access
                    pass
                else:
                    #If its a read
                    if optype == OpType.READ:
                        #Change owner to sharer
                        dentry.sharers.append(dentry.owner)
                        #Remove owner
                        dentry.owner = None
                        #Change state
                        dentry.state = DirectoryState.S
                        #Allocate on the requesting host
                        replacement_addr = self.hosts[requestor].allocate(addr)
                        #If there is any replacement, handle it
                        if replacement_addr != None:
                            self.handle_host_eviction(replacement_addr,self.device.find_directory_entry(replacement_addr))
                        #Add requestor to list of sharers
                        dentry.sharers.append(requestor)
                        #Calculate path
                        #requestor -> dir -> owner -> dir -> requestor
                        path_cost = self.net.path_cost(requestor,i,dir_holder.id,old_owner,i,dir_holder.id,requestor)
                    else:
                        #Allocate on requestor
                        replacement_addr = self.hosts[requestor].allocate(addr)
                        #If there is any replacement, handle it
                        if replacement_addr != None:
                            self.handle_host_eviction(replacement_addr,self.device.find_directory_entry(replacement_addr))
                        #Remove line from the original owner
                        self.hosts[dentry.owner].evict(addr)
                        #Set requestor as new owner
                        dentry.owner = requestor
                        #Calculate path
                        #requestor -> dir -> owner -> dir -> requestor
                        path_cost = self.net.path_cost(requestor,i,dir_holder.id,old_owner,i,dir_holder.id,requestor)
                    #Write the updated dentry
                    dir_holder.set_line(addr,dentry)
            elif dentry.state == DirectoryState.S:
                old_sharer_list = dentry.sharers[:]
                #If operation is read
                if optype == OpType.READ:
                    #If requestor is a sharer, dont do anything
                    if requestor in dentry.sharers:
                        pass
                    #Add requestor
                    else:
                        #Allocate on the requesting host
                        replacement_addr = self.hosts[requestor].allocate(addr)
                        #If there is any replacement, handle it
                        if replacement_addr != None:
                            self.handle_host_eviction(replacement_addr,self.device.find_directory_entry(replacement_addr))
                        #Add requestor as sharer in directory
                        dentry.sharers.append(requestor)
                        #Write the updated entry
                        dir_holder.set_line(addr,dentry)
                        #Calculate path
                        #requestor -> dir -> closest sharer -> dir -> requestor
                        closest_sharer = self.net.closest_node(requestor,old_sharer_list)
                        path_cost = self.net.path_cost(requestor,i,dir_holder.id,closest_sharer,i,dir_holder.id,requestor)
                #If operation is write
                else:
                    #Allocate on the requesting host
                    replacement_addr = self.hosts[requestor].allocate(addr)
                    #If there is any replacement, handle it
                    if replacement_addr != None:
                        self.handle_host_eviction(replacement_addr,self.device.find_directory_entry(replacement_addr))
                    #Remove the line from all sharers
                    for hostid in dentry.sharers:
                        #Evict line from all hosts
                        #Dont remove it from requestor in case it is part of sharers
                        if hostid == requestor:
                            continue
                        self.hosts[hostid].evict(addr)
                    #Empty the sharer list
                    dentry.sharers = []
                    #Set reuestor as owner
                    dentry.owner = requestor
                    #Set new state
                    dentry.state = DirectoryState.A
                    dir_holder.set_line(addr,dentry)
                    #Calculate path
                    #requestor -> dir -> furthest sharer -> dir -> requestor
                    farthest_sharer = self.net.furthest_node(requestor,old_sharer_list)
                    path_cost = self.net.path_cost(requestor,i,dir_holder.id,farthest_sharer,i,dir_holder.id,requestor)
        else:
            #We dont have the directory entry
            #Need to allocate it
            #Find the destination to allocate
            destination_id = self.device.placement_policy(addr,optype,requestor,self.net.intermediate_path,self.reqid)
            #Get destination object
            destination = self.device.resolve_object(destination_id)
            #Allocate on the destination
            dentry = DirectoryEntry()
            if optype == OpType.READ:
                dentry.state = DirectoryState.S
                dentry.sharers = [requestor]
                dentry.owner = None
            else:
                dentry.state = DirectoryState.A
                dentry.sharers = []
                dentry.owner = requestor
            replacement_addr = destination.allocate(addr,dentry)
            #Handle the replacement                    
            if replacement_addr != None:
                self.handle_directory_eviction(replacement_addr,destination.get_line(replacement_addr))
            #Give host copy of the line
            replacement_addr = self.hosts[requestor].allocate(addr)
            if replacement_addr != None:
                self.handle_host_eviction(replacement_addr,destination.get_line(replacement_addr))
                
            #Calculate path
            #requestor -> device -> requestor
            path_cost = self.net.path_cost(requestor,i,self.device.id,i,requestor)
            
        #Once coherence operations are complete, check if we want to migrate the entry
        self.migration_policy()
        
        #Verification checks
        dentry = self.device.find_directory_entry(addr)
        #Lots of em
        #Check state
        assert not (dentry.owner != None and len(dentry.sharers) > 0), f"Line has owner {dentry.owner} and sharers {dentry.sharers} at the same time"
        assert (dentry.state == DirectoryState.A and dentry.owner != None and len(dentry.sharers) == 0) or \
               (dentry.state == DirectoryState.S and dentry.owner == None and len(dentry.sharers) > 0), \
                f"Invalid combination of state {dentry.state}, owner {dentry.owner} and sharers {dentry.sharers}"
        
        #Transaction specific checks
        #The line requested should exist
        assert dentry != None, f"Newly allocated line cannot be found"
        #If the request was for a read, then the allocated state should be S or A
        #If the request was for a write, then the allocated state should be A
        assert (optype == OpType.READ and (dentry.state == DirectoryState.S or dentry.state == DirectoryState.A)) or \
               (optype == OpType.WRITE and dentry.state == DirectoryState.A), f"Incorrect state allocated. Requested {optype} and got {dentry.state}"
        #Requestor should be owner or sharer
        assert (optype == OpType.READ and (requestor in dentry.sharers or requestor == dentry.owner)) or \
               (optype == OpType.WRITE and requestor == dentry.owner), f"Requestor {requestor} not owner {dentry.owner} not in sharers {dentry.sharers}"
        #Owner should have a copy of the line
        if dentry.state == DirectoryState.A:
            assert self.hosts[dentry.owner].check_hit(addr), f"Host {dentry.owner} is owner, but does not have copy of the line"
        if dentry.state == DirectoryState.S:
            for hostid in dentry.sharers:
                assert self.hosts[hostid].check_hit(addr), f"Host {hostid} is sharer, but does not have copy of the line"
        
        self.verify_system_state()

        self.reqid += 1

class Config:
    '''
    Class which holds the configuration parameters we need for the simulation
    '''
    def __init__(self,filename):
        '''
        Parse a json file and update the config parameters
        '''
        with open(filename) as file:
            d = json.load(file)
            self.d = d

        self.num_hosts = d["Num hosts"]
        self.host_line_size = d["Host line size"]
        self.host_num_lines = d["Host num lines"]
        self.host_assoc = d["Host assoc"]
        self.device_line_size = d["Device line size"]
        self.device_num_lines = d["Device num lines"]
        self.device_assoc = d["Device assoc"]
        self.num_switches = d["Num switches"]
        self.switch_line_size = d["Switch line size"]
        self.switch_num_lines = d["Switch num lines"]
        self.switch_assoc = d["Switch assoc"]

if __name__ == "__main__":
    
    config_file = sys.argv[1]
    trace_file = sys.argv[2]
    
    cfg = Config(config_file)
    
    hosts = [CXLHost(cfg.host_line_size,cfg.host_num_lines,cfg.host_assoc,i) for i in range(cfg.num_hosts)]
    device = CXLDevice(cfg.device_line_size,cfg.device_num_lines,cfg.device_assoc,cfg.num_hosts)
    switches = {i:CXLSwitch(cfg.switch_line_size,cfg.switch_num_lines,cfg.switch_assoc,i) for i in range(cfg.num_hosts+1,cfg.num_hosts+1+cfg.num_switches)}
    
    device.set_switches(switches)
    
    N = CXLNet(num_hosts=4,num_devices=1,num_switches=9)
    #Build the network topology
    edges = [(5,6),(6,7),(8,9),(9,10),(11,12),(12,13),
             (5,8),(6,9),(7,10),(8,11),(9,12),(10,13),
             (0,8),(1,5),(2,6),(3,7),(4,11)]
    N.G.add_edges_from(edges)
    N.draw()
    
    N.set_intermediate(8,[11,8])
    
    simulator = CoherenceEngine(hosts, device, switches)
    simulator.add_network(N)
    simulator.describe()
    
    with open(trace_file) as file:
        while True:
            line = file.readline()
            if not line:
                break
            s = line.split(' ')
            addr = int(s[0],16)
            rw = OpType.READ if s[1] == 'R' else OpType.WRITE
            hostid = int(s[2].strip())
            
            simulator.process_req(addr,rw,hostid)
    
    
    