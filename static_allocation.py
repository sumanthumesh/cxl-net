import sys
import os
import networkx as nx
import matplotlib.pyplot as plt
from cache.cachesim import DirectoryEntry, HostCache, SnoopFilter, BaseCache, debug_print, OpType, DirectoryState
from cache import cachesim
from typing import List, Dict, Set, Tuple
import json

# cachesim.DEBUG = True
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
    
    def path_cost(self,nodes: List[int]):
        '''
        Given a set of nodes, this will give the path cost travelling along these nodes
        '''
        debug_print(f"Path: {nodes}")
        cost = 0
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
            return replacement_addr
            # #Remove the line
            # self.evict(replacement_addr)
        #Add the new line
        self.set_line(addr)
        return None
    
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
            return replacement_addr
            #Remove the line
            # self.evict(replacement_addr)
        #Add the new line
        self.set_line(addr,data)
        return None
    
    def evict(self,addr):
        tag, setid, blk = self.split_addr(addr)
        assert tag in self.entries[setid].keys(), f"Entry {hex(addr)} not found in HostCache {self.id} during eviction"
        #We no longer need to track this for LRU
        self.del_from_lru(addr)
        debug_print(f"Evicted {hex(self.get_addr(addr))} from Switch {self.id} in set {setid}")
        self.delete_line(tag,setid)
        
    def remove_sharer(self,addr,hostid):
        tag, setid, blk = self.split_addr(addr)
        self.entries[setid][tag].data.sharers.remove(hostid)

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
            return replacement_addr
            #Remove the line
            # self.evict(replacement_addr)
        #Add the new line
        self.set_line(addr,data)
        return None
        
    def evict(self,addr):
        tag, setid, blk = self.split_addr(addr)
        assert tag in self.entries[setid].keys(), f"Entry {hex(addr)} not found in HostCache {self.id} during eviction"
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
    
    def find_directory_location(self,addr):
        '''
        Return directory entry by searching both device and switches
        '''
        
        if self.search_entry_device(addr):
            return self.id
        elif self.search_entry_switch(addr) != None:
            return self.search_entry_switch(addr)
        else:
            return None
                
class CoherenceEngine:
    
    def __init__(self, hosts: List[CXLHost], device: CXLDevice, switches: Dict[int,CXLSwitch]):
        self.hosts = hosts
        self.device = device
        self.switches = switches
        
        self.net: CXLNet = None
        
        self.reqid = 0
    
        #Different communication flows for which we can track hops
        self.communication_flows = {
            1: "Host initiated eviction from owner, delete directory",
            2: "Host initiated eviction from lone sharer, delete directory",
            3: "Host initiated eviction from one of multiple sharers",
            4: "Directory initiated eviction from owner, directory deleted",
            5: "Directory initiated eviction from all sharers, directory deleted",
            6: "Request shared permission for a line that is exclusively owned by another host",
            7: "Request exclusive permission for a line that is exclusively owned by another host",
            8: "Request shared permission for a line that is shared by another host",
            9: "Request exclusive permission for a line that is solely shared by the current host",
            10: "Request exclusive permission for a line that is shared by other hosts",
            11: "Request an invalid line"
        }
        
        #Record of different communication paths
        self.flow_records: Dict[int, Dict[str,int]] = {key:{
                                    "Improved":0,
                                    "Same":0,
                                    "Deteriorated":0,
                                    "Benefit":0
                                 } 
                             for key in self.communication_flows.keys()}
        
        #Data about which host or host pairs are doing most of the communication
        self.communicating_hosts: Dict[Set[int],int] = dict()
    
    def set_placement_policy(self,policy:str):
        self.placement_policy = policy
    
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

    def static_path_benefit(self,in_network_path:List[int],base_path:List[int],path_type:int):
        '''
        Record or assess the benefits from static allocation
        '''
        in_network_cost = self.net.path_cost(in_network_path)
        base_cost = self.net.path_cost(base_path)
        if in_network_cost > base_cost:
            debug_print(f"Deteriorated path {self.reqid}")
            self.flow_records[path_type]["Deteriorated"] += 1
        elif in_network_cost < base_cost:
            debug_print(f"Improved path {self.reqid}")
            self.flow_records[path_type]["Improved"] += 1
        else:
            debug_print(f"Unchanged path {self.reqid}")
            self.flow_records[path_type]["Same"] += 1
        
        #Record this path flow
        self.flow_records[path_type]["Benefit"] += base_cost - in_network_cost
        
        #Find the set of hosts involved in this transaction
        involved_hosts = tuple(set(base_path).intersection(self.net.host_ids))
        if involved_hosts not in self.communicating_hosts.keys():
            self.communicating_hosts[involved_hosts] = 1
        else:
            self.communicating_hosts[involved_hosts] += 1

    def handle_host_eviction(self,addr:int,dentry:DirectoryEntry,evicting_host:int):
        '''
        When a host chooses to evict an entry, need to update device about it
        '''
        if addr == None:
            return
        
        debug_print(f"Replacing {hex(addr)} from {evicting_host}")
        debug_print(f"Current state {hex(addr)}:{dentry}")
        
        i = self.net.intermediate
        
        dir_node_id = self.device.find_directory_location(addr)
        assert dir_node_id != None, f"Couldnt find directory entry of {hex(addr)} to modify after evicting from host {evicting_host}"
        assert dir_node_id == self.device.id or dir_node_id in self.net.switch_ids, f"{dir_node_id} not device or switch"
        dir_holder = self.device.resolve_object(dir_node_id)
        tag,setid,blk = dir_holder.split_addr(addr)
        #Remove from owner
        if dentry.state == DirectoryState.A:
            #Calculate path
            #owner -> device -> owner
            path = [dentry.owner,i,self.device.id,i,dentry.owner]
            base_path = [dentry.owner,self.device.id,dentry.owner]
            self.static_path_benefit(path,base_path,1)
            debug_print("Here check it out 1")
            self.hosts[dentry.owner].evict(addr)
            #Since there is no host with valid copy left, remove directory entry
            dir_holder.evict(addr)
        elif dentry.state == DirectoryState.S:
            if len(dentry.sharers) == 1:
                lone_sharer = True
            else:
                lone_sharer = False
            #Calculate path
            furthest_sharer = self.net.furthest_node(self.device.id,dentry.sharers)
            #Remove from sharer list
            dir_holder.remove_sharer(addr,evicting_host)
            #Remove from host
            self.hosts[evicting_host].evict(addr)
            #If it was a lone sharer, evict entry from directory
            if lone_sharer:
                dir_holder.evict(addr)
                #Path
                #Evicting host -> device -> Evicting host
                path = [evicting_host,i,self.device.id,i,evicting_host]
                base_path = [evicting_host,self.device.id,evicting_host]
                self.static_path_benefit(path,base_path,2)
                debug_print("Here check it out 2")
            else:
                #Evicting host -> dir location -> evicting host
                path = [evicting_host,i,dir_node_id,i,evicting_host]
                base_path = [evicting_host,dir_node_id,evicting_host]
                self.static_path_benefit(path,base_path,3)
                debug_print("Here check it out 3")
                
            
    def handle_directory_eviction(self,addr:int,dentry:DirectoryEntry,location:int):
        '''
        When a directory entry is evicted, all copies of the data in the hosts need to be invalidated
        '''
        
        if addr == None:
            return
        
        debug_print(f"Replacing {hex(addr)} from {location}")
        debug_print(f"Current state {hex(addr)}:{dentry}")
        
        i = self.net.intermediate
        
        if dentry.state == DirectoryState.A:
            #Calculate path
            #dir location -> owner -> device
            path = [location,i,dentry.owner,i,self.device.id]
            base_path = [self.device.id,dentry.owner,self.device.id]
            self.static_path_benefit(path,base_path,4)
            debug_print("Here check it out 4")
            #Evict from owner
            self.hosts[dentry.owner].evict(addr)
        elif dentry.state == DirectoryState.S:
            #Calculate path
            #dir location -> furthest sharer -> device
            furthest_sharer = self.net.furthest_node(location,dentry.sharers)
            path = [location,i,furthest_sharer,i,self.device.id]
            base_path = [self.device.id,furthest_sharer,self.device.id]
            self.static_path_benefit(path,base_path,5)
            debug_print("Here check it out 5")
            #Evict from all sharers
            for hostid in dentry.sharers:
                self.hosts[hostid].evict(addr)
                
        #Remove line from directory
        if location == self.device.id:
            self.device.evict(addr)
        else:
            self.switches[location].evict(addr)
        
    def placement_policy(self,addr:int,optype:OpType,requestor:int,intermediate_path:List[int],reqid:int):
        '''
        Function implements the placement policy. It returns the node id where we need to allocate the entry for a new line that was previously in the invalid state
        '''
        
        #Choose between multiple placement policies
        if self.placement_policy == "modulo":
        #Randomly place on device or switches
        #Choose based on modulus
            possible_dir_locations = intermediate_path + [self.device.id]
            return possible_dir_locations[reqid % len(possible_dir_locations)]
        elif self.placement_policy == "default":
        #Choose only the device as possible directory location    
            return self.device.id    
        else:
            print("Unknown placement policy")
    
    def migration_policy(self,addr:int,requestor:int):
        '''
        This function is called every transaction and performs migration of directory entry
        '''
        if self.migration_policy == "lazy":
            #Get the directory entry
            dentry:DirectoryEntry = self.device.find_directory_entry(addr)
            #Migrate only if
            #1.Directory entry is currently on the device
            #2.There is only one current sharer
            #3.The sharer is different from the requestor
            if self.device.find_directory_location(addr) == self.device.id and \
               (len(dentry.sharers) == 1 or dentry.owner != None) and \
               (requestor not in dentry.sharers or requestor != dentry.owner):
                
                #Need to find a switch to put the directory on
                #Idea is to find the switch which represents the shortest path
                #Requestor -> intermediate -> selected switch -> current sharer -> intermediate -> selected switch -> requestor
                #Aliasing
                i = self.net.intermediate
                current_holder = dentry.owner if dentry.state == DirectoryState.A else dentry.sharers[0]
                costs:Dict[int,int] = dict()
                for switchid in self.net.intermediate_path:
                    cost = self.net.path_cost(requestor,i,switchid,current_holder,i,switchid,requestor)
                    costs[switchid] = cost
                #Find the switch that requires the minimum cost
                selected_switch: CXLSwitch = min(costs,key=costs.get)
                debug_print(f"Migrating {hex(addr)} from {current_holder} to {selected_switch}")
                #Now allocate entry on this switch
                replacement_addr = selected_switch.allocate(addr,dentry)
                #Handle the replacement                    
                if replacement_addr != None:
                    self.handle_directory_eviction(replacement_addr,selected_switch.get_line(replacement_addr),selected_switch)
                    #Now reattempt to allocate line
                    temp = selected_switch.allocate(addr,dentry)
                    assert temp == None, f"Directory allocation on {selected_switch.id} failed"
                return selected_switch
            else:
                return None 
                
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
                    assert not host.check_hit(addr), f"Line {hex(addr)} found in {host.id} which is not owner {dentry.owner}"
        if len(dentry.sharers) > 0:
            for hostid in dentry.sharers:
                assert self.hosts[hostid].check_hit(addr), f"Sharer {hostid} does not have a copy of the line"
            #Verify that no other host has this line
            for host in self.hosts:
                if host.id not in dentry.sharers:
                    assert not host.check_hit(addr), f"Line {hex(addr)} found in {host.id} which is not a sharer {dentry.sharers}"
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
                for tag,line in cacheset.items():
                    self.verify_line(line.addr)
        
        # Check LRU of all nodes
        for hostid in self.net.host_ids:
            self.hosts[hostid].verify_lru()
        self.device.verify_lru()
        for switchid in self.net.switch_ids:
            self.switches[switchid].verify_lru()
        
        debug_print(f"System state verified")
        
    def print_flow_records(self,filename:str):
        
        total_improved_count = 0
        total_same_count = 0
        total_deteriorated_count = 0
        total_benefit = 0
        #First print per path type records
        for path_type,stats in self.flow_records.items():
            print(f"Type: {self.communication_flows[path_type]}")
            total_improved_count += stats["Improved"]
            total_same_count += stats["Same"]
            total_deteriorated_count += stats["Deteriorated"]
            total_benefit += stats["Benefit"]
            for key,val in stats.items():
                print(f"{key}:{val}")
            if stats['Improved']+stats['Same']+stats['Deteriorated'] != 0:
                print(f"AVG Benefit: {stats['Benefit']/(stats['Improved']+stats['Same']+stats['Deteriorated'])}")
            
        self.flow_records[-1] = {
            "Type" : "Overall",
            "Improved" : total_improved_count,
            "Same" : total_same_count,
            "Deteriorated" : total_deteriorated_count,
            "Benefit" : total_benefit,
            "AVG Benefit" : total_benefit/(total_improved_count+total_same_count+total_deteriorated_count)
        }
        
        with open(filename,"w") as file:
            json.dump(self.flow_records,file,indent=4)
        
        #Print aggregate results
        print(f"Total Improved: {total_improved_count}")
        print(f"Total Same: {total_same_count}")
        print(f"Total Deteriorated: {total_deteriorated_count}")
        print(f"Overall AVG benefit: {total_benefit/(total_improved_count+total_same_count+total_deteriorated_count)}")
    
    def print_communicating_hosts(self):
        
        sorted_data = dict(sorted(self.communicating_hosts.items(),key=lambda item: item[1], reverse=True))
        
        for hostset,count in sorted_data.items():
            print(f"{hostset}:{count}")
    
    def process_req(self, addr:int, optype: OpType, requestor: int):
        '''
        Process request one by one
        '''
        
        ########################
        #For debugging help
        if self.reqid == 23097:
            # cachesim.DEBUG = True
            pass
        ########################

        debug_print(f"{self.reqid}: {hex(addr)} {optype} {requestor}")
        # if addr == 0x5642ccc750e0:
        #     print(f"{self.reqid}: {hex(addr)} {optype} {requestor}")

        if self.reqid % 10000 == 0:
            print(self.reqid)
        hit = False
        switchid = self.device.search_entry_switch(addr)
        dir_holder = None
        
        #Needed for path costs
        i = self.net.intermediate
        path_cost = 0
        
        #Check if entry exists on switch or device
        if self.device.search_entry_device(addr):
            hit = True
            debug_print(f"Entry found in device")
            dir_holder = self.device
        elif switchid != None:
            hit = True
            debug_print(f"Entry found on switch {switchid}")
            dir_holder = self.device.switches[switchid]
        else:
            debug_print(f"Line {hex(addr)} not found")
            
        if hit:
            #Entry might be lazy migrated before being served, keep in mind
            #This migration will happen after the new request has been received
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
                            self.handle_host_eviction(replacement_addr,self.device.find_directory_entry(replacement_addr),requestor)
                            #Now reattempt to allocate line
                            temp = self.hosts[requestor].allocate(addr)
                            assert temp == None, f"Host allocation on {requestor} failed"
                        #Add requestor to list of sharers
                        dentry.sharers.append(requestor)
                        #Calculate path
                        #If we do migration, then the dir_holder will change
                        new_dest = self.migration_policy(addr,requestor)
                        if new_dest != None:
                            assert self.device.find_directory_location(addr) == new_dest, f"Migration of {hex(addr)} from {dir_holder} to {new_dest} unsuccessful"
                            #requestor -> i -> device -> new dir -> owner -> i -> new dir -> requestor
                            path = [requestor,i,self.device.id,new_dest,old_owner,i,new_dest,requestor]
                        else:    
                            #If no migration then
                            assert self.device.find_directory_location(addr) == dir_holder, f"Entry for {hex(addr)} not found in {dir_holder}"
                            #requestor -> i -> dir -> owner -> i -> dir -> requestor
                            path = [requestor,i,dir_holder.id,old_owner,i,dir_holder.id,requestor]
                        base_path = [requestor,self.device.id,old_owner,self.device.id,requestor]
                        path_cost = self.static_path_benefit(path,base_path,6)
                        debug_print("Here check it out 6")
                    else:
                        #Allocate on requestor
                        replacement_addr = self.hosts[requestor].allocate(addr)
                        #If there is any replacement, handle it
                        if replacement_addr != None:
                            self.handle_host_eviction(replacement_addr,self.device.find_directory_entry(replacement_addr),requestor)
                            #Now reattempt to allocate line
                            temp = self.hosts[requestor].allocate(addr)
                            assert temp == None, f"Host allocation on {destination.id} failed"
                        #Remove line from the original owner
                        self.hosts[dentry.owner].evict(addr)
                        #Set requestor as new owner
                        dentry.owner = requestor
                        #Calculate path
                        #If we do migration, then the dir_holder will change
                        new_dest = self.migration_policy(addr,requestor)
                        if new_dest != None:
                            assert self.device.find_directory_location(addr) == new_dest, f"Migration of {hex(addr)} from {dir_holder} to {new_dest} unsuccessful"
                            #requestor -> i -> device -> new dir -> owner -> i -> new dir -> requestor
                            path = [requestor,i,self.device.id,new_dest,old_owner,i,new_dest,requestor]
                        else:    
                            #If no migration then
                            assert self.device.find_directory_location(addr) == dir_holder, f"Entry for {hex(addr)} not found in {dir_holder}"
                            #requestor -> i -> dir -> owner -> i -> dir -> requestor
                            path = [requestor,i,dir_holder.id,old_owner,i,dir_holder.id,requestor]
                        base_path = [requestor,self.device.id,old_owner,self.device.id,requestor]
                        path_cost = self.static_path_benefit(path,base_path,7)
                        debug_print("Here check it out 7")
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
                            self.handle_host_eviction(replacement_addr,self.device.find_directory_entry(replacement_addr),requestor)
                            #Now reattempt to allocate line
                            temp = self.hosts[requestor].allocate(addr)
                            assert temp == None, f"Host allocation on {destination.id} failed"
                        #Add requestor as sharer in directory
                        dentry.sharers.append(requestor)
                        #Write the updated entry
                        dir_holder.set_line(addr,dentry)
                        #Calculate path
                        #If we do migration, then the dir_holder will change
                        new_dest = self.migration_policy(addr,requestor)
                        if new_dest != None:
                            assert self.device.find_directory_location(addr) == new_dest, f"Migration of {hex(addr)} from {dir_holder} to {new_dest} unsuccessful"
                            #requestor -> i -> device -> new dir -> owner -> i -> new dir -> requestor
                            path = [requestor,i,self.device.id,new_dest,old_owner,i,new_dest,requestor]
                        else:    
                            #If no migration then
                            assert self.device.find_directory_location(addr) == dir_holder.id, f"Entry for {hex(addr)} not found in {dir_holder}"
                            #requestor -> i -> dir -> closest sharer -> i -> dir -> requestor
                            closest_sharer = self.net.closest_node(requestor,old_sharer_list)
                            path = [requestor,i,dir_holder.id,closest_sharer,i,dir_holder.id,requestor]
                        base_path = [requestor,self.device.id,closest_sharer,self.device.id,requestor]
                        path_cost = self.static_path_benefit(path,base_path,8)
                        debug_print("Here check it out 8")
                #If operation is write
                else:
                    #Requestor already has line and is only sharer
                    if requestor in dentry.sharers and len(dentry.sharers) == 1:
                        # Requestor only needs permission, not data
                        #Calculate path
                        #requestor -> dir -> requestor
                        path = [requestor,i,dir_holder.id,i,requestor]
                        base_path = [requestor,self.device.id,requestor]
                        path_cost = self.static_path_benefit(path,base_path,9)
                        debug_print("Here check it out 9")
                    else:
                        #If we do migration, then the dir_holder will change
                        new_dest = self.migration_policy(addr,requestor)
                        if new_dest != None:
                            assert self.device.find_directory_location(addr) == new_dest, f"Migration of {hex(addr)} from {dir_holder} to {new_dest} unsuccessful"
                            #requestor -> i -> device -> new dir -> owner -> i -> new dir -> requestor
                            path = [requestor,i,self.device.id,new_dest,old_owner,i,new_dest,requestor]
                        else:    
                            #If no migration then
                            assert self.device.find_directory_location(addr) == dir_holder.id, f"Entry for {hex(addr)} not found in {dir_holder}"
                            #req -> dir -> furthest sharer -> dir -> req
                            farthest_sharer = self.net.furthest_node(requestor,old_sharer_list)
                            path = [requestor,i,dir_holder.id,farthest_sharer,i,dir_holder.id,requestor]
                        base_path = [requestor,self.device.id,farthest_sharer,self.device.id,requestor]
                        path_cost = self.static_path_benefit(path,base_path,10)
                        debug_print("Here check it out 10")
                        
                        if requestor not in dentry.sharers:
                            # Requestor needs data, dir needs acknowledgements
                            #Allocate on the requesting host
                            replacement_addr = self.hosts[requestor].allocate(addr)
                            #If there is any replacement, handle it
                            if replacement_addr != None:
                                self.handle_host_eviction(replacement_addr,self.device.find_directory_entry(replacement_addr),requestor)
                                #Now reattempt to allocate line
                                temp = self.hosts[requestor].allocate(addr)
                                assert temp == None, f"Host allocation on {destination.id} failed"
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
        else:
            #We dont have the directory entry
            #Need to allocate it
            #Find the destination to allocate
            destination_id = self.placement_policy(addr,optype,requestor,self.net.intermediate_path,self.reqid)
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
            # #Handle the replacement                    
            if replacement_addr != None:
                self.handle_directory_eviction(replacement_addr,destination.get_line(replacement_addr),destination_id)
                #Now reattempt to allocate line
                temp = destination.allocate(addr,dentry)
                assert temp == None, f"Directory allocation on {destination.id} failed"
            #Give host copy of the line
            replacement_addr = self.hosts[requestor].allocate(addr)
            if replacement_addr != None:
                self.handle_host_eviction(replacement_addr,self.device.find_directory_entry(replacement_addr),requestor)
                #Now reattempt to allocate line
                temp = self.hosts[requestor].allocate(addr)
                assert temp == None, f"Host allocation on {destination.id} failed"
                
            #Calculate path
            #requestor -> device -> requestor
            path = [requestor,i,self.device.id,i,requestor]
            base_path = [requestor,self.device.id,requestor]
            path_cost = self.static_path_benefit(path,base_path,11)
            debug_print("Here check it out 11")
            
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
        
        if self.reqid % 1000000 == 0:
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
        self.intermediate = d["Intermediate switch"]
        self.intermediate_path = d["Intermediate path"]
        self.output_json = d["Output json"]
        self.placement_policy = d["Placement policy"]
        self.migration_policy = d["Migration policy"]

    def print(self):
        #Write the config onto console
        print("#####################")
        print("Config")
        for key,val in self.d.items():
            print(f"{key}:{val}")
        print("#####################")

if __name__ == "__main__":
    
    config_file = sys.argv[1]
    trace_file = sys.argv[2]
    
    cfg = Config(config_file)
    cfg.print()
    
    hosts = [CXLHost(cfg.host_line_size,cfg.host_num_lines,cfg.host_assoc,i) for i in range(cfg.num_hosts)]
    device = CXLDevice(cfg.device_line_size,cfg.device_num_lines,cfg.device_assoc,cfg.num_hosts)
    switches = {i:CXLSwitch(cfg.switch_line_size,cfg.switch_num_lines,cfg.switch_assoc,i) for i in range(cfg.num_hosts+1,cfg.num_hosts+1+cfg.num_switches)}
    
    device.set_switches(switches)
    
    # N = CXLNet(num_hosts=cfg.num_hosts,num_devices=1,num_switches=cfg.num_switches)
    # #Build the network topology
    # edges = [(5,6),(6,7),(8,9),(9,10),(11,12),(12,13),
    #          (5,8),(6,9),(7,10),(8,11),(9,12),(10,13),
    #          (0,8),(1,5),(2,6),(3,7),(4,11)]
    # N.G.add_edges_from(edges)
    
    # N = CXLNet(num_hosts=cfg.num_hosts,num_devices=1,num_switches=cfg.num_switches)
    # #Build the network topology
    # edges = [
    #     (17,18),(18,19),(19,20),(21,22),(22,23),(23,24),(25,26),(26,27),(27,28),(29,30),(30,31),(31,32),
    #     (17,21),(18,22),(19,23),(20,24),(21,25),(22,26),(23,27),(24,28),(25,29),(26,30),(27,31),(28,32),
    #     (0,17),(1,18),(2,19),(3,20),
    #     (4,20),(5,24),(6,28),(7,32),
    #     (8,32),(9,31),(10,30),(11,29),
    #     (12,29),(13,25),(14,21),(15,17),
    #     (16,31)
    #     ]
    # N.G.add_edges_from(edges)
    
    N = CXLNet(num_hosts=cfg.num_hosts,num_devices=1,num_switches=cfg.num_switches)
    #Build the network topology
    edges = [
        (17,25),(17,26),(18,25),(18,26),
        (19,27),(19,28),(20,27),(20,28),
        (21,29),(21,30),(22,29),(22,30),
        (23,31),(23,32),(24,31),(24,32),
        (25,33),(25,34),(26,35),(26,36),
        (27,33),(27,34),(28,35),(28,36),
        (29,33),(29,34),(30,35),(30,36),
        (31,33),(31,34),(32,35),(32,36),
        (0,17),(1,17),(2,18),(3,18),
        (4,19),(5,19),(6,20),(7,20),
        (8,21),(9,21),(10,22),(11,22),
        (12,23),(13,23),(14,24),(15,24),
        (16,22)
        ]
    N.G.add_edges_from(edges)
    
    N.draw()
    
    N.set_intermediate(cfg.intermediate,cfg.intermediate_path)
    
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
    print(f"Finished processing requests without triggering any assertions")
    
    #Process the cost benefit data
    
    print(simulator.print_flow_records(cfg.output_json))
    # simulator.print_communicating_hosts()
        
    
    
    