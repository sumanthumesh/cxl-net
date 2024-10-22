import networkx as nx
import matplotlib.pyplot as plt
from cache.cachesim import BaseCache,CacheLine,DirectoryEntry,DirectoryState,HostCache,SnoopFilter,OpType,debug_print,Config
from typing import List, Dict
import sys
import json
from cache import cachesim

ADDR_WIDTH = 64
cachesim.DEBUG = True

class DirectoryEntryExtended(DirectoryEntry):

    def __init__(self):
        super().__init__()
        self.dir_location = None
    
    def __str__(self):
        return f"{self.state},{self.sharers},{self.owner} on {self.dir_location}"

class CXLNet:

    def __init__(self,num_hosts:int,num_devices:int,num_switches:int):
        '''
        Initialize network. Specify number of nodes.
        Create networkx graph
        '''
        self.num_hosts = num_hosts
        self.num_devices = num_devices
        self.num_switches = num_switches

        self.host_ids = [f"H{i}" for i in range(self.num_hosts)]
        self.device_ids = [f"D{i}" for i in range(self.num_devices)]
        self.switch_ids = [f"S{i}" for i in range(self.num_switches)]

        self.nodeids = self.host_ids + self.device_ids + self.switch_ids
        print(self.nodeids)

        self.G = nx.Graph()
        #Add nodes
        self.G.add_nodes_from(self.nodeids)


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
        
        colors = {
            'H':'Red',
            'D':'Blue',
            'S':'Green'
        }

        print(list(self.G.nodes()))
        print(list(self.G.edges()))

        node_colors = [colors[n[0]] for n in self.nodeids]
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

class CXLHost(HostCache):
    '''
    Basecache with LRU implemented
    '''
    def __init__(self,blk_size,num_lines,assoc,id=-1):
        #Call parent constructor
        super().__init__(blk_size,num_lines,assoc,id)

    def allocate(self, addr):
        '''
        Add a new line to the cache
        '''
        tag, setid, blk = self.split_addr(addr)
        if self.check_hit(addr):
            #Cache already has line, move on
            pass
        else:
            #Cache doesnt have line, 
            #Check if there is space for line
            if not self.set_full(setid):
                #Set the line
                self.set_line(addr)
            else:
                #No space. Evict a line
                #Get LRU
                replacement_addr = self.replacement_candidate(addr)
                #Tell snoop filter to remove it
                self.dir.writeback(replacement_addr)
                #Remove it from host
                self.evict(addr)

class CXLDevice(SnoopFilter):

    def __init__(self, blk_size, num_entries, assoc, id=-1):
        super().__init__(blk_size, num_entries, assoc, id)


class TopLevelSimulator:

    def __init__(self,hosts: List[CXLHost],device: CXLDevice, network: CXLNet):
        self.hosts = hosts
        self.snpf = device
        self.network = network
        self.reqid = 0

        self.node_id_vs_label: Dict[int, str] = dict()
        for h in hosts:
            self.node_id_vs_label[h.id] = f"H{h.id}"
        print(self.node_id_vs_label)
        self.node_id_vs_label[snpf.id] = f"D0"
        print(self.node_id_vs_label)
        for i in range(network.num_switches):
            self.node_id_vs_label[network.num_hosts + 1 + i] = f"S{i}"
        print(self.node_id_vs_label)

        #Inverse list
        self.label_vs_node_id = {val:key for key,val in self.node_id_vs_label.items()}
        
        with open("nodes.json","w") as file:
            json.dump(self.node_id_vs_label,file,indent=4)


    def node_id_to_label(self,nodeid:int)->str:
        '''
        Convert node id which is an int to node label used in network X
        '''
        return self.node_id_vs_label[nodeid]

    def label_to_node_id(self,label:str)->int:
        '''
        Convert node label used in network X to node id which is an int 
        '''
        return self.label_vs_node_id[label]

    def allocate_switch(self,requestor_id: int):
        '''
        Return switch ID where we will place our directory
        '''
        #For now I am just going to allocate greedily on the node closest to host
        path_device_to_switch = nx.shortest_path(self.network.G,source=f"D0",target=self.node_id_to_label(requestor_id))
        switch_label = path_device_to_switch[-2]
        assert switch_label[0] == 'S','Invalid node for switch allocation'
        debug_print(f"Allocated space for line on {switch_label}")
        return self.label_to_node_id(switch_label)

    def calculate_hops(self,directory_entry: DirectoryEntryExtended, optype: OpType, requestor_id: int, switch_id: int = None):
        '''
        Given details of a transaction, find out how many theoretical hops it will take
        '''
        debug_print(f"Calculate hops")
        #If there is no directory entry
        if directory_entry == None:
            #Path will be
            #Req -> Device -> Switch -> Req
            #                 Switch -> Device
            debug_print(f"{optype} when invalid")
            return self.network.cost(self.node_id_to_label(requestor_id),f"D0") +\
                   self.network.cost(f"D0",self.node_id_to_label(switch_id)) +\
                   self.network.cost(self.node_id_to_label(switch_id),self.node_id_to_label(requestor_id))
        else:
            #Directory entry exists
            #Read when modified or Write when modified
            if directory_entry.state == DirectoryState.A:
                #Path will be
                #Req -> Switch -> Owner -> Switch -> Req
                debug_print(f"{optype} when modified, dir on {self.node_id_to_label(directory_entry.dir_location)}")
                return 2 * (self.network.cost(self.node_id_to_label(requestor_id),self.node_id_to_label(directory_entry.dir_location)) +\
                            self.network.cost(self.node_id_to_label(directory_entry.dir_location),self.node_id_to_label(directory_entry.owner)))
            elif directory_entry.state == DirectoryState.S and optype == OpType.READ:
                #Path will be
                #Req -> Dir Location -> Closest Sharer -> Dir Location -> Req
                path_to_closest_sharer = min([nx.shortest_path_length(self.network.G,source=self.node_id_to_label(directory_entry.dir_location),target=self.node_id_to_label(sharer)) for sharer in directory_entry.sharers])
                debug_print(f"{optype} when shared, dir on {self.node_id_to_label(directory_entry.dir_location)}")
                return 2 * (self.network.cost(self.node_id_to_label(requestor_id),self.node_id_to_label(directory_entry.dir_location)) +\
                            path_to_closest_sharer)
            elif directory_entry.state == DirectoryState.S and optype == OpType.WRITE:
                #Path will be
                #Req -> Dir Location -> Furthest sharer -> Dir Location -> Req
                path_to_furthest_sharer = max([nx.shortest_path_length(self.G,source=self.node_id_to_label(directory_entry.dir_location),target=self.node_id_to_label(sharer)) for sharer in directory_entry.sharers])
                debug_print(f"{optype} when shared, dir on {self.node_id_to_label(directory_entry.dir_location)}")
                return 2 * (self.network.cost(self.node_id_to_label(requestor_id),self.node_id_to_label(directory_entry.dir_location)) +\
                            path_to_furthest_sharer)
            else:
                print("Unexpected scenario")
                print(exit)
    
    def process_req(self,addr:int,optype:OpType,requestor:int):
        '''
        Function processes all the requests made in this address space
        '''

        self.snpf.req_id = self.reqid
        debug_print(f"{self.reqid},{hex(addr)},{optype},{requestor}")
        self.reqid += 1
        ##############################################
        #This will helpful for using the GUI debugger
        if self.reqid == 640:
            pass
        ##############################################
        
        #Increment hit counter of either switch of snpf
        if self.snpf.check_hit(addr):
            self.snpf.stats["Hit"] += 1
        else:
            self.snpf.stats["Miss"] += 1
            
        #Check if there is a directory entry for this address
        if self.snpf.check_hit(addr):
            debug_print(f"{hex(addr)} is a hit located on {self.node_id_to_label(snpf.get_line(addr).dir_location)} {self.snpf.split_addr(addr)}")
            
            #Update the LRU (since it is a hit)
            self.snpf.add_to_lru(addr)
            self.hosts[requestor].add_to_lru(addr)
            
            dentry:DirectoryEntryExtended = self.snpf.get_line(addr)
            debug_print(dentry)
            if dentry.state == DirectoryState.A:
                if dentry.owner == requestor:
                    #If the requestor is same as owner, any operation can be accomodated
                    #If the requestor already as copy and is owner, there are no coherence messages generated
                    pass
                else:
                    #Handle based on read or write
                    if optype == OpType.READ:
                        #Make owner a sharer
                        self.snpf.add_sharer(addr,dentry.owner)
                        #Add the requestor as a sharer
                        self.snpf.add_sharer(addr,requestor)
                        #Remove owner
                        self.snpf.set_owner(addr,None)
                        #Set new state
                        self.snpf.set_state(addr,DirectoryState.S)
                        #Add the line to new sharers cache
                        self.hosts[requestor].allocate(addr)
                        #Network section
                        self.calculate_hops(dentry,optype,requestor,None)
                    elif optype == OpType.WRITE:
                        #Evict line from the current owners cache
                        self.hosts[dentry.owner].evict(addr)
                        #Change owner to new owner
                        self.snpf.set_owner(addr,requestor)
                        #Set new state
                        self.snpf.set_state(addr,DirectoryState.A)
                        #Add the line to new owners cache
                        self.hosts[requestor].allocate(addr)
                    #Network section
                    self.calculate_hops(dentry,optype,requestor,None)
            elif dentry.state == DirectoryState.S:
                if optype == OpType.READ:
                    #Is requestor a sharer?
                    if requestor in self.snpf.get_sharers(addr):
                        #Sharer can do read without any change
                        pass
                    else:
                        #Add requestor as sharer
                        self.snpf.add_sharer(addr,requestor)
                        #Add copy of line to new sharers cache
                        self.hosts[requestor].allocate(addr)
                        #Network section
                        self.calculate_hops(dentry,optype,requestor,None)
                elif optype == OpType.WRITE:
                    #Invalidate the line form cache of all hosts
                    for hostid in self.snpf.get_sharers(addr):
                        self.hosts[hostid].evict(addr)
                    #Remove all the sharers
                    self.snpf.remove_all_sharers(addr)
                    #Add requestor as new owner
                    self.snpf.set_owner(addr,requestor)
                    #Set new state
                    self.snpf.set_state(addr,DirectoryState.A)
                    #Add the line to new owners cache
                    self.hosts[requestor].allocate(addr)
                    #Network section
                    self.calculate_hops(dentry,optype,requestor,None)
            else:
                print(f"Unexpected state {dentry.state}")
            self.snpf.update_addr(addr)
        else:
            tag, setid, blk = self.snpf.split_addr(addr)
            #Line is in invalid state and no copy exists on any host
            debug_print(f"{hex(addr)} is a miss")
            #CHeck if there is any space on the snoop filter to hold data
            
            #Alias for requesting host
            h:HostCache = self.hosts[requestor]
            
            #Allocate a switch
            allocated_switch = self.allocate_switch(requestor)

            if len(self.snpf.entries[setid]) < self.snpf.assoc:
                #We don't need to evict anything in snoopfilter
                #Just set the correct state
                d = DirectoryEntryExtended()
                if optype == OpType.READ:
                    d.state = DirectoryState.S
                    d.sharers.append(requestor)
                elif OpType.WRITE:
                    d.state = DirectoryState.A
                    d.owner = requestor
                d.dir_location = allocated_switch
                self.snpf.set_line(addr,d)
                #Network section
                self.calculate_hops(d,optype,requestor,allocated_switch)
                #Now allocate line for this on host who will become sharer
                #We might need to evict line on hosts because of this
                #Allocate on this host
                h.allocate(addr)
            else:
                #Value of new directory entry
                d = DirectoryEntryExtended()
                if optype == OpType.READ:
                    d.state = DirectoryState.S
                    d.sharers.append(requestor)
                     
                elif optype == OpType.WRITE:
                    d.state = DirectoryState.A
                    d.owner = requestor
                d.dir_location = allocated_switch
                #We need to evict an entry in the snoop filter
                self.snpf.allocate(addr,d)
                #Network section
                self.calculate_hops(d,optype,requestor,allocated_switch)
                # #Set the line in directory
                # self.snpf.set_line(addr,d)
                #Add the copy on the host
                h.allocate(addr)      
        
        # self.print_status()        
        #Now that we have done all the state transitions, we can verify if everything is correct with some extra conditions
        #Make sure directory entry exists in device 
        if not self.snpf.check_hit(addr):
            print(f"Line not found in Device")        
            exit(2)
        #Make sure requestor has a copy of the line
        if not self.hosts[requestor].check_hit(addr):
            print(f"Requestor {requestor} does not have shared copy")
            exit(2)
        
        #If it is a read, make sure line is in S or A state
        if optype == OpType.READ:
            assert self.snpf.get_state(addr) == DirectoryState.A or self.snpf.get_state(addr) == DirectoryState.S, "Line being read from is not in A or S state"
        elif optype == OpType.WRITE:
            assert self.snpf.get_state(addr) == DirectoryState.A, "Line being written to is not in A state"
        
        #State specific constraints
        if self.snpf.get_state(addr) == DirectoryState.S:
            #If in shared state, owner should be undefined and sharer list should not be empty
            assert self.snpf.get_owner(addr) == None, "Owner defined in S state"
            assert len(self.snpf.get_sharers(addr)) > 0, "Emptry sharer list in S state"
            #Requestor should be a sharer
            assert requestor in self.snpf.get_sharers(addr), "Requestor is not a member of sharer list"
            #Each sharer should have a copy of the line
            for hostid in self.snpf.get_sharers(addr):
                assert self.hosts[hostid].check_hit(addr), f"Sharer {hostid} does not have copy of line"
            #Number of hosts with copy of line and number of sharers in sharer list should match
            hosts_with_copy = []
            for host in self.hosts:
                if host.check_hit(addr):
                    hosts_with_copy.append(host.id)
            assert sorted(hosts_with_copy) == sorted(self.snpf.get_sharers(addr)), f"Hosts with copy of line and sharer list do not match"
        elif self.snpf.get_state(addr) == DirectoryState.A:
            #If in exclusive state, owner should be defined and sharer list should be empty
            assert self.snpf.get_owner(addr) != None, "Owner undefined in A state"
            assert len(self.snpf.get_sharers(addr)) == 0, "Non-Emptry sharer list in A state"
            #Requestor and owner should be same
            assert requestor == self.snpf.get_owner(addr), "Requestor and owner in A state are not the same"
            #Host with copy of line must be requestor and owner
            for host in self.hosts:
                if host.check_hit(addr):
                    assert host.id == requestor and host.id == self.snpf.get_owner(addr), f"Host with copy ({host.id}), owner ({self.snpf.get_owner(addr)}) and requestor ({requestor}) are all not the same"
        
        #After every transaction, verify entire system state
        # self.verify_system_state()

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


if __name__ == '__main__':

    
    config_file = sys.argv[1]
    
    trace_file = sys.argv[2]
    
    if len(sys.argv) == 4 and sys.argv[3] == 'debug':
        DEBUG = True
    
    # host_cache = HostCache(64,8,2,0)
    
    #Clear eviction log file
    with open('eviction.log','w') as file:
        pass
    
    #Parse the config file
    cfg = Config(config_file)
    
    #Write the config onto console
    print("#####################")
    print("Config")
    for key,val in cfg.d.items():
        print(f"{key}:{val}")
    print("#####################")
    
    hosts = [CXLHost(cfg.host_line_size,cfg.host_num_lines,cfg.host_assoc,i) for i in range(cfg.num_hosts)]
    snpf = CXLDevice(cfg.device_line_size,cfg.device_num_lines,cfg.device_assoc,cfg.num_hosts)

    #Provide each host with a reference to the snoop filter
    for host in hosts:
        host.set_dir(snpf)
    #Provide snoop filter with the list of hosts
    snpf.set_hosts(hosts)
    
    N = CXLNet(num_hosts=cfg.num_hosts,num_devices=1,num_switches=cfg.num_switches)
    #Build the network topology
    edges = [("S0","S1"),("S1","S2"),("S3","S4"),("S4","S5"),("S6","S7"),("S7","S8"),
             ("S0","S3"),("S1","S4"),("S2","S5"),("S3","S6"),("S4","S7"),("S5","S8"),
             ("H0","S0"),("H1","S1"),("H2","S2"),("H3","S8"),("D0","S7")]
    N.G.add_edges_from(edges)
    N.draw()

    sim = TopLevelSimulator(hosts,snpf,N)

    #Read requests from trace file line by line and input to the coherence engine
    with open(trace_file) as file:
        while True:
            line = file.readline()
            if not line:
                break
            split_line = line.split(' ')
            addr = int(split_line[0],16)
            optype = OpType.READ if split_line[1] == 'R' else OpType.WRITE
            hostid = int(split_line[2].strip())
            #Now send request to coherence engine
            sim.process_req(addr,optype,hostid)
    