import networkx as nx
import matplotlib.pyplot as plt

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


        self.nodeids = set(self.host_ids).union(set(self.device_ids).union(set(self.switch_ids)))
        

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

        node_colors = [colors[n[0]] for n in self.nodeids]
        
        nx.draw_networkx(self.G,pos=positions,node_color=node_colors,with_labels=True)
        plt.savefig('CXL_Topology.png')
        # nx.drawing.nx_pydot.graphviz_layout(self.G, "CXL_Topology.dot")


if __name__ == '__main__':

    N = CXLNet(num_hosts=2,num_devices=2,num_switches=1)
    N.connect("H0","S0")
    N.connect("H1","S0")
    N.connect("D0","S0")
    N.connect("D1","S0")
    N.draw()