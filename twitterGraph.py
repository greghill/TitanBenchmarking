import networkx
import re
num_nodes = 10000
num_reachability = 100
g = networkx.Graph()


# make graph
f = open('../twitter_rv.net', 'r')

for line in f:
    m = re.split('\s+', line)
    for node in m:
        g.add_node(node)
f.close()
