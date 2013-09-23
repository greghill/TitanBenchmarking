import networkx
import re
num_nodes = 10000
num_reachability = 100
g = networkx.Graph()


count = 0
# make graph
f = open('../twitter_rv.net', 'r')

for line in f:
    m = re.split('\s+', line)
    count += 1
    for node in m:
        g.add_node(node)
    if (count % 1400000 == 0):
        print count/14000000. 
f.close()
