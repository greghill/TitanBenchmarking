import networkx
import random
num_nodes = 10000
num_reachability = 100
g = networkx.fast_gnp_random_graph(num_nodes, .001, 0, True)


# make graph
f = open('graph2.rec', 'w')
for node in g.nodes():
    toPrint = str(node)
    for nbr in g.neighbors(node):
        toPrint += " " + str(nbr)

    f.write(toPrint + '\n')
f.close()
# make paths
f = open('requests2.rec', 'w')
for i in range(0, num_reachability):
    source = random.randint(0, num_nodes-1);
    sink = random.randint(0, num_nodes-1);
    while source == sink:
        sink = random.randint(0, num_nodes-1);

    # reachability
    toWrite = str(source)+" " + str(sink)
    has_path = networkx.has_path(g, source, sink)
    toWrite += " "+ str(has_path)
    # 2 hop paths
    has_n_hop = False
    for path in networkx.all_simple_paths(g, source, sink, cutoff=2):
        has_n_hop = True
    toWrite += " "+ str(has_n_hop)
    # 4 hop paths
    has_n_hop = False
    for path in networkx.all_simple_paths(g, source, sink, cutoff=4):
        has_n_hop = True
    toWrite += " "+ str(has_n_hop)
    f.write(toWrite + '\n')
f.close()
