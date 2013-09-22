import networkx
import random
num_nodes = 5000
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

    f.write(str(source)+" " + str(sink) + " "+ str(networkx.has_path(g, source, sink)) + '\n')
f.close()
'''
import random
f = open('graph2.rec', 'w')
nodes = 1000
edges = 100000
adjList = dict()
for i in range(0, nodes):
    adjList[i] = set()

for i in range(0, edges):
    while True:
        source = random.randint(0, nodes-1);
        sink = random.randint(0, nodes-1);
        if source != sink and sink not in adjList[source]:
            adjList[source] = adjList[source].union(set([sink]))
            break

for source, sinks in adjList.iteritems():
    toPrint = str(source)
    for nbr in sinks:
        toPrint += " " + str(nbr)

    f.write(toPrint + '\n')
f.close()
'''
