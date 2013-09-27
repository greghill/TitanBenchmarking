import re
f = open('graph2.rec', 'r')
newFile = []
for line in f:
    m = re.split('\s',line)
    num_edges = len(m)-2
    newLine = m[0] + " " + str(num_edges)
    for nbr in m[1:]:
        newLine += " " + nbr
    newFile.append(newLine + "\n")
f.close()
f = open('graph2.rec', 'w')
for line in newFile:
    f.write(line)
f.close();
