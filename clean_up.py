import re
f = open('graph2.rec', 'r')
set1 = set()
set2 = set()
for line in f:
    m = re.split('\s',line)
    set1.add(m[0])
    for num in m:
        set2.add(num)

print set2.difference(set1)
