d1 = {}
d2 = {}
with open('ans_java') as f1:
    for line in f1:
        a, b = line.split()
        d1[a] = b
    
with open('ans_spark') as f2:
    for line in f2:
        a, b = line.split()
        d2[a] = b

assert len(d1) == len(d2)

s1 = set(d1.values())
for k in d1:
    if k == d1[k]:
        s1.remove(d1[k])
assert not s1

s2 = set(d2.values())
for k in d2:
    if k == d2[k]:
        s2.remove(d2[k])
assert not s2

d12 = {}
d21 = {}
for k in d1:
    if d1[k] in d12 or d2[k] in d21:
        assert d1[k] in d12 and d2[k] in d21
        assert d12[d1[k]] == d2[k] and d21[d2[k]] == d1[k]
    else:
        d12[d1[k]] = d2[k]
        d21[d2[k]] = d1[k]

assert len(d12) == len(d21)


print('success')
