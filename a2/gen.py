from __future__ import print_function
from random import randrange

for i in range(1000):
    print('t'+str(i), end='')
    for i in range(1000):
        print(',', end='')
        r=randrange(0,6)
        print(r if r > 0 else '', end='')
    print()
