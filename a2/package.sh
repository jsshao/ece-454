#!/bin/sh

FNAME=ece454750a2a.tar.gz

tar -czf $FNAME ece454/Task?.java ece454/Task?.scala group.txt

echo
echo Your tarball file name is: $FNAME
echo
echo Your group members are: `cat group.txt`
echo
echo Good luck!
echo
