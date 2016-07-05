#!/bin/sh

FNAME=ece454750a2b.tar.gz

tar -czf $FNAME ece454/*.java group.txt

echo
echo Your tarball file name is: $FNAME
echo
echo Your group members are: `cat group.txt`
echo
echo Good luck!
echo
