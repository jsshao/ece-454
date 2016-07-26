#!/bin/bash

export CLASSPATH=".:build:lib/*"

echo Server 1 output redirected to server1.out
timeout 1h java ece454.StorageNode ecelinux3 10421 ecelinux3:10420 /$USER &> server1.out &

echo Server 2 output redirected to server2.out
timeout 1h java ece454.StorageNode ecelinux3 10422 ecelinux3:10420 /$USER &> server2.out &
