#!/bin/bash

export CLASSPATH=".:build:lib/*"

echo Server 1 output redirected to server1.out
timeout 1h java ece454.StorageNode localhost 5567 localhost:2181 /$USER &> server1.out &

echo Server 2 output redirected to server2.out
timeout 1h java ece454.StorageNode localhost 6678 localhost:2181 /$USER &> server2.out &
