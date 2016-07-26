#!/bin/bash

export CLASSPATH=".:build:lib/*"

#java ece454.A3Client localhost:2181 /$USER 8 10000 100
#java ece454.A3Client ecelinux3:10420 /$USER 8 100000 100
java ece454.A3Client ecelinux3:10420 /$USER 32 100000 100
