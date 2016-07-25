#!/bin/bash

export CLASSPATH=".:build:lib/*"

java ece454.StorageNode ecelinux1 10422 ecelinux3:10420 /$USER
