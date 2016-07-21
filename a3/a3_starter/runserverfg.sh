#!/bin/bash

export CLASSPATH=".:build:lib/*"

java ece454.StorageNode localhost 5567 localhost:2181 /$USER
