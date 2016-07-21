#!/bin/bash

export CLASSPATH=".:build:lib/*"

echo Cleaning up
rm -fr build/*

echo Building
thrift --gen java a3.thrift
if [ $? -ne 0 ]; then
    exit
fi
javac -d build gen-java/ece454/*.java
if [ $? -ne 0 ]; then
    exit
fi
javac -d build ece454/*.java
if [ $? -ne 0 ]; then
    exit
fi

echo Done
