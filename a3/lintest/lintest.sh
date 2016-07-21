#!/bin/bash

export CLASSPATH=".:build:lib/*"

echo Cleaning up
rm -fr build/test/*

echo Building
javac -d build ece454/test/*.java
if [ $? -ne 0 ]; then
    exit
fi

echo Running
java ece454.test.LinearizabilityTest execution.log scores.txt

echo Done
#echo Top of scores.txt file:
#head scores.txt
echo Set of distinct scores observed in scores.txt:
cat scores.txt | cut -d' ' -f9 | sort -u
echo "Explanation: "
echo "0 means OK"
echo "1 means linearizability violation (bug!)"
echo "2 means a get operation returned a value that was never assigned by a put operation (bug!)"
