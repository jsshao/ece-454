#!/bin/bash

JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk/
SPARK_HOME=/opt/spark-1.6.1-bin-hadoop2.6
export CLASSPATH=.:"$SPARK_HOME/lib/*"

echo --- Deleting
rm *.jar
rm ece454/*.class

echo --- Compiling
$JAVA_HOME/bin/javac ece454/*.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf myCC.jar ece454/*.class

echo --- Running
#INPUT=input/huge.txt
#INPUT=input/large.txt
#INPUT=input/medium.txt
#INPUT=input/small.txt
INPUT=input/tiny.txt
NUMTHREADS=4

rm -fr output
time $JAVA_HOME/bin/java -Xmx1g -cp myCC.jar ece454.CC $NUMTHREADS $INPUT ans_java
