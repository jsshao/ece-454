#!/bin/bash

SCALA_HOME=/opt/scala-2.11.6
SPARK_HOME=/opt/spark-1.6.1-bin-hadoop2.6
export CLASSPATH=.:"$SPARK_HOME/lib/*"

echo --- Deleting
rm *.jar
rm ece454/*.class

echo --- Compiling
$SCALA_HOME/bin/scalac ece454/*.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
jar -cf SparkCC.jar ece454/*.class

echo --- Running
#INPUT=input/huge.txt
#INPUT=input/large.txt
#INPUT=input/medium.txt
#INPUT=input/small.txt
INPUT=input/tiny.txt

rm -fr output ans_spark
ulimit -u 100
time timeout 1h $SPARK_HOME/bin/spark-submit --driver-memory 2g --master "local[1]" --class ece454.SparkCC SparkCC.jar $INPUT output

cat output/part* > ans_spark
