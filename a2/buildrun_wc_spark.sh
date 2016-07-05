#!/bin/bash

#export JAVA_TOOL_OPTIONS=-Xmx1g
SCALA_HOME=/opt/scala-2.10.6
SPARK_HOME=/opt/spark-1.6.1-bin-hadoop2.6
export CLASSPATH=.:"$SPARK_HOME/lib/*"

echo --- Deleting
rm $1.jar
rm ece454/$1*.class

echo --- Compiling
$SCALA_HOME/bin/scalac ece454/$1.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
jar -cf $1.jar ece454/$1*.class

echo --- Running
INPUT=sample_input
OUTPUT=output_spark

rm -fr $OUTPUT
$SPARK_HOME/bin/spark-submit --master "local[*]" --class ece454.$1 $1.jar $INPUT $OUTPUT

cat $OUTPUT/*
