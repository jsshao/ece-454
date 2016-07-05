#!/bin/sh

#export JAVA_TOOL_OPTIONS=-Xmx1g
export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.101-2.6.6.1.el7_2.x86_64
export HADOOP_HOME=/opt/hadoop-2.7.2
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

echo --- Deleting
rm $1.jar
rm ece454/$1*.class

echo --- Compiling
$JAVA_HOME/bin/javac ece454/$1.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
jar -cf $1.jar ece454/$1*.class

echo --- Running
INPUT=sample_input
OUTPUT=output_hadoop

rm -fr $OUTPUT
$HADOOP_HOME/bin/hadoop jar $1.jar ece454.$1 $INPUT $OUTPUT

cat $OUTPUT/*
