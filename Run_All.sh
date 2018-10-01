#!/bin/bash
# This script takes 0-3 arguments
# ./Run_All.sh <Input folder> <Output folder> <Jar>
# If there is nothing specified default values are used

# Default values
input='/P2Sample'
output='/p2output'
jar='./target/PA2-1.0.jar'

# change input
if [ $# -gt 0 ]
  then
    input=$1
fi

# change output
if [ $# -gt 1 ]
  then
    output=$2
fi

# change jar
if [ $# -gt 2 ]
  then
    jar=$3
fi


# Run all jobs at Once by having 2 of them run in the background
# This works but mixes the console outputs
$HADOOP_HOME/bin/hadoop jar ${jar} cs435.josiahm.pa2.drivers.JobOneDriver ${input} ${output}
