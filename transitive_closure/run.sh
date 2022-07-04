#!/bin/bash
for core in 8 7 6 5 4 3 2 1
do
    $SPARK_HOME/bin/spark-submit --executor-memory 20g --driver-memory 10g --class "org.apache.spark.examples.SparkTC" --master local[${core}] target/scala-2.12/tc_2.12-1.0.jar /opt/data/tc_opaque_fb/facebook_combined.txt > big_p${core}.txt
done
