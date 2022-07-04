#!/bin/bash
for core in 1 2 3 4 5 6 7 8
do
    $SPARK_HOME/bin/spark-submit --executor-memory 20g --driver-memory 10g --class "org.apache.spark.examples.SparkDij" --master local[${core}] target/scala-2.12/dij_2.12-1.0.jar /opt/data/dij_opaque_CA/processed-CA.txt > big_p${core}.txt
done
