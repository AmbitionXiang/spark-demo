#!/bin/bash
for core in 8 7 6 5 4 3 2 1
do
    $SPARK_HOME/bin/spark-submit --executor-memory 20g --driver-memory 10g --class "org.apache.spark.examples.SparkKMeans" --master local[${core}] target/scala-2.12/kmeans_2.12-1.0.jar /opt/data/km_opaque_41065/test_file_0 10 0.3 > big_p${core}.txt
done
