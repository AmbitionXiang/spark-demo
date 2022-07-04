#!/bin/bash
for core in 8 7 6 5 4 3 2 1
do
    $SPARK_HOME/bin/spark-submit --executor-memory 20g --driver-memory 10g --class "org.apache.spark.examples.SparkMM" --master local[${core}] target/scala-2.12/mm_2.12-1.0.jar /opt/data/mm_opaque_a_2000_20/test_file_0 /opt/data/mm_opaque_b_20_2000/test_file_0 > big_p${core}.txt
done
