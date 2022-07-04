#!/bin/bash
for core in 8 7 6 5 4 3 2 1
do
    $SPARK_HOME/bin/spark-submit --executor-memory 20g --driver-memory 10g --class "org.apache.spark.examples.SparkPC" --master local[${core}] target/scala-2.12/pc_2.12-1.0.jar /opt/data/pe_opaque_a_107/test_file_0 /opt/data/pe_opaque_b_107/test_file_0 > big_p${core}.txt
done
