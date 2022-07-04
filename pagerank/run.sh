#!/bin/bash
for core in 8 7 6 5 4 3 2 1
do
    $SPARK_HOME/bin/spark-submit --executor-memory 20g --driver-memory 10g  --class "org.apache.spark.examples.SparkPageRank" --master local[${core}] target/scala-2.12/page-rank_2.12-1.0.jar /opt/data/pr_opaque_cit-Patents/test_file_0 1 > big_p${core}.txt
done
