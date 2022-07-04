#/bin/bash

$SPARK_HOME/bin/spark-submit --master local[1] dij.py /opt/data/dij_opaque_1/test_file_0 &> output_dij.txt
$SPARK_HOME/bin/spark-submit --master local[1] km.py /opt/data/km_opaque_50000_5/test_file_0 10 0.3  &> output_km.txt
$SPARK_HOME/bin/spark-submit --master local[1] lr.py /opt/data/lr_opaque_small/test_file_0 &> output_lr.txt
$SPARK_HOME/bin/spark-submit --master local[1] mm.py /opt/data/mm_opaque_a_100/test_file_0 /opt/data/mm_opaque_b_100/test_file_0  &> output_mm.txt
$SPARK_HOME/bin/spark-submit --master local[1] pc.py /opt/data/pe_opaque_a_105/test_file_0 /opt/data/pe_opaque_b_105/test_file_0  &> output_pc.txt
$SPARK_HOME/bin/spark-submit --master local[1] pr.py ../sgx-pyspark/sgx-pyspark-demo/input/pr_opaque.txt 1 &> output_pr.txt
$SPARK_HOME/bin/spark-submit --master local[1] tc.py /opt/data/tc_opaque_2/test_file_0 &> output_tc.txt
$SPARK_HOME/bin/spark-submit --master local[1] tri.py /opt/data/tc_opaque_7/test_file_0 &> output_tri.txt