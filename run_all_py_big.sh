#/bin/bash

$SPARK_HOME/bin/spark-submit --executor-memory 20g --driver-memory 10g --master local[1] dij.py /opt/data/true_data/processed-CA.txt &> output_dij.txt
$SPARK_HOME/bin/spark-submit --executor-memory 20g --driver-memory 10g --master local[1] km.py /opt/data/km_opaque_41065/test_file_0 10 0.3  &> output_km.txt
$SPARK_HOME/bin/spark-submit --executor-memory 20g --driver-memory 10g --master local[1] lr.py /opt/data/lr_opaque_51072/test_file_0 &> output_lr.txt
$SPARK_HOME/bin/spark-submit --executor-memory 20g --driver-memory 10g --master local[1] mm.py /opt/data/mm_opaque_a_2000_20/test_file_0 /opt/data/mm_opaque_b_20_2000/test_file_0  &> output_mm.txt
$SPARK_HOME/bin/spark-submit --executor-memory 20g --driver-memory 10g --master local[1] pc.py /opt/data/pe_opaque_a_107/test_file_0 /opt/data/pe_opaque_b_107/test_file_0  &> output_pc.txt
$SPARK_HOME/bin/spark-submit --executor-memory 20g --driver-memory 10g --master local[1] pr.py /opt/data/pr_opaque_cit-Patents/test_file_0 1 &> output_pr.txt
$SPARK_HOME/bin/spark-submit --executor-memory 20g --driver-memory 10g --master local[1] tc.py /opt/data/true_data/facebook_combined.txt &> output_tc.txt
$SPARK_HOME/bin/spark-submit --executor-memory 20g --driver-memory 10g --master local[1] tri.py /opt/data/true_data/soc-Slashdot0811.txt &> output_tri.txt