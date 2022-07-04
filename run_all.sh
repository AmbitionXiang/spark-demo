#/bin/bash

for i in triangle_counting transitive_closure pearson_correlation pagerank matrix_multiplication logistic_regression kmeans dijkstra
do
    cd $i
    ./run.sh
    cd ..
done