import sys, time
import os
from operator import add

from pyspark.sql import SparkSession
import time

def parseEdge(line):
    parts = line.split(' ')
    return (int(parts[0]), int(parts[1]))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: transitive closure <file>")
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonTC")\
        .getOrCreate()

    print("begin to time")
    tic = time.time()

    tc = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])\
        .map(parseEdge)\
        .cache()

    edges = tc.map(lambda x_y: (x_y[1], x_y[0]))

    oldCount = 0
    nextCount = tc.count()
    it = 0
    while nextCount != oldCount and it < 5:
        oldCount = nextCount
        # Perform the join, obtaining an RDD of (y, (z, x)) pairs,
        # then project the result to obtain the new (x, z) paths.
        new_edges = tc.join(edges).map(lambda __a_b: (__a_b[1][1], __a_b[1][0]))
        tc = tc.union(new_edges).distinct().cache()
        nextCount = tc.count()
        it += 1

    print("TC has %i edges" % tc.count())
    toc = time.time()
    print("transitive closure in %s seconds" % (toc - tic))

    # need to time
    spark.stop()

