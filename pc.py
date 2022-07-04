import sys, time
import os
import math
import random
from operator import add

from pyspark.sql import SparkSession
import time

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pearson-correlation <file1> <file2>")
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonPC")\
        .getOrCreate()

    print("begin to time")
    tic = time.time()

    x = spark.read.format("text").load(sys.argv[1]).rdd.map(lambda r: r[0])\
        .map(lambda x: float(x))

    y = spark.read.format("text").load(sys.argv[2]).rdd.map(lambda r: r[0])\
        .map(lambda x: float(x))
    
    x_count = x.count()
    mx = x.reduce(lambda a,b: a+b)/x_count
    y_count = y.count()
    my = y.reduce(lambda a,b: a+b)/y_count

    def comp(p):
        up = (p[0] - mx) * (p[1] - my)
        lowx = (p[0] - mx) * (p[0] - mx)
        lowy = (p[1] - my) * (p[1] - my)
        return (up, lowx, lowy)

    r = x.zip(y)\
        .map(comp)\
        .reduce(lambda a,b: (a[0]+b[0], a[1]+b[1], a[2]+b[2]))
    print("r: ", r[0]/(math.sqrt(r[1])*math.sqrt(r[2])))
    
    # need to time
    toc = time.time()
    print("pearson correlation in %s seconds" % (toc - tic))
    spark.stop()
