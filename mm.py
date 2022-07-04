import sys, time
import os
import math
import random
from operator import add

from pyspark.sql import SparkSession
import time

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: matrix-multiplication <file1> <file2>")
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonMM")\
        .getOrCreate()

    print("begin to time")
    tic = time.time()

    def readMatrixA(row_str):
        t = row_str.split(' ')
        row = (int(t[1]), (int(t[0]), float(t[2])))
        return row

    def readMatrixB(row_str):
        t = row_str.split(' ')
        row = (int(t[0]), (int(t[1]), float(t[2])))
        return row

    ma = spark.read.format("text").load(sys.argv[1]).rdd.map(lambda r: r[0])\
        .map(readMatrixA)

    mb = spark.read.format("text").load(sys.argv[2]).rdd.map(lambda r: r[0])\
        .map(readMatrixB)
    # (int, ((int, float), (int, float)))
    temp = ma.join(mb).map(lambda x: ((x[1][0][0], x[1][1][0]), x[1][0][1] * x[1][1][1]))
    mc = temp.reduceByKey(lambda x,y: x+y)
    count = mc.count()

    toc = time.time()
    print("Matrix Multiplication in %s seconds" % (toc - tic))

    print("Count: " + str(count))
    # need to time
    spark.stop()
