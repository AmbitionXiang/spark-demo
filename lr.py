import sys, time
import os
import math
import random
from operator import add

from pyspark.sql import SparkSession
import time

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: logistic regression <file>")
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonLR")\
        .getOrCreate()
    D = 2  # Number of dimensions
    iterations = 3
    # Initialize w to a random value
    w= [2 * random.uniform(0, 1) - 1 for i in range(D)]

    print("Initial w: " + str(w))

    def readPoint(row_str):
        row = [float(i) for i in row_str.split(' ')]
        return row

    def gradient(row, w):
        Y = row[D]    # point labels (first column of input file)
        X = row[:D]   # point coordinates
        # For each point (x, y), compute gradient function, then sum these up
        dot = sum([X[i]*w[i] for i in range(D)])
        return [(1.0 / (1.0 + math.exp(-Y * dot)) - 1.0) * Y * x for x in X]

    def add(x, y):
        x += y
        return x

    # need to time
    print("begin to time")
    tic = time.time()

    lines = spark.read.format("text").load(sys.argv[1]).rdd.map(lambda r: r[0])
    points = lines.map(readPoint)
    for i in range(iterations):
        print("On iteration %i" % (i + 1))
        r = points.map(lambda m: gradient(m, w)).reduce(add)
        w = [w[i] - r[i] for i in range(D)]
    
    toc = time.time()
    print("Logistic regression in %s seconds" % (toc - tic))
    print("Final w: " + str(w))
    # need to time
    spark.stop()
