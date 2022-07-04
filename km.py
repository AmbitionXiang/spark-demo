import sys, time
import os
import math
import random
from operator import add

from pyspark.sql import SparkSession
import time

def parseVector(line):
    return [float(x) for x in line.split(' ')]

def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")
    dim = len(p)
    for i in range(len(centers)):
        tempDist = squaredDistance(p, centers[i])
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex

def squaredDistance(v1, v2):
    dim = len(v1)
    return sum([(v1[j] - v2[j]) ** 2 for j in range(dim)])
    
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: kmeans <file> <k> <convergeDist>")
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonKM")\
        .getOrCreate()

    print("begin to time")
    tic = time.time()

    lines = spark.read.format("text").load(sys.argv[1]).rdd.map(lambda r: r[0])
    data = lines.map(parseVector).cache()
    K = int(sys.argv[2])
    convergeDist = float(sys.argv[3])

    kPoints = data.takeSample(False, K, 1)
    tempDist = 1.0
    it = 0
    while tempDist > convergeDist and it < 5:
        print("cur iteration"+str(it))
        closest = data.map(
            lambda p: (closestPoint(p, kPoints), (p, 1)))
        pointStats = closest.reduceByKey(
            lambda p1_c1, p2_c2: ([p1_c1[0][i] + p2_c2[0][i] for i in range(len(p1_c1[0]))], p1_c1[1] + p2_c2[1]))
        newPoints = pointStats.map(
            lambda st: (st[0], [x/st[1][1] for x in st[1][0]])).collect()

        tempDist = sum(squaredDistance(kPoints[iK], p) for (iK, p) in newPoints)

        for (iK, p) in newPoints:
            kPoints[iK] = p
        it += 1
    
    toc = time.time()
    print("Kmeans in %s seconds" % (toc - tic))
    print("Final centers: " + str(kPoints))
    # need to time
    spark.stop()
