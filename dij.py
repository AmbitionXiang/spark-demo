import sys, time
import os
from operator import add

from pyspark.sql import SparkSession
import time

def custom_split_nodes_text_file(line):
    parts = line.split(' ')
    if (len(parts) < 3):
        nid = parts[0]
        distance = int(parts[1])
        return (int(nid), (distance, [], nid))
    else:
        nid = parts[0]
        distance = int(parts[1])
        tmp = parts[2]
        neighbors = tmp.split(':')[:-1]
        return (int(nid), (distance, neighbors, nid))

def custom_split_nodes_iterative(node):
    (nid, (distance, neighbors, path)) = node
    elements = path.split('->')
    if (int(elements[-1]) != nid):
        path = path + "->" + str(nid)
    
    return (nid, (distance, neighbors, path))

def custom_split_neighbor(parent_path, parent_distance, neighbor):
    parts = neighbor.split(',')
    nid = parts[0]
    distance = int(parts[1])
    distance = parent_distance + distance
    path = parent_path + "->" + nid
    return (int(nid), (distance, [], path))

def min_distance(node_value1, node_value2):
    neighbors = []
    distance = 0
    path = ""
    if (len(node_value1[1])>0):
        neighbors = node_value1[1]
    else:
        neighbors = node_value2[1]
    dist1 = node_value1[0]
    dist2 = node_value2[0]
    if (dist1 <= dist2):
        distance = dist1
        path = node_value1[2]
    else:
        distance = dist2
        path = node_value2[2]
    return (distance, neighbors, path)

def func(node):
    (nid, (data0, data1, data2)) = node
    res = [node]
    if (len(data1) != 0):
        res += [custom_split_neighbor(data2, data0, neighbor)  for neighbor in data1]
    return res

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
    
    nodes = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])\
        .map(custom_split_nodes_text_file)

    oldCount = nodes.aggregate(0, lambda acc, node: acc+node[1][0], lambda acc1, acc2: acc1+acc2)
    nextCount = oldCount
    it = 0
    while (it == 0 or nextCount != oldCount) and it < 5:
        oldCount = nextCount
        mapper = nodes.flatMap(func)
        reducer = mapper.reduceByKey(lambda x, y: min_distance(x, y))
        nodes = reducer.map(custom_split_nodes_iterative)\
            .cache()
        nextCount = nodes.aggregate(0, lambda acc, node: acc+node[1][0], lambda acc1, acc2: acc1+acc2)
        it += 1

    toc = time.time()
    print("Dijkstra in %s seconds" % (toc - tic))
    print("Dij res: %i" % nextCount)

    # need to time
    spark.stop()
