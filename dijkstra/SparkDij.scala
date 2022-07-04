/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.sql.SparkSession

/**
 * Dijkstra
 */
object SparkDij extends Serializable {
  def customSplitNodesTextFile(line: String): (Int, (Int, Option[Vector[String]], String)) = {
    val parts = line.split("\\s+")
    if (parts.length < 3) {
      val nid = parts(0)
      val distance = parts(1).toInt
      (nid.toInt, (distance, None, nid))
    } else {
      val nid = parts(0)
      val distance = parts(1).toInt
      val tmp = parts(2)
      var neighbors = tmp.split(':').toVector.dropRight(1)
      (nid.toInt, (distance, Some(neighbors), nid))
    }
  }

  def customSplitNodesIterative(node: (Int, (Int, Option[Vector[String]], String))): (Int, (Int, Option[Vector[String]], String)) = {
    var (nid, (distance, neighbors, path)) = node
    val elements = path.split("->")
    if (elements.last.toInt != nid) {
      path = path + "->" + nid
    }
    return (nid, (distance, neighbors, path))
  }

  def customSplitNeighbor(parentPath: String, parentDistance: Int, neighbor: String): (Int, (Int, Option[Vector[String]], String)) = {
    val parts = neighbor.split(',')
    val nid = parts(0)
    var distance = parts(1).toInt
    distance = parentDistance + distance
    val path = parentPath + "->" + nid
    return (nid.toInt, (distance, None, path))
  }

  def minDistance(nodeValue1: (Int, Option[Vector[String]], String), nodeValue2: (Int, Option[Vector[String]], String)): (Int, Option[Vector[String]], String) = {
    var neighbors: Option[Vector[String]] = None
    var distance = 0
    var path = ""
    if (!nodeValue1._2.isEmpty) {
      neighbors = nodeValue1._2
    } else {
      neighbors = nodeValue2._2
    }
    val dist1 = nodeValue1._1
    val dist2 = nodeValue2._1
    if (dist1 <= dist2) {
      distance = dist1
      path = nodeValue1._3
    } else {
      distance = dist2
      path = nodeValue2._3
    }
    return (distance, neighbors, path)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkDij")
      .getOrCreate()
    val t0 = System.nanoTime()
    val lines = spark.read.textFile(args(0)).rdd
    var nodes = lines.map{ n => customSplitNodesTextFile(n) }

    val func0 = (accu: Int, node: (Int, (Int, Option[Vector[String]], String))) => accu + node._2._1
    val func1 = (accu1: Int, accu2: Int) => accu1 + accu2 
    var oldCount = nodes.aggregate(0)(func0, func1)
    var newCount = oldCount
    var iterations = 0

    do {
      oldCount = newCount
      iterations += 1
      val mapper = nodes.flatMap { n =>
        val (nid, (data0, data1, data2)) = n
        var res = Vector.empty :+ n
        if (!data1.isEmpty) {
          val d = data1.get
          res = res ++ d.map { neighbor => customSplitNeighbor(data2, data0, neighbor) }.toVector
        }
        res.iterator
      }
      val reducer = mapper.reduceByKey { (x, y) => minDistance(x, y) }
      nodes = reducer.map { node => customSplitNodesIterative(node) }
      nodes.cache()
      newCount = nodes.aggregate(0)(func0, func1)
    } while (oldCount != newCount && iterations < 5)
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    println(s"Dij res: ${newCount} ")
    spark.stop()
  }
}
