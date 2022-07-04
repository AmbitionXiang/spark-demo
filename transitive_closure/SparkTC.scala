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
 * Transitive closure on a graph.
 */
object SparkTC {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkTC")
      .getOrCreate()
    val t0 = System.nanoTime()
    val lines = spark.read.textFile(args(0)).rdd
    var tc = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.cache()

    // Linear transitive closure: each round grows paths by one edge,
    // by joining the graph's edges with the already-discovered paths.
    // e.g. join the path (y, z) from the TC with the edge (x, y) from
    // the graph to obtain the path (x, z).

    // Because join() joins on keys, the edges are stored in reversed order.
    val edges = tc.map(x => (x._2, x._1))

    // This join is iterated until a fixed point is reached.
    var oldCount = 0L
    var nextCount = tc.count()
    var iter = 0L
    do {
      oldCount = nextCount
      // Perform the join, obtaining an RDD of (y, (z, x)) pairs,
      // then project the result to obtain the new (x, z) paths.
      tc = tc.union(tc.join(edges).map(x => (x._2._2, x._2._1))).distinct().cache()
      nextCount = tc.count()
      println(s"oldCount: ${oldCount}, nextCount: ${nextCount}")
      val t2 = System.nanoTime()
      println("Elapsed time: " + (t2 - t0) + "ns")
      iter += 1
    } while (nextCount != oldCount && iter < 5)
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    println(s"TC has ${tc.count()} edges.")
    spark.stop()
  }
}
