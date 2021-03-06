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
object SparkTri {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkTri")
      .getOrCreate()
    val t0 = System.nanoTime()
    val lines = spark.read.textFile(args(0)).rdd
    var graph = lines.flatMap(edge => {
      val vertices = edge.split(" ").map(_.toInt).sorted
      if (vertices(0) == vertices(1)) List()
      else List((vertices(0), vertices(1)))
    }).distinct.cache

    println(graph.join(graph).keyBy(_._2).join(graph.map(_ -> 1)).count)
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    spark.stop()
  }
}
