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
 * Matrix Multiplication
 */
object SparkMM {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkMM")
      .getOrCreate()
    val t0 = System.nanoTime()
    val ma = spark.read.textFile(args(0)).rdd.map{ s =>
      val parts = s.split("\\s+")
      (parts(1).toInt, (parts(0).toInt, parts(2).toDouble))
    }
    val mb = spark.read.textFile(args(1)).rdd.map{ s =>
      val parts = s.split("\\s+")
      (parts(0).toInt, (parts(1).toInt, parts(2).toDouble))
    }

    val tmp = ma.join(mb).map{ n => ((n._2._1._1, n._2._2._1), n._2._1._2 * n._2._2._2) }
    val mc = tmp.reduceByKey(_ + _)
    val count = mc.count()

    val t1 = System.nanoTime()
    println("Count: " + count + ", Elapsed time: " + (t1 - t0) + "ns")
    spark.stop()
  }
}
