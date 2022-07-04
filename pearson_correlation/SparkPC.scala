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
import scala.math.sqrt
import scala.util.Random

import org.apache.spark.sql.SparkSession

/**
 * Pearson correlation
 */
object SparkPC {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkPC")
      .getOrCreate()

    val t0 = System.nanoTime()

    val x = spark.read.textFile(args(0)).rdd.map(_.toDouble)
    val y = spark.read.textFile(args(1)).rdd.map(_.toDouble)
    val mx = x.reduce(_ + _)/x.count()
    val my = y.reduce(_ + _)/y.count()
    val r = x.zipPartitions(y) { (rddIter1,rddIter2) => {
        var result = List[(Double, Double)]()
        while(rddIter1.hasNext && rddIter2.hasNext) {
          result ::= (rddIter1.next(), rddIter2.next())
        }
        result.iterator 
      }
    }.map{ p => 
      val up = (p._1 - mx) * (p._2 - my);
      val lowx = (p._1 - mx) * (p._1 - mx);
      val lowy = (p._2 - my) * (p._2 - my);
      (up, lowx, lowy)
    }.reduce{ case ((a1, b1, c1), (a2, b2, c2)) => (a1 + a2, b1 + b2, c1 + c2) }
    
    println("r = " + r._1 / (sqrt(r._2) * sqrt(r._3)))
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    spark.stop()
  }
}
