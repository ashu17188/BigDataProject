package org.ashu.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object BasicAvg {

  def main(args: Array[String]) {
    val sc = SparkSession.builder().appName("BasicAvg").config("spark.master", "local").getOrCreate()
    val input = sc.sparkContext.parallelize(List(1, 2, 3, 4))
    val result = computeAvg(input)
    val avg = result._1 / result._2.toFloat
    println(result)

  }
  def computeAvg(input: RDD[Int]) = {
    input.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1),
      (x,y) => (x._1 + y._1, x._2 + y._2))
  }
}