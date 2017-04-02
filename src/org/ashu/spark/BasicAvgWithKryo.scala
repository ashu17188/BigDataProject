package org.ashu.spark

import org.apache.spark._
import org.apache.spark.sql.SparkSession

object BasicAvgWithKryo {
  def main(args: Array[String]) {

    val sc = SparkSession.builder().appName("basicAvgWithKryo").
      config("spark.master", "local").getOrCreate()
    val input = sc.sparkContext.parallelize(List(1, 2, 3, 4))
    val result = input.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1),
      (x, y) => (x._1 + y._1, x._2 + y._2))
    val avg = result._1 / result._2.toFloat
    println(result)
  }
}
