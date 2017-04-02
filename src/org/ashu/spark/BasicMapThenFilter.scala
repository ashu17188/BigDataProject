package org.ashu.spark

import org.apache.spark._
import org.apache.spark.sql.SparkSession


object BasicMapThenFilter {
  def main(args: Array[String]) {
    val sc = SparkSession.builder().appName("BasicMapThenFilter").
      config("spark.master", "local").getOrCreate()
    val input = sc.sparkContext.parallelize(List(1, 2, 3, 4))
    val squared = input.map(x => x * x)
    val result = squared.filter(x => x != 1)
    println(result.collect().mkString(","))
  }
}
