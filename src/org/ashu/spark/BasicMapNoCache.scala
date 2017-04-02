package org.ashu.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object BasicMapNoCache {
  def main(args: Array[String]) {
    val sc = SparkSession.builder().appName("BasicMapNoCache").config("spark.master", "local").
      getOrCreate()
    val input = sc.sparkContext.parallelize(List(1, 2, 3, 4))
    val result = input.map(x => x * x)
    // will compute result twice
    println(result.count())
    println(result.collect().mkString(","))
  }
}
