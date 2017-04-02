package org.ashu.spark

import org.apache.spark._
import org.apache.spark.sql.SparkSession

object BasicMap {
  def main(args: Array[String]) {

    val sc = SparkSession.builder().appName("BasicMap").config("spark.master", "local").
      getOrCreate()
    val input = sc.sparkContext.parallelize(List(1, 2, 3, 4))
    val result = input.map(x => x * x)
    println(result.collect().mkString(","))
  }
}
