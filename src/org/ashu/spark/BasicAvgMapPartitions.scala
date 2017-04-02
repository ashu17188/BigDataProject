package org.ashu.spark

import org.apache.spark._
import org.apache.spark.sql.SparkSession

object BasicAvgMapPartitions {
  case class AvgCount(var total: Int = 0, var num: Int = 0) {
    def merge(other: AvgCount): AvgCount = {
      total += other.total
      num += other.num
      this
    }
    def merge(input: Iterator[Int]): AvgCount = {
      input.foreach { elem =>
        total += elem
        num += 1
      }
      this
    }
    def avg(): Float = {
      total / num.toFloat;
    }
  }

  def main(args: Array[String]) {
    val sc = SparkSession
      .builder
      .appName("BasicAvgMapPartitions")
      .config("spark.master", "local")
      .getOrCreate()
    val input = sc.sparkContext.parallelize(List(1, 2, 3, 4))
    val result = input.mapPartitions(partition =>
      // Here we only want to return a single element for each partition, but mapPartitions requires that we wrap our return in an Iterator
      Iterator(AvgCount(0, 0).merge(partition)))
      .reduce((x, y) => x.merge(y))
    println(result)
  }
}
