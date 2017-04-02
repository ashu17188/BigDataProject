package org.ashu.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession

object BasicFilterUnionCombo {
  def main(args: Array[String]) {
    val sc = SparkSession.builder().appName("BasicFilterUnionCombo").
      config("spark.master", "local").getOrCreate()
    val inputRDD = sc.sparkContext.textFile("/home/hduser/BigData_Workspace/BigDataProject/resources/data/BasicFilterUnionCombo.txt")
    val errorsRDD = inputRDD.filter(_.contains("error"))
    val warningsRDD = inputRDD.filter(_.contains("warn"))
    val badLinesRDD = errorsRDD.union(warningsRDD)
    println(badLinesRDD.collect().mkString("\n"))
  }
}
