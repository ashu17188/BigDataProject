package org.ashu.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession

object BasicTextFromFTP {
  def main(args: Array[String]) {
    val sc = SparkSession.builder().appName("BasicTextFromFTP").
      config("spark.master", "local").getOrCreate()
    val file = sc.sparkContext.textFile("ftp://user:pwd/192.168.1.5/brecht-d-m/map/input.nt")
    println(file.collect().mkString("\n"))
  }
}
