package org.ashu.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession

object BasicLoadNums {
  def main(args: Array[String]) {
    val sc = SparkSession
      .builder
      .appName("BasicLoadNums")
      .config("spark.master", "local")
      .getOrCreate()
    val file = sc.sparkContext.textFile("/home/hduser/BigData_Workspace/BigDataProject/resources/data/num.txt")
//      val file = sc.sparkContext.parallelize(List(1,2,3,4,5))
      val errorLines = sc.sparkContext.accumulator(0) // Create an Accumulator[Int] initialized to 0
    val dataLines = sc.sparkContext.accumulator(0) // Create a second Accumulator[Int] initialized to 0
    val counts = file.flatMap(line => {
      try {
        val input = line.split(" ")
        val data = Some((input(0), input(1).toInt))
        dataLines += 1
        data
      } catch {
        case e: java.lang.NumberFormatException => {
          errorLines += 1
          None
        }
        case e: java.lang.ArrayIndexOutOfBoundsException => {
          errorLines += 1
          None
        }
      }
    }).reduceByKey(_ + _)
    if (errorLines.value < 0.1 * dataLines.value) {
      counts.saveAsTextFile("/home/hduser/BigData_Workspace/BigDataProject/resources/output1.txt")
    } else {
      println(s"Too many errors ${errorLines.value} for ${dataLines.value}")
    }
  }
}
