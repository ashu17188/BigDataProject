package org.ashu.spark

import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]) {

    val sc = SparkSession
      .builder
      .appName("wordcount")
      .config("spark.master", "local")
      .getOrCreate()

    val input = sc.sparkContext.textFile("/home/hduser/BigData_Workspace/BigDataProject/resources/input.txt")
    // Split up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into word and count.
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile("/home/hduser/BigData_Workspace/BigDataProject/resources/output.txt")
  }
}