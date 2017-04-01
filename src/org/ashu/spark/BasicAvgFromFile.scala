package org.ashu.spark

import org.apache.spark.sql.SparkSession

/**
 * 
 */
object BasicAvgFromFile {
  def main(args: Array[String]) {

    val sc = SparkSession
      .builder
      .appName("BasicAvgFromFile")
      .config("spark.master", "local")
      .getOrCreate()
    //  val sc = new SparkContext(master, "BasicAvg", System.getenv("SPARK_HOME"))
//    val input = sc.sparkContext.textFile("/home/hduser/BigData_Workspace/BigDataProject/resources/data/num.txt")
      val input = sc.sparkContext.parallelize(List(1,2,3,4,5,6))
      val result = input.map(_.toInt).aggregate((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val avg = result._1 / result._2.toFloat
    println(result)
  }
}
