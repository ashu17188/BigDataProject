package org.ashu.spark

import org.apache.spark._
import org.apache.spark.SparkContext._

object BasicSaveSequenceFile {
    def main(args: Array[String]) {
      val master = "local"
      val outputFile = "/home/hduser/BigData_Workspace/BigDataProject/resources/BasicSaveSequenceFileOutput.txt"
      val sc = new SparkContext(master, "BasicSaveSequenceFile", System.getenv("SPARK_HOME"))
      val data = sc.parallelize(List(("Holden", 3), ("Kay", 6), ("Snail", 2)))
      data.saveAsSequenceFile(outputFile)
    }
}
