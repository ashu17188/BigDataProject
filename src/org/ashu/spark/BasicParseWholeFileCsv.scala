package org.ashu.spark

import java.io.StringReader

import org.apache.spark._
import play.api.libs.json._
import scala.util.parsing.json.JSON
import scala.collection.JavaConversions._
import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.sql.SparkSession

object BasicParseWholeFileCsv {
  def main(args: Array[String]) {
    val sc = SparkSession.builder().appName("BasicParseWholeFileCsv").
      config("spark.master", "local").getOrCreate()

    val inputFile = "/home/hduser/BigData_Workspace/BigDataProject/resources/data/"
    val input = sc.sparkContext.wholeTextFiles(inputFile)
    val result = input.flatMap {
      case (_, txt) =>
        val reader = new CSVReader(new StringReader(txt));
        reader.readAll()
    }
    println(result.collect().map(_.toList).mkString(","))
  }
}
