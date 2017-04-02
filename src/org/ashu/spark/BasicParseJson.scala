package org.ashu.spark

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import play.api.libs.json.Json

object BasicParseJson {
/*  case class Person(name: String, lovesPandas: Boolean)

  implicit val personReads = Json.format[Person]

  def main(args: Array[String]) {
    val sc = SparkSession.builder().appName("BasicParseJson").config("spark.master", "local").getOrCreate()

    //   val master = args(0)
    val inputFile = args(1)
    val outputFile = args(2)
    //   val sc = new SparkContext(master, "BasicParseJson", System.getenv("SPARK_HOME"))
    val input = sc.sparkContext.textFile(inputFile)
    val parsed = input.map(Json.parse(_))
    // We use asOpt combined with flatMap so that if it fails to parse we
    // get back a None and the flatMap essentially skips the result.
    val result = parsed.flatMap(record => personReads.reads(record).asOpt)
    result.filter(_.lovesPandas).map(Json.toJson(_)).saveAsTextFile(outputFile)
  }
   
  */
}
