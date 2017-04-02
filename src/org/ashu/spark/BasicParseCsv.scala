package org.ashu.spark

import java.io.StringReader
import java.io.StringWriter

import org.apache.spark._
import scala.util.parsing.json.JSON
import scala.collection.JavaConversions._

import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.sql.SparkSession

object BasicParseCsv {
  case class Person(name: String, favouriteAnimal: String)

  def main(args: Array[String]) {
    val sc = SparkSession.builder().appName("BasicParseCsv").
    config("spark.master", "local").getOrCreate()
    val inputFile = "/home/hduser/BigData_Workspace/BigDataProject/resources/data/favourite_animals.csv"
    val outputFile = "/home/hduser/BigData_Workspace/BigDataProject/resources/output2.txt"
    //    val sc = new SparkContext(master, "BasicParseCsv", System.getenv("SPARK_HOME"))
    val input = sc.sparkContext.textFile(inputFile)
    val result = input.map { line =>
      val reader = new CSVReader(new StringReader(line));
      reader.readNext();
    }
    val people = result.map(x => Person(x(0), x(1)))
    val pandaLovers = people.filter(person => person.favouriteAnimal == "panda")
    pandaLovers.map(person => List(person.name, person.favouriteAnimal).toArray).mapPartitions { people =>
      val stringWriter = new StringWriter();
      val csvWriter = new CSVWriter(stringWriter);
      csvWriter.writeAll(people.toList)
      Iterator(stringWriter.toString)
    }.saveAsTextFile(outputFile)
  }
}
