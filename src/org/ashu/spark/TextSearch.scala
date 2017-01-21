package org.ashu.spark

import org.apache.spark.sql.SparkSession

/*
 * http://spark.apache.org/examples.html
 * DataFrame API Examples-Text Search
 * 
 */
object TextSearch {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("text_search").config("spark.master","local").getOrCreate();

    val textFile = spark.sparkContext.textFile("/home/hduser/BigData_Workspace/BigDataProject/resources/textSearchInput.txt");

    // Creates a DataFrame having a single column named "line"
  /*  val df = textFile.toDF("line");
    val errors = df.filter(col("line").like("%ERROR%"))
    // Counts all the errors
    errors.count()
    // Counts errors mentioning MySQL
    errors.filter(col("line").like("%MySQL%")).count()
    // Fetches the MySQL errors as an array of strings
    errors.filter(col("line").like("%MySQL%")).collect()*/
  }
}