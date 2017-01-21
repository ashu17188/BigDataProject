package org.ashu.spark

import org.apache.spark.sql.SparkSession



object SqlDatabaseConnect {
  def main(args: Array[String]) {
    val sc = SparkSession
      .builder.appName("wordcount").config("spark.master", "local").getOrCreate();

    val url =
      "jdbc:mysql://localhost:3306/mydsn?user=root&password=root"
    val df = sc.sqlContext
      .read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", url)
      .option("dbtable", "mydsn.people")
      .load()

    // Looks the schema of this DataFrame.
    df.printSchema()

    // Counts people by age
    val countsByAge = df.groupBy("age").count()
    countsByAge.show()

    // Saves countsByAge to S3 in the JSON format.
    countsByAge.write.format("json").save("s3a://...")
  }
}