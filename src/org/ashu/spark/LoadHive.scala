package org.ashu.spark

import org.apache.spark._
import org.apache.spark.sql.hive.HiveContext


object LoadHive {
  def main(args: Array[String]) {
   /* if (args.length < 2) {
      println("Usage: [sparkmaster] [tablename]")
      exit(1)
    }*/
    val master = "local"
    val tableName = "users"
    val sc = new SparkContext(master, "LoadHive", System.getenv("SPARK_HOME"))
    val hiveCtx = new HiveContext(sc)
    val input = hiveCtx.sql("FROM src SELECT key, value")
   // val data:Map[] = input.map(_.getInt(0)
   // println(data.collect().toList)
    }
}
