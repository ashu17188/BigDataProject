package org.ashu.spark

import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark._
import org.apache.spark.SparkContext._


object BasicStreamingExample {
  def main(args: Array[String]) {
  /*  if (args.length < 2) {
      System.err.println("Usage BasicStreamingExample <master> <output>")
    }
    val Array(master, output) = args.take(2)*/

    val conf = new SparkConf().setMaster("local").setAppName("BasicStreamingExample")
    val ssc = new StreamingContext(conf, Seconds(30))

    val lines = ssc.socketTextStream("bbc.co.uk" , 8080)
    val words = lines.flatMap(_.split(" "))
    val wc = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)

    wc.saveAsTextFiles("/home/hduser/BigData_Workspace/BigDataProject/resources/BasicStreamingExampleOutput.txt")
    wc.print

    println("pandas: sscstart")
    ssc.start()
    println("pandas: awaittermination")
    ssc.awaitTermination()
    println("pandas: done!")
  }
}
