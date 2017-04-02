package org.ashu.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.sql.SparkSession


object BasicLoadSequenceFile {
    def main(args: Array[String]) {
      val sc = SparkSession.builder().appName("BasicLoadSequenceFile").
      config("spark.master","local").getOrCreate()
      val data = sc.sparkContext.sequenceFile("/home/hduser/BigData_Workspace/BigDataProject/resources/data/Gm9_genome_seq.txt",
          classOf[Text], classOf[IntWritable]).map{case (x, y) =>
        (x.toString, y.get())}
      println(data.collect().toList)
    }
}
