package org.ashu.spark

import scala.collection.JavaConversions._
import org.apache.spark._
import com.twitter.elephantbird.mapreduce.input.LzoJsonInputFormat
import org.apache.hadoop.io.{LongWritable, MapWritable, Text, BooleanWritable}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, Job => NewHadoopJob}
import java.util.HashMap

object LoadJsonWithElephantBird {
  def main(args: Array[String]) {
   /* if (args.length < 2) {
      println("Usage: [sparkmaster] [inputfile]")
      exit(1)
    }*/
    val master = "local"
    val inputFile = "/home/hduser/BigData_Workspace/BigDataProject/resources/data/pandainfo.json"
    val sc = new SparkContext(master, "LoadJsonWithElephantBird", System.getenv("SPARK_HOME"))
    val conf = new NewHadoopJob().getConfiguration
    conf.set("io.compression.codecs","com.hadoop.compression.lzo.LzopCodec")
    conf.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec")
    val input = sc.newAPIHadoopFile(inputFile, classOf[LzoJsonInputFormat], classOf[LongWritable], classOf[MapWritable], conf).map{case (x, y) =>
      (x.get, y.entrySet().map{entry =>
        (entry.getKey().asInstanceOf[Text].toString(),
         entry.getValue() match {
           case t: Text => t.toString()
           case b: BooleanWritable => b.get()
           case _ => throw new Exception("unexpected input")
         }
        )})}
    println(input.collect().seq)
    }
}
