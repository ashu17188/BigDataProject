package org.ashu.spark

import org.apache.spark._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.reflect.ClassTag
import org.apache.spark.sql.SparkSession

object BasicIntersectByKey {

  def intersectByKey[K: ClassTag, V: ClassTag](rdd1: RDD[(K, V)], rdd2: RDD[(K, V)]): RDD[(K, V)] = {
    rdd1.cogroup(rdd2).flatMapValues {
      case (Nil, _) => None
      case (_, Nil) => None
      case (x, y)   => x ++ y
    }
  }

  def main(args: Array[String]) {
    val sc = SparkSession.builder().appName("BasicIntersectByKey").
      config("spark.master", "local").getOrCreate()
    val rdd1 = sc.sparkContext.parallelize(List((1, "panda"), (2, "happy")))
    val rdd2 = sc.sparkContext.parallelize(List((2, "pandas")))
    val iRdd = intersectByKey(rdd1, rdd2)
    val panda: List[(Int, String)] = iRdd.collect().toList
    panda.map(println(_))
    sc.stop()
  }
}
