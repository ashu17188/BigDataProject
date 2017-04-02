package org.ashu.spark

import org.apache.spark._

/*import org.eclipse.jetty.client.HttpClient
import org.apache.spark.sql.SparkSession
*/
object BasicMapPartitions {
  def main(args: Array[String]) {
/*    val sc = SparkSession.builder().appName("BasicMapPartitions").config("spark.master", "local").
      getOrCreate()
    val input = sc.sparkContext.parallelize(List("KK6JKQ", "Ve3UoW", "kk6jlk", "W6BB"))
    val result = input.mapPartitions {
      signs =>
        val client = new HttpClient()
        client.start()
        signs.map { sign =>
          val exchange = new ContentExchange(true);
          exchange.setURL(s"http://qrzcq.com/call/${sign}")
          client.send(exchange)
          exchange
        }.map { exchange =>
          exchange.waitForDone();
          exchange.getResponseContent()
        }
    }
    println(result.collect().mkString(","))
*/  }
}
