package org.ashu.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.JdbcRDD
import java.sql.{ PreparedStatement, DriverManager, ResultSet }
import org.apache.hadoop.mapred.lib.db._
import org.apache.hadoop.mapred.JobConf

object WriteSimpleDB {
  def main(args: Array[String]) {
    /*if (args.length < 1) {
      println("Usage: [sparkmaster]")
      exit(1)
    }*/
    val master = "local"
    val sc = new SparkContext(master, "WriteSimpleJdbc", System.getenv("SPARK_HOME"))
    val data = sc.parallelize(List((4, "cat1", 33)))
    // foreach partition method
    data.foreachPartition { records =>
      records.foreach(record => println("fake db write"))
    }
    // DBOutputFormat approach
    val records = data.map(e => (catRecord(e._1, e._2, e._3), null))
    val tableName = "mydsn.user"
    val fields = Array("id", "name", "age")
    val jobConf = new JobConf()
    DBConfiguration.configureDB(jobConf, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost/mydsn?user=root&password=root")
    DBOutputFormat.setOutput(jobConf, tableName, fields: _*)
    records.saveAsHadoopDataset(jobConf)
  }
  case class catRecord(id: Int, name: String, age: Int) extends DBWritable {
    override def write(s: PreparedStatement) {
      s.setInt(1, id)
      s.setString(2, name)
      s.setInt(3, age)
    }
    override def readFields(r: ResultSet) = {
      // blank since only used for writing
    }
  }

}
/*
CREATE TABLE `user` (
  `id` int(11) NOT NULL,
  `name` varchar(45) DEFAULT NULL,
  `age` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
*/