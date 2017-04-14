package org.ashu.spark

import org.apache.spark._
import org.apache.spark.rdd.JdbcRDD
import java.sql.{DriverManager, ResultSet}

object LoadSimpleJdbc {
  def main(args: Array[String]) {
   /* if (args.length < 1) {
      println("Usage: [sparkmaster]")
      exit(1)
    }*/
    val master = "local"
    val sc = new SparkContext(master, "LoadSimpleJdbc", System.getenv("SPARK_HOME"))
    val data = new JdbcRDD(sc,
      createConnection, "SELECT * FROM mydsn.employee WHERE ? <= employee_id AND employee_id <= ?",
      lowerBound = 1, upperBound = 3, numPartitions = 2, mapRow = extractValues)
    println(data.collect().toList)
  }

  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    DriverManager.getConnection("jdbc:mysql://localhost/mydsn?user=root&password=root");
  }

  def extractValues(r: ResultSet) = {
    (r.getInt(1), r.getString(2))
  }
}
/*
 CREATE TABLE `employee` (
  `employee_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `first_name` varchar(45) NOT NULL,
  `last_name` varchar(45) NOT NULL,
  `salary` int(10) unsigned NOT NULL,
  `joining_date` datetime DEFAULT NULL,
  `department` varchar(45) NOT NULL,
  PRIMARY KEY (`employee_id`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=latin1;
 
 
 
*/