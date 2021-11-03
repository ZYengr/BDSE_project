package com.mysql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties

object spark_mySQLConn {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val spark = SparkSession.builder().config(sparkConf)
      .getOrCreate()
    val jdbcurl = "jdbc:mysql://10.120.30.118:3306/user"

    val prop = new Properties()
    prop.put("user", "ubuntu")
    prop.put("password", "ubuntu")
    prop.put("driver", "com.mysql.cj.jdbc.Driver")

    val df = spark.sqlContext.read.jdbc(jdbcurl, "user", prop)
    df.show(5)

    spark.close()
  }

}
