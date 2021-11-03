package com.mongodb_test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}


object spark_mongodbConn_read {
  def main(args: Array[String]): Unit = {
    val mongo_uri = "mongodb://10.120.30.118:27017"

    val sparkConf = new SparkConf()
      .set("uri", mongo_uri)
      .setMaster("local[*]")
      .setAppName("mongodb read test")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    //載入review data (data load from Mongodb, which is the list that 300km distance range from central of Portland)
//    val review_df = spark.read  //yelp_review_data(spark)
//      .format("mongo")
//      .option("uri", mongo_uri)
//      .option("database", "yelp")
//      .option("collection", "reviews")
//      .load()
//      .select(col("user_id"), col("business_id"), col("stars").as("score")
//        , col("date").as("timestamp"))
//    review_df.printSchema()
//    review_df.show(10)

    import spark.implicits._
    val kafkaInform = spark.read.json("Yelp/result/test.json")
    val user_id = kafkaInform.select("user_id").collect()(0).get(0)
    println("user_id: "+user_id)
    // Create a DataFrame with Array of Struct column
    val resDF = kafkaInform.select($"user_id",explode($"business_id_listInRange")).select("col.*")
//    resDF.printSchema()
//    resDF.show()

    spark.close()
  }

}
