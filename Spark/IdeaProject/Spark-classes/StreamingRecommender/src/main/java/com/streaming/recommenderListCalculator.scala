package com.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col, desc, explode, expr, lit}
import org.jblas.DoubleMatrix

import scala.collection.mutable

object recommenderListCalculator {
  def main(args: Array[String]): Unit = {
    // TODO create SparkSQL environment
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate() // 在使用DataFrame時，如果涉及轉換操作，則需要引入轉換規則
    // TODO execute
    import spark.implicits._
    val kafkaInform = spark.read.json("Yelp/result/test.json")
    val user_id = kafkaInform.select("user_id").collect()(0).get(0)
    println("user_id: "+user_id)
    // Create a DataFrame with Array of Struct column
    val resDF = kafkaInform.select($"user_id",explode($"business_id_listInRange")).select("col.*")
//    resList.printSchema()

    val mongo_uri = "mongodb://10.120.30.118:27017/ALS"
    // load user and res features
    val userFeaturesDF = spark.read //yelp_review_data(spark)
      .format("mongo")
      .option("uri", mongo_uri)
      .option("collection", "userFeatures")
      .load().select("user_id","user_feature")

    val resFeaturesDF = spark.read //yelp_review_data(spark)
      .format("mongo")
      .option("uri", mongo_uri)
      .option("collection", "resFeatures")
      .load().select("business_id","res_feature")

    // get user feature and res feature list
    val userFeature = userFeaturesDF
      .filter(col("user_id") === user_id)
      .collect()(0).get(1).asInstanceOf[mutable.WrappedArray[java.lang.Double]].array
//    userFeature.printSchema()
    val resFeature = resFeaturesDF.join(resDF.select("business_id","stars"), "business_id")
//      .map{x=>
//      (x.get(0).toString,x.get(1).asInstanceOf[mutable.WrappedArray[java.lang.Double]].array)
//    }
    // DotProduct calculation
    // https://stackoverflow.com/questions/60574826/dot-product-in-spark-scala
    val frame = resFeature.withColumn("userFeature", lit(userFeature))
      .withColumn("DotProduct", expr("aggregate(zip_with(res_feature, userFeature, (x, y) -> x * y), 0D, (sum, x) -> sum + x)"))
    frame.orderBy(desc("DotProduct")).show()
    frame.orderBy(asc("DotProduct")).show()
    frame.describe("DotProduct").show()
    // TODO close environment
    spark.close()
  }

}
