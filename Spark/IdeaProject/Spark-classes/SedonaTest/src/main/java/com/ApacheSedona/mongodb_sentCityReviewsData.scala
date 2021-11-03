package com.ApacheSedona

import com.ApacheSedona.sedona_example._
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}

object mongodb_sentCityReviewsData {
  val CITY_NAME = "Portland"

  def main(args: Array[String]): Unit = {
    // TODO create SparkSQL environment
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder()
      .config(sparkConf)
//      .config("spark.mongodb.output.uri", s"mongodb://10.120.30.118:27017/yelp.reviews")
      // Enable Sedona custom Kryo serializer
      .config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
      .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
      .getOrCreate()
    SedonaSQLRegistrator.registerAll(spark)

    // TODO execute
    // load data
    val restaurants_df = yelp_data(spark, "restaurants")
    val reviews_df = yelp_data(spark, "reviews")
    //    val resCity = restaurants_df.filter(s"city == '${CITY_NAME}'")
    val res_300km = locationFilter(spark, restaurants_df, CITY_NAME,300)
    println(f"$CITY_NAME 300km restaurants count:" + res_300km.count())
    val reviewsCityDF = reviews_df.join(res_300km.select("business_id"), "business_id")
    println(f"$CITY_NAME 300km reviews data count:" + reviewsCityDF.count())

    println("saving ...")
    reviewsCityDF
      .write.format("mongo")
      .option("uri", "mongodb://10.120.30.118:27017")
      .option("database", "yelp")
      .option("collection", "reviews")
      .mode("overwrite").save()
    println("complete")

    // TODO close environment
    spark.close()
  }

  def yelp_data(spark: SparkSession, filename: String): DataFrame = {
    val path = f"Yelp/Dataset/Done/${filename}.csv"
    val df = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true", "multiLine" -> "true", "escape" -> "\"")) //,"escape"->"\'"
      .csv(path)
    df
  }

}
