package com.ApacheSedona

import sedona_example.distantFilter
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, column, concat, lit, struct, to_json}

import java.time.LocalDateTime

object sedonaFilter {
  val MASTER = "yarn"
  val NAME_NODE = "bdse49"
  val FilE_NAME = "restaurants"
  val CITY_NAME = "Portland"
  val PHOTO_FilE_NAME = "photo"
  def main(args: Array[String]): Unit = {
    val localDateTime = LocalDateTime.now().toString
    val user_id = args(3)
    // TODO create SparkSQL environment
    val sparkConf = new SparkConf().setMaster(MASTER).setAppName("Yelp_restaurants_filter")
    val spark = SparkSession.builder()
      .config(sparkConf)
      // Enable Sedona custom Kryo serializer
      .config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
      .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
      .getOrCreate() // org.apache.sedona.core.serde.SedonaKryoRegistrator
    import spark.implicits._
    SedonaSQLRegistrator.registerAll(spark)
    val df = spark.read.load(f"hdfs://$NAME_NODE:8020/YelpParquet/$FilE_NAME.parquet")
      .filter('latitude.isNotNull).filter('longitude.isNotNull).filter('business_id.isNotNull)
    val ResLoc = df
      .filter(col("city") === CITY_NAME)
      .withColumn("location",
        concat(lit("POINT("), $"latitude", lit(" "), $"longitude",lit(")")).as("loc"))
      .select("business_id","location")
    ResLoc.createOrReplaceTempView("ResLoc")
    val ResLoc2 = spark.sql("SELECT business_id, ST_GeomFromWKT(location) as location FROM ResLoc".stripMargin)

    val latitude = args(0).toDouble
    val longitude = args(1).toDouble
    val dis_range_km = args(2).toFloat
    // get photo_id
    val photo_df = spark.read.load(f"hdfs://$NAME_NODE:8020/YelpParquet/$PHOTO_FilE_NAME.parquet")
      .groupBy("business_id")
      .agg(collect_list("photo_id").as("photo_id"),collect_list("caption").as("caption"))
      .withColumnRenamed("business_id","business_id_photo")
    // Create a Geometry type column
    val Res_dist_filter_df = distantFilter(spark, ResLoc2, latitude, longitude, dis_range_km)
    val output = Res_dist_filter_df
      .join(df, "business_id")
      .join(photo_df,Res_dist_filter_df("business_id") === photo_df("business_id_photo"),"left")
      .select("business_id","name", "address", "dist_km", "latitude", "longitude", "stars", "hours", "photo_id", "caption")
//    output.write.mode(SaveMode.Append).json(f"hdfs://$NAME_NODE:8020/YelpParquet/dis_filter.json")

    // transform dataframe to Json format
    val df2maps = output.collect.map(r => {
      val tuples = output.columns.zip(r.toSeq).map{x=>
        if (x._2.isInstanceOf[String])
          (x._1, x._2.asInstanceOf[String])
         else
          (x._1,String.valueOf(x._2))
      }
      Map(tuples: _*)
    })
//    val map = Map(df2maps.map(p => (p.getOrElse("business_id", null), p)): _*) // change to map type
    val frame = Seq((user_id,df2maps)).toDF("user_id","business_id_listInRange")
    val df2kafka = frame.select(lit(localDateTime).as("key"),to_json(struct(frame.columns.map(column): _*)).as("value"))

    // send Json data to Kafka
    val topic = "longMessage"
    df2kafka.selectExpr(f"CAST(key AS STRING)", f"CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "bdse42:9092,bdse169:9092,bdse170:9092")
      .option("topic", topic)
      .option("kafka.max.request.size","4194304")
      .save()


    // TODO close environment
    spark.close()
  }

}
