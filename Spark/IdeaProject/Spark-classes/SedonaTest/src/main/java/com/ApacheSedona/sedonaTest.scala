package com.ApacheSedona

import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{asc, avg, col, collect_list, column, concat, lit, struct, sum, to_json}
import org.apache.spark.sql.sedona_sql.expressions.ST_Distance

import java.time.LocalDateTime

object sedona_example {
  val CITY_NAME = "Portland"

  def main(args: Array[String]): Unit = {
    // TODO create SparkSQL environment
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sedona_example")
    val spark = SparkSession.builder()
      .config(sparkConf)
      // Enable Sedona custom Kryo serializer
      .config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
      .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
      .getOrCreate() // org.apache.sedona.core.serde.SedonaKryoRegistrator
    import spark.implicits._ // 在使用DataFrame時，如果涉及轉換操作，則需要引入轉換規則
    SedonaSQLRegistrator.registerAll(spark)

    // TODO execute
    // load data
    val filename = "restaurants"
    val df = yelp_data(spark, filename)
      .filter('latitude.isNotNull).filter('longitude.isNotNull).filter('business_id.isNotNull)
    //    println(df.filter(col("longitude").isNull || col("longitude") === "").count())

    //    println("================ filter by a distance and central of city ================")
    //    val res_26km = locationFilter(spark, df, CITY_NAME,26)
    //    res_26km.printSchema()
    //    res_26km.show(10)
    //    res_26km.select("dist_km").describe().show()

    //    println("================ filter by city name ================")
    // Create a Geometry type column
    val ResLoc = df
      .filter(col("city") === CITY_NAME)
      .withColumn("location",
        concat(lit("POINT("), $"latitude", lit(" "), $"longitude", lit(")")).as("loc"))
      .select("business_id", "location")
    ResLoc.createOrReplaceTempView("ResLoc")
    val ResLoc2 = spark.sql("SELECT business_id, ST_GeomFromWKT(location) as location FROM ResLoc".stripMargin)

    //    Portland Art Museum: 45.516178041092424, -122.68332450863616
    val latitude = 45.516178041092424
    val longitude = -122.68332450863616
    val Res_dist_filter_df = distantFilter(spark, ResLoc2, latitude, longitude, 2)

    val output = Res_dist_filter_df.join(df, "business_id")
      .select("name", "address", "dist_km", "latitude", "longitude", "stars", "hours")
    println("output dataframe: ")
    output.show(10)

    val df2maps = output.collect.map(r => {
      val tuples = output.columns.zip(r.toSeq).map { x =>
        if (x._2.isInstanceOf[String])
          (x._1, x._2.asInstanceOf[String])
        else
          (x._1, String.valueOf(x._2))
      }
      Map(tuples: _*)
    })
    val map = Map(df2maps.map(p => (p.getOrElse("business_id", null), p)): _*)
    //var 設定
    val user_id = "wefkmd5cs5efw"
    val localDateTime = LocalDateTime.now().toString
    val jSON = output.toJSON.rdd.reduce(_ + "," + _)
    val res_id_json = "{" + jSON + "}"
    //     output 資料
    val frame = Seq((user_id, df2maps)).toDF("user_id", "business_id_listInRange")
    println("frame(Restaurants info):")
    frame.printSchema()
    frame.show()
    val df2kafka = frame.select(lit(localDateTime).as("key"), to_json(struct(frame.columns.map(column): _*)).as("value"))
//    println("df2kafka:")
//    println(df2kafka.collectAsList().get(0)(1))

    val topic = "longMessage"
    df2kafka.selectExpr(f"CAST(key AS STRING)", f"CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "bdse42:9092,bdse169:9092,bdse170:9092")
      .option("topic", topic)
      .save()

    // TODO close environment
    spark.close()
  }

  def locationFilter(spark: SparkSession, df: DataFrame, city_name: String, dis_range_km: Double) = {
    import spark.implicits._
    val avgDF = df.filter(col("city") === city_name)
      .filter(col("latitude").isNotNull).filter(col("longitude").isNotNull)
      .select(avg("latitude"), avg("longitude"))
    avgDF.show()
    val latitude = avgDF.collect()(0).get(0).asInstanceOf[Double]
    val longitude = avgDF.collect()(0).get(1).asInstanceOf[Double]
    // Create a Geometry type column
    val ResLoc = df
      .filter('latitude.isNotNull).filter('longitude.isNotNull)
      .withColumn("location",
        concat(lit("POINT("), $"latitude", lit(" "), $"longitude", lit(")")).as("loc"))
      .select("business_id", "location")
    ResLoc.createOrReplaceTempView("ResLoc")
    val ResLocGeom = spark.sql("SELECT business_id, ST_GeomFromWKT(location) as location FROM ResLoc".stripMargin)
    distantFilter(spark, ResLocGeom, latitude, longitude, dis_range_km)
  }

  def distantFilter(spark: SparkSession, df: DataFrame, latitude: Double, longitude: Double, dis_range_km: Double): DataFrame = {
    df.createOrReplaceTempView("ResLoc")
    val Res_dist_df = spark.sql(("SELECT business_id," +
      f"ST_Distance(location,ST_GeomFromWKT('POINT($latitude $longitude)'))*100 AS dist_km FROM ResLoc").stripMargin)
      .filter(col("dist_km") < dis_range_km)
      .orderBy(asc("dist_km")).toDF()
    Res_dist_df
  }

  def yelp_data(spark: SparkSession, filename: String): DataFrame = {
    val path = f"Yelp/Dataset/Done/$filename.csv"
    val df = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true", "multiLine" -> "true", "escape" -> "\"")) //,"escape"->"\'"
      .csv(path)
    df
  }
}

/*
================ filter by a distance and central of city ================
+-----------------+-------------------+
|    avg(latitude)|     avg(longitude)|
+-----------------+-------------------+
|45.52155624899827|-122.65333746144259|
+-----------------+-------------------+

root
|-- business_id: string (nullable = true)
  |-- dist_km: double (nullable = false)

  +--------------------+--------------------+
  |         business_id|             dist_km|
  +--------------------+--------------------+
  |ydOrglIOU1ImnpU1V...|0.030831241386857042|
  |cFadNtYO5AYWl70tn...|  0.0460580192428258|
  |26CgmajAEW_oD06E8...|0.051363706740589765|
  |sHmYWLsJM44yoU8Yy...|  0.1401961824906851|
  |oO5VVowQVHMnyHxPS...| 0.14594678534178446|
  |9SgqGk0FL8Nsks_D0...| 0.16057210964458246|
  |VKRQSjqn4XjNrdboS...| 0.16511058708492587|
  |GkIzT4KNgff-f9FPa...| 0.17554504022475192|
  |DOfD6GuQ37dcTaRlk...| 0.17787411355159843|
  |GZYj9KY7S7v1ELoe9...| 0.17853296631420956|
  +--------------------+--------------------+
  only showing top 10 rows

  +-------+--------------------+
  |summary|             dist_km|
  +-------+--------------------+
  |  count|                8164|
  |   mean|   8.350708338537888|
  | stddev|   6.044609563607438|
  |    min|0.030831241386857042|
  |    max|   24.59536863707614|
  +-------+--------------------+

  ================ filter by city name ================
  root
  |-- business_id: string (nullable = true)
  |-- dist_km: double (nullable = false)

  +--------------------+--------------------+
  |         business_id|             dist_km|
  +--------------------+--------------------+
  |J8vHgX23BRGXWPbGa...|0.057040095390721274|
  |rkT1xtABcp1njv0pU...|  0.0907254111718115|
  |QmVlkV5O4IgCp116E...| 0.10233178505152456|
  |wk62LiusYiNNWdjAi...| 0.11982627521069575|
  |ZPQY4VrsBhwd6z4dM...|  0.1456574382916342|
  |E4E7CxmvXLFwiD0n0...|  0.1588710349518161|
  |4XyXfIPnyXPVE2VY3...|  0.1599161817980601|
  |-V4awcpOMI6-tM4yh...| 0.16018934982917574|
  |UqNS2fdERh7RNmUZM...| 0.16189074351615312|
  |DfwPVQijs8EDKEG9c...| 0.16704961706171428|
  +--------------------+--------------------+
  only showing top 10 rows

  +-------+--------------------+
  |summary|             dist_km|
  +-------+--------------------+
  |  count|                5730|
  |   mean|   5.801496163053845|
  | stddev|  4.5549984259067955|
  |    min|0.057040095390721274|
  |    max|   22.02755765045918|
  +-------+--------------------+


  Process finished with exit code 0

*/