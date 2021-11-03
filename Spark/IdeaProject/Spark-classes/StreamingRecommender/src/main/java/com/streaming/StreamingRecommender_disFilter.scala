package com.streaming

import com.streaming.SteamingRecommender.{kafkaStreaming, yelp_review_data}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ForeachWriter, SparkSession}
import org.apache.spark.sql.functions.{col, explode, expr, lit}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats

import java.time.LocalDateTime
import java.util.Properties
import scala.collection.mutable

object StreamingRecommender_disFilter {
  def main(args: Array[String]): Unit = {
    val MASTER = "yarn" //"local[*]" "yarn"
    val mongo_uri = "mongodb://10.120.30.118:27017"
    // TODO create SparkSQL environment
    val sparkConf = new SparkConf()
      .setMaster(MASTER).setAppName("StreamingRecommender-test_0.1")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    implicit val formats = org.json4s.DefaultFormats
    import spark.implicits._

    // TODO create streaming context
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc,Seconds(3)) //batch duration

    // TODO 處理
    // load user and res features
    val mongo_uri_ALS = "mongodb://10.120.30.118:27017/ALS"
    val userFeaturesDF = readMongoDB(spark,mongo_uri,"ALS","userFeatures")
      .select("user_id","user_feature")

    val resFeaturesDF = readMongoDB(spark,mongo_uri,"ALS","resFeatures")
      .select("business_id","res_feature")

    // broadcast 說明:https://ithelp.ithome.com.tw/articles/10187119
    val userFeaturesDF_bc = sc.broadcast(userFeaturesDF)
    val resFeaturesDF_bc = sc.broadcast(resFeaturesDF)

    // 從mongoDB載入ALS 訓練完的的模型參數
//    val W_factor = readMongoDB(spark,mongo_uri,"ALS","W_factor")
//    val U_factor = readMongoDB(spark,mongo_uri,"ALS","U_factor")
//    val b_factor = readMongoDB(spark,mongo_uri,"ALS","b_factor")
//    val c_factor = readMongoDB(spark,mongo_uri,"ALS","c_factor")

    // TODO for output String to Kafka
    val kafkaProducerProps = new Properties()
    kafkaProducerProps.put("bootstrap.servers","bdse42:9092,bdse169:9092,bdse170:9092")
    kafkaProducerProps.put("key.serializer", classOf[StringSerializer].getName)
    kafkaProducerProps.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](kafkaProducerProps)

    // 通過Kafka創建一個DStream
    val kafkaStream = kafkaStreaming(ssc, "longMessage")
    val resInfoStream = kafkaStream.map { msg =>
      msg.value()
    }

    resInfoStream.foreachRDD{rdd =>
      val jSONStringArray = rdd.collect()
      val sparkrdd = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      jSONStringArray.foreach{ data =>
        val kafkaInfo = sparkrdd.read.json(Seq(data).toDS())
        // 取得user_id
        val user_id = kafkaInfo.select("user_id").collect()(0).get(0)
        println("user_id: "+user_id)
        kafkaInfo.select($"user_id",explode($"business_id_listInRange"))

        // restaurant list in a range of distance
        val resDF = kafkaInfo.select($"user_id",explode($"business_id_listInRange")).select("col.*")

        // get user feature and res feature list
        val userFeature = userFeaturesDF_bc.value
          .filter(col("user_id") === user_id)
          .collect()(0).get(1).asInstanceOf[mutable.WrappedArray[java.lang.Double]].array
        val resFeature = resFeaturesDF_bc.value
          .join(resDF.select("business_id","stars"), "business_id")
        // DotProduct calculation (https://stackoverflow.com/questions/60574826/dot-product-in-spark-scala)
        val frame = resFeature.withColumn("userFeature", lit(userFeature))
          .withColumn("DotProduct", expr("aggregate(zip_with(res_feature, userFeature, (x, y) -> x * y), 0D, (sum, x) -> sum + x)"))
        //implement weight and bias

        val resDF_rating4 = resDF.withColumn("stars", resDF("stars").cast("Float")).filter(col("stars") > 4)
        try{
          val resList = resDF_rating4.select("business_id")
            .collect.map(_.getString(0))
          val outputMap = Map("user_id"->user_id, "recommender_list"->resList)
          val outputJson = org.json4s.jackson.Serialization.write(outputMap)
          //write to kafka
          // ref https://stackoverflow.com/questions/61285666/how-to-write-kafka-producer-in-scala
          producer.send(new ProducerRecord[String, String]("mytopic",LocalDateTime.now().toString,outputJson))
        }catch {
          case ex: RuntimeException=>
            println("maybe SedonaTrest version is lower that 0.4.1")
            ex.printStackTrace()
        }
      }
    }

    //開始接收和處理數據
    ssc.start()
    println("streaming started")
    ssc.awaitTermination()
  }

  def readMongoDB(spark: SparkSession, mongo_uri:String, database_name: String, collection_name:String) ={
    spark.read //yelp_review_data(spark)
      .format("mongo")
      .option("uri", mongo_uri)
      .option("database",database_name)
      .option("collection", collection_name)
      .load()
  }

}