package com.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

// LFM所需要的資料
case class resRating(userIdIndex:Int,resIdIndex:Int,score:Double) //先移除timestamp ,timestamp: TimestampType
// 推薦的內容(restaurant,score)
case class Recommendation(resIdIndex:Int,score:Double)
// 定義預測評分的用戶推薦列表(找出適合使用者的餐廳)
case class UserRecs(userIdIndex:Int,recs: Seq[Recommendation])
// 基於LFM定義餐廳特徵向量的近似矩陣(找出相似的餐廳)
case class ResRecs(resIdIndex:Int,recs: Seq[Recommendation])

// change IdIndex to original ID
// 推薦的內容(restaurant,score)
case class ORecommendation(resId:String,score:Double)
// 定義預測評分的用戶推薦列表(找出適合使用者的餐廳)
case class OUserRecs(userId:String,recs: Seq[ORecommendation])
// 基於LFM定義餐廳特徵向量的近似矩陣(找出相似的餐廳)
case class OResRecs(resId:String,recs: Seq[ORecommendation])

case class MongoConfig(uri:String, db:String)

object recommenderLFM {

  val USER_MAX_RECOMMENDATION = 50
  val NAME_NODE = "bdse49"
  val FilE_NAME = "rating"

  def main(args: Array[String]): Unit = {
    val mongo_uri_input = "mongodb://10.120.30.118:27017"
//    val config = Map(
//      "spark.cores" -> "yarn"
//      , "mongo.uri" -> "mongodb://10.120.30.118:27017/recommender"
//      , "mongo.db" -> "recommender" )

//    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("OfflineRecommenderLFM")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._ // 在使用DataFrame時，如果涉及轉換操作，則需要引入轉換規則
    // load data
    // 1. load from csv file
//    val resRating = yelp_data(spark, "reviews")
    // 2. load from hdfs (yarn mode)
//    val resRating = spark.read.load(f"hdfs://$NAME_NODE:8020/YelpParquet/$FilE_NAME.parquet").dropDuplicates("text")
//      .select("user_id", "business_id", "stars").distinct()

    // 3. load from mongodb(Portland 周圍300km的評論)
    val resRating = spark.read  //yelp_review_data(spark)
      .format("mongo")
      .option("uri", mongo_uri_input)
      .option("database", "yelp")
      .option("collection", "reviews")
      .load()
      .select(col("user_id"), col("business_id"), col("stars")).distinct()

    // 建立user與res的ID index
    val userID = resRating.select("user_id").distinct()
      .withColumn("userIdIndex",monotonically_increasing_id().cast(IntegerType))
    val resID = resRating.select("business_id").distinct()
      .withColumn("resIdIndex",monotonically_increasing_id().cast(IntegerType))
    var resRatingWithID = resRating.join(userID, "user_id")
    resRatingWithID = resRatingWithID.join(resID, "business_id")

    // index to ID
//    val userIDMap = userID.collect().map(x=>Map(x.get(1).asInstanceOf[Int]->x.get(0).toString)).reduce(_++_)
//    val resIDMap = resID.collect().map(x=>Map(x.get(1).asInstanceOf[Int]->x.get(0).toString)).reduce(_++_)


    // 轉換為RDD
    val resRatingRDD = resRatingWithID.select(col("userIdIndex"), col("resIdIndex"), col("stars")
      .as("score")).as[resRating].rdd.map(x =>(x.userIdIndex,x.resIdIndex,x.score))
    val userRDD = resRatingRDD.map(_._1)
    val resRDD = resRatingRDD.map(_._2)

    // training(使用ALS矩陣拆解方法)
    val trainData = resRatingRDD.map(x=>Rating(x._1,x._2,x._3))
    val (rank, lambda, iterations) = (50,0.01,8) //(args(0).toInt,args(1).toDouble,args(2).toInt) //20, 0.01, 8
    val model = ALS.train(trainData,rank, iterations, lambda)
    // 儲存 userFeature and productFeature
    val userIdFeatures = model.userFeatures
      .toDF("userIdIndex","user_feature")
      .join(userID,"userIdIndex").drop("userIdIndex")
    val resIdFeatures = model.userFeatures
      .toDF("resIdIndex", "res_feature")
      .join(resID,"resIdIndex").drop("resIdIndex")

    val mongo_uri = "mongodb://10.120.30.118:27017"
    userIdFeatures.write.format("mongo")
      .option("uri", mongo_uri)
      .option("database", "ALS")
      .option("collection", "userFeatures").mode("overwrite").save()
    resIdFeatures.write.format("mongo")
      .option("uri", mongo_uri)
      .option("database", "ALS")
      .option("collection", "resFeatures").mode("overwrite").save()

//    // 計算笛卡兒積 (Cartesian product)
//    val userRes = userRDD.cartesian(resRDD)
//
//    // 使用model的predict方法來預測評分
//    val preRatings = model.predict(userRes)
//    val userRecs = preRatings
//      .filter(_.rating > 0) // filter 出大於0的項目
//      .map(ratings => (ratings.user, (ratings.product, ratings.rating)))
//      .groupByKey()
//      .map{
//        case (userid,recs) =>OUserRecs(userIDMap.get(userid).get, recs.toList.sortWith(_._2 > _._2)
//          .take(USER_MAX_RECOMMENDATION).map{x=>
//          ORecommendation(resIDMap.get(x._1).get,x._2)
//        })
//      }.toDF()
//    println("userRecs:")
//    userRecs.printSchema()
//    userRecs.show(10)
//    //TODO 儲存:使用者推薦列表
////    userRecs.write.option("header",true).csv("Yelp/recommenderSystem/userRecs.csv")
//    userRecs.write
//      .format("mongo")
//      .mode("overwrite")
//      .option("uri",mongoConfig.uri)
//      .option("collection", "user_recommended_restaurants")
//      .save()
//
//    // 基於restaurant特徵 計算近似矩陣 (取得餐廳相似度列表)
//    val resFeatures = model.productFeatures.map {
//      case (resid, features) => (resid, new DoubleMatrix(features))
//    }
//    // 算出餐廳與餐廳之間的近似矩陣
//    val resRecs = resFeatures.cartesian(resFeatures).filter{
//      case (resA,resB) => resA._1 != resB._1
//    }.map{
//      case (resA,resB) =>{
//        (resA._1,(resB._1,this.cosSim(resA._2,resB._2)))
//      }
//    }.filter(_._2._2 > 0.6) //挑選相似度大於0.6的元素
//      .groupByKey().map{
//      case (resid,recs) => OResRecs(resIDMap.get(resid).get,recs.toList.sortWith(_._2 > _._2)
//        .map(rec=> ORecommendation(resIDMap.get(rec._1).get,rec._2)))
//    }.toDF()
//
//    println("resRecs:")
//    userRecs.printSchema()
//    userRecs.show(10)
//    //TODO 餐廳的相似列表
////    resRecs.write.option("header",true).csv("Yelp/recommenderSystem/resRecs.csv")
//    resRecs.write
//      .format("mongo")
//      .mode("overwrite")
//      .option("uri",mongoConfig.uri)
//      .option("collection", "restaurant_similarity")
//      .save()
//

    // close
    spark.close()
  }

  def yelp_data(spark: SparkSession,filename:String): DataFrame ={
    val path = f"Yelp/Dataset/Done/$filename.csv"
    val df = spark.read
      .options(Map("header"->"true","inferSchema"->"true","multiLine"->"true","escape"->"\"")) //,"escape"->"\'"
      .csv(path)
    df
  }

  def cosSim(res1: DoubleMatrix, res2: DoubleMatrix):Double ={
    res1.dot(res2)/(res1.norm2()*res2.norm2())
  }
}
