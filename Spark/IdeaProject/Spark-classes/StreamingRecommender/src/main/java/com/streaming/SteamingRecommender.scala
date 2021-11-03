package com.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.language.postfixOps
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._

import scala.collection.convert.ImplicitConversions.`map AsJavaMap`
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ConnHelper extends Serializable{
//  lazy val jedis = new Jedis("localhost")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://10.120.30.118:27017/recommender"))
}
case class MongConfig(uri:String,db:String)

case class resRating(userIdIndex:Int,resIdIndex:Int,score:Double,timestamp: TimestampType)

case class Recommendation(resIdIndex:Int,score:Double)

case class UserRecs(userIdIndex:Int,recs: Seq[Recommendation])

case class ResRecs(resIdIndex:Int,recs: Seq[Recommendation])

object SteamingRecommender {
  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_RES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_RESTAURANT_RECS_COLLECTION = "RestaurantRecs"

  def main(args: Array[String]): Unit = {
    val config = Map( "spark.cores" -> "local[*]"
      , "mongo.uri" -> "mongodb://10.120.30.118:27017/recommender"
      , "mongo.db" -> "recommender"
      , "kafka.topic" -> "recommender" )
    // TODO create SparkSQL environment
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    implicit val mongConfig = MongConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._ // 在使用DataFrame時，如果涉及轉換操作，則需要引入轉換規則

    // TODO create streaming context
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc,Seconds(3)) //batch duration

    // TODO 處理
    //載入review data (後續要更換為暫存器儲存?)
    val review_df = yelp_review_data(spark)
      .select(col("userIdIndex"), col("resIdIndex"), col("stars").as("score")
      , col("date").as("timestamp"))
    review_df.printSchema()

    //載入餐廳相似度矩陣，並把它廣播出去(待MongoDB建立)
    val simResMatrix = spark.read
      .option("url",config("mongo.uri"))
      .option("collection",MONGODB_RESTAURANT_RECS_COLLECTION)
      .format("com.mongodb.spark.sql").load()
      .as[ResRecs].rdd
      .map{restaurantRecs => //為了提高查詢相似度餐廳的效率，故轉換為map
        (restaurantRecs.resIdIndex , restaurantRecs.recs.map(x=>(x.resIdIndex,x.score)).toMap)
      }.collectAsMap()
    val simMatrixBroadCast = sc.broadcast(simResMatrix) // broadcast 說明:https://ithelp.ithome.com.tw/articles/10187119

    // 連接web app 訊息 (通過Kafka創建一個DStream)
    val kafkaStream = kafkaStreaming(ssc, config("group.id"))

    // 把原始數據轉換成評分stream(userIdIndex,resIdIndex,score,timestamp)
    // 格式
    val ratingStream = kafkaStream.map{msg =>
      implicit val formats = DefaultFormats
      val rating_json = parse(msg.value())
      val userIDIndex = (rating_json \ "user").extractOpt[Int].get
      val resIdIndex = (rating_json \ "restaurant").extractOpt[Int].get
      val rating = (rating_json \ "rating").extractOpt[Double].get
      val timestamp = (rating_json \ "timestamp").extractOpt[Int].get
      println("receive message: "+(userIDIndex,resIdIndex,rating,timestamp))
      (userIDIndex,resIdIndex,rating,timestamp)
    }
    // 繼續做stream 處理，時實算法核心
    ratingStream.foreachRDD{rdds =>
      rdds.foreach{ case(userIdIndex, resIdIndex, score, timestamp) =>
        println("rating data >>>>>>>>>")
        //1. 從 HDFS or MongoDB 裡獲取當前用戶最近的K次評分，保存成Array[(resID, score)] // 原範例中，當前用戶最近的K次評分是存在redis
        val userRecentlyRatings = getUserRecentlyRatings(MAX_USER_RATINGS_NUM, userIdIndex, review_df)
        //2. 從相似度矩陣中取出當前餐廳最相似的N個餐廳，作為備選列表，Array[resID] //TODO 需要再對距離篩選
        val candidateRess = getTopSimRes(MAX_SIM_RES_NUM, resIdIndex, userIdIndex, simMatrixBroadCast.value)
        //3. 對每個備選電影，計算推薦優先順序，得到當前用戶的實時推薦列表，Array[(resID, score)]
        val streamRecs = computerResScores(candidateRess, userRecentlyRatings, simMatrixBroadCast.value)
        //4. 把推薦數據保存到 mongodb
        saveDataToDB(userIdIndex,streamRecs)
      }
    }

    spark.close()
    //開始接收和處理數據
    ssc.start()
    println("streaming started")
    ssc.awaitTermination()

  }

  def getUserRecentlyRatings(num: Int, userIdIndex: Int, review_df: DataFrame)={
    review_df.filter(f"userIdIndex == '$userIdIndex'")
      .orderBy(col("timestamp").desc).limit(num)
      .rdd.map(row => (row.get(1).asInstanceOf[Int], row.get(2).asInstanceOf[Int].toDouble)).collect()
  }

  /**
   * @param num 相似餐廳的數量
   * @param resIdIndex 將比較的餐廳ID
   * @param userIdIndex user id
   * @param simRestaurants 相似度矩陣
   * @return 篩選後的推薦餐廳列表
   */
  def getTopSimRes(num: Int, resIdIndex: Int, userIdIndex: Int
                   , simRestaurants: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                  (implicit mongoConfig: MongConfig)= {
    // 1. 從相似度矩陣當中拿到所有相似的餐廳
    val allSimRestaurants = simRestaurants(resIdIndex).toArray
    // 2. 查詢用戶過去所評分過的餐廳
    //TODO 在mongodb中，需要有一個資料表是波特蘭的review data
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid"-> userIdIndex)).toArray.map{
      item =>
        item.get("resIdIndex").toString.toInt
    }
    // 3. 過濾已評分過的電影，並排序輸出
    allSimRestaurants
      .filter(x => !ratingExist.contains(x._1))
      //TODO 需要再篩掉1km以外的餐廳
      .sortWith(_._2 > _._2)
      .take(num).map(x =>x._1)
  }

  /**
   * 計算待選餐廳的推薦分數
   *
   * @param candidateRess 待選餐廳列表
   * @param userRecentlyRatings 用戶最近K次的評分
   * @param SimRess
   * @return
   */
  def computerResScores
  (candidateRess: Array[Int], userRecentlyRatings: Array[(Int, Double)], SimRess: collection.Map[Int, Map[Int, Double]]):Array[(Int,Double)] = {
    // 定義一個AarrayBuffer, 用於保存每個備選餐廳的基本得分
    var scores = ArrayBuffer[(Int,Double)]()
    // 定義一個HashNap，保存每一間餐廳的增強減弱因子
    val increMap = mutable.HashMap[Int,Int]()
    val decreMap = mutable.HashMap[Int,Int]()

    for(candidateRes <- candidateRess; userRecentlyRating<-userRecentlyRatings){
      // 拿到備選餐廳和最近評分餐廳的相似度
      val simScore = getRessSimScore(candidateRes,userRecentlyRating._1,SimRess)
      if(simScore > 0.7){
        //計算備選餐廳的基本得分
        scores += ((candidateRes,simScore * userRecentlyRating._2))
        if(userRecentlyRating._2 > 3){
          increMap(candidateRes) = increMap.getOrDefault(candidateRes,0)+1
        }else{
          decreMap(candidateRes) = decreMap.getOrDefault(candidateRes,0)+1
        }
      }
    }
    //根據備選餐廳的resIdIndex做groupby，計算推薦評分
    scores.groupBy(_._1).map{
      //groupBy之後回傳Map(resIdIndex -> ArrayBuffer[(resIdIndex,score)])
      case (resIdIndex,scoreList) =>
        (resIdIndex,(scoreList.map(_._2).sum / scoreList.length)+logFactor(increMap.getOrDefault(resIdIndex,1)) -logFactor(decreMap.getOrDefault(resIdIndex,1)))
    }.toArray

  }

  // 獲取相似度評分
  def getRessSimScore(resIdIndex_1: Int, resIdIndex_2: Int, SimRess: collection.Map[Int, Map[Int, Double]]): Double ={
    SimRess.get(resIdIndex_1) match {
      case Some(sims) => sims.get(resIdIndex_2) match {
        case Some(score) => score
        case None =>0.0
      }
    }
  }
  def logFactor(m: Int): Double ={
    val N =10 //以N為底
    math.log(m)/ math.log(10)
  }

  def saveDataToDB(userIdIndex: Int, streamRecs: Array[(Int, Double)])(implicit mongConfig: MongConfig): Unit ={
    val streamRecsCollection = ConnHelper.mongoClient(mongConfig.db)(MONGODB_STREAM_RECS_COLLECTION)
    streamRecsCollection.findAndRemove(MongoDBObject("uid" -> userIdIndex))
    streamRecsCollection.insert(
      MongoDBObject(
        "uid" -> userIdIndex, "recs" ->streamRecs.map(
        x => MongoDBObject("resId" -> x._1,"score"->x._2)
        )
      )
    )
  }

  def yelp_review_data(spark: SparkSession): DataFrame ={
    val path = f"Yelp/Dataset/Done/reviews.csv"
    val df = spark.read
      .options(Map("header"->"true","inferSchema"->"true","multiLine"->"true","escape"->"\"")) //,"escape"->"\'"
      .csv(path)
    val userID = df.select("user_id").distinct()
      .withColumn("userIdIndex",monotonically_increasing_id().cast(IntegerType))
    val resID = df.select("business_id").distinct()
      .withColumn("resIdIndex",monotonically_increasing_id().cast(IntegerType))
    val resRatingWithID = df.join(userID, "user_id").join(resID, "business_id")
    resRatingWithID
  }

  def kafkaStreaming(ssc: StreamingContext, topic: String) = {
    val kafkaPara: Map[String, Object] = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
        "bdse42:9092,bdse169:9092,bdse170:9092"
      , ConsumerConfig.GROUP_ID_CONFIG -> topic
      , "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
      , "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
      , "auto.offset.reset" -> "latest" //this is default
    )
    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc
      , LocationStrategies.PreferConsistent
      , ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaPara))
    kafkaStream
  }

}
