package com.offline

import com.offline.recommenderLFM.yelp_data
import org.apache.spark.SparkConf
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.types.IntegerType
import org.jblas.DoubleMatrix

import scala.collection.mutable.ArrayBuffer

object ALSTrainer_testing {
  val MASTER = "local[*]"
  val NAME_NODE = "bdse174"
  val RANK_ARRAY = List.range(10, 120+1, 30).toArray
  val LAMBDA_ARRAY =ArrayBuffer[Double]()
  for (i <- 0 to 3){
    LAMBDA_ARRAY.append(Math.pow(0.1,i))
  }
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster(MASTER).setAppName("ALSTrainer")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._ // 在使用DataFrame時，如果涉及轉換操作，則需要引入轉換規則
    // load data
//    val resRating = yelp_data(spark, "reviews").select("user_id", "business_id", "stars").distinct().limit(50000) // local mode
    // 3. load from mongodb
    val mongo_uri_input = "mongodb://10.120.30.118:27017"
    val resRating = spark.read  //yelp_review_data(spark)
      .format("mongo")
      .option("uri", mongo_uri_input)
      .option("database", "yelp")
      .option("collection", "reviews")
      .load()
      .select(col("user_id"), col("business_id"), col("stars")).distinct()
    //    val resRating = spark.read.load(f"hdfs://$NAME_NODE:8020/YelpParquet/rating.parquet").select("user_id", "business_id", "stars").distinct()
    // 建立user與res的ID index
    val userID = resRating.select("user_id").distinct()
      .withColumn("userIdIndex", monotonically_increasing_id().cast(IntegerType))
    val resID = resRating.select("business_id").distinct()
      .withColumn("resIdIndex", monotonically_increasing_id().cast(IntegerType))
    var resRatingWithID = resRating.join(userID, "user_id")
    resRatingWithID = resRatingWithID.join(resID, "business_id")
    println("data loading completed")
    resRating.count()
    // 轉換為RDD
    val resRatingRDD = resRatingWithID.select(col("userIdIndex"), col("resIdIndex"), col("stars").as("score"))
      .as[resRating].rdd.map(x => (x.userIdIndex, x.resIdIndex, x.score))
    val userRDD = resRatingRDD.map(_._1)
    val resRDD = resRatingRDD.map(_._2)
    val resRatingRDD_recs = resRatingRDD.map(x => Rating(x._1, x._2, x._3))

    // split dataset to training dataset and test dataset
    val splits = resRatingRDD_recs.randomSplit(Array(0.8, 0.2))
    val trainRDD = splits(0)
    val testRDD = splits(1)
    println("training and testing dataset preparation completed")

    //MAP@K 需求參數
    // 計算笛卡兒積 (Cartesian product)
    val userRes = userRDD.cartesian(resRDD)


    // 參數選擇及最佳化
    adjustALSParam(trainRDD, testRDD)

    // TODO close environment
    spark.close()
  }

  def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]): Unit = {
    val rankArray = Array(50)
    val lambdaArray = Array(0.01)
//    val rankArray = RANK_ARRAY
//    val lambdaArray = LAMBDA_ARRAY.toArray
    val iterationArray = Array(8)
    println("start adjust ALS parameter:")
    val result = for (rank <- rankArray; lambda <- lambdaArray; iteration <- iterationArray)
      yield {
        val model = ALS.train(trainData, rank, iteration, lambda)
        val rmse = getRMSE(model, testData)
        val MAPatK = getmapK(model, testData)
        (rank, lambda, iteration, rmse, MAPatK)
      }
    result.foreach(println)
    //print optimized result
    println(f"optimized parameter (rank, lambda, iteration):" + result.minBy(_._3))
  }

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    // 計算預測評分
    val userProduct = data.map(item => (item.user, item.product))
    val predictRating = model.predict(userProduct)
    // 以 user id 與product id 作為外建, inner join實際值與預測值
    val observed = data.map(item => ((item.user, item.product), item.rating))
    val predict = predictRating.map(item => ((item.user, item.product), item.rating))
    // 計算 RMSE
    breeze.numerics.sqrt(
      // inner join的結果為 ((userid, product id),(actual,predict))
      observed.join(predict).map {
        case ((userID, productID), (act, pre)) =>
          val err = act - pre
          err * err
      }.mean()
    )
  }

  def getmapK(model : MatrixFactorizationModel,testData: RDD[Rating]): Double={
    // compute MAP@K
    val itemFactors = model.productFeatures.map { case (resId, factor) => factor }.collect()
    val itemMatrix = new DoubleMatrix(itemFactors) //need to find a way to broadcast this variable
    val allRecs = model.userFeatures.map { case (userId, arr) =>
      val userVector = new DoubleMatrix(arr)
      val scores = itemMatrix.mmul(userVector)
      val sortedWithId = scores.data.zipWithIndex.sortBy(_._1)
      val recommendedIds = sortedWithId.map(_._2).toSeq
      (userId, recommendedIds)
    }
    val userRess = testData.map { case Rating(user, product, rating) => (user, product) }.groupBy(_._1)
    val MAPK = allRecs.join(userRess).map{ case (userId,(predicted, actualwithIds)) =>
      val actual = actualwithIds.map(_._2).toSeq
      avgPrecisionK(actual,predicted,20)
    }.reduce(_+_)/allRecs.count()
    MAPK
  }

  def avgPrecisionK(actual: Seq[Int], predicted: Seq[Int], k: Int): Double = {
    val predK = predicted.take(k)
    var score = 0.0
    var numHits = 0.0
    for ((p, i) <- predK.zipWithIndex) {
      if (actual.contains(p)) {
        numHits += 1.0
        score += numHits / (i.toDouble + 1.0)
      }
    }
    if (actual.isEmpty) {
      1.0
    } else {
      score / scala.math.min(actual.size, k).toDouble
    }
  }

}
