package com.offline

import com.offline.recommenderLFM.yelp_data
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.types.IntegerType

object ALSTrainer {
  val MASTER = "yarn"
  val NAME_NODE = "bdse49"
  val RANK_ARRAY = List.range(10, 40 + 1, 10).toArray
  val ITERATION_ARRAY = List.range(15, 30 + 1, 5).toArray
  val LAMBDA_ARRAY = Array(0.05, 0.03, 0.01)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster(MASTER).setAppName("ALSTrainer_dropDuplicatesText")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._ // 在使用DataFrame時，如果涉及轉換操作，則需要引入轉換規則

    // https://www.cnblogs.com/hexu105/p/7975922.html
    spark.sparkContext.setCheckpointDir("hdfs:///jar/check_point_directory/ALS")

    // load data
    //    val resRating = yelp_data(spark, "reviews").select("user_id", "business_id", "stars").distinct().limit(500) // local mode
    val resRating = spark.read.load(f"hdfs://$NAME_NODE:8020/YelpParquet/rating.parquet")
      .dropDuplicates("text")
      .select("user_id", "business_id", "stars")
      .filter(col("user_id").isNotNull)
      .filter(col("business_id").isNotNull)
      .filter(col("stars").isNotNull)
      .distinct()
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

    // 參數選擇及最佳化
    adjustALSParam(trainRDD, testRDD)

    // TODO close environment
    spark.close()
  }

  def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]): Unit = {
    //    val rankArray = Array(20,50,100)
    val lambdaArray = LAMBDA_ARRAY
    val rankArray = RANK_ARRAY
    //    val lambdaArray = LAMBDA_ARRAY.toArray
    val iterationArray = ITERATION_ARRAY
    println("start adjust ALS parameter:")
    val result = for (rank <- rankArray; lambda <- lambdaArray; iteration <- iterationArray)
      yield {
        val model = ALS.train(trainData, rank, iteration, lambda)
        val rmse = getRMSE(model, testData)
        (rank, lambda, iteration, rmse)
      }
    result.foreach(println)
    //print optimized result
    println(f"optimized parameter (rank, lambda, iteration) and rmse:" + result.minBy(_._4))
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
