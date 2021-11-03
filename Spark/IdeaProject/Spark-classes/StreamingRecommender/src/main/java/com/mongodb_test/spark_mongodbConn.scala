package com.mongodb_test

import org.apache.spark.sql.SparkSession
import org.bson.Document
import org.bson.types.ObjectId
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.toDocumentRDDFunctions

object spark_mongodbConn {
  def main(args: Array[String]): Unit = {
    /* For Self-Contained Scala Apps: Create the SparkSession
     * CREATED AUTOMATICALLY IN spark-shell */
    val sparkSession = SparkSession.builder()
    .master("local")
      .appName("MongoSparkConnectorIntro")
//      .config("spark.mongodb.input.uri", "mongodb://10.120.30.118:27017/test.students")
//      .config("spark.mongodb.output.uri", "mongodb://10.120.30.118:27017/test.character")
      .getOrCreate()

    val docs = """
      {"name": "Bilbo Baggins", "age": 50}
      {"name": "Gandalf", "age": 1000}
      {"name": "Thorin", "age": 195}
      {"name": "Balin", "age": 178}
      {"name": "Kíli", "age": 77}
      {"name": "Dwalin", "age": 169}
      {"name": "Óin", "age": 167}
      {"name": "Glóin", "age": 158}
      {"name": "Fíli", "age": 82}
      {"name": "Bombur"}"""
      .trim.stripMargin.split("[\\r\\n]+").toSeq
//    sparkSession.sparkContext.parallelize(docs.map(Document.parse)).saveToMongoDB()


    sparkSession.close()
  }

}
