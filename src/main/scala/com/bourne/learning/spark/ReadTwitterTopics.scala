package com.bourne.learning.spark

import org.apache.spark.sql.SparkSession

object ReadTwitterTopics extends App {

  val spark = SparkSession
    .builder()
    .appName("Read Twitter Tweet Topic")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val rawTwitterDF = spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "twitter_tweets_01")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .as[String]

  val jsonDF = spark
    .read
    .json(rawTwitterDF)

  val fewColDF = jsonDF.select("user.name", "user.id", "user.followers_count", "created_at", "id_str", "lang",
    "place", "reply_count", "retweet_count", "text")

  fewColDF.printSchema()
  fewColDF.show(false)
}
