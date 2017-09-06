package com.knoldus

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming extends App{

  Logger.getLogger("org").setLevel(Level.OFF)
  val logger = Logger.getLogger("Spark")

  val sparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  val streamingContext = new StreamingContext(sparkConf,Seconds(2))
  val data = streamingContext.receiverStream(new CustomReceiver)
  val stream = data.map(_.split(" ").length)

  stream.foreachRDD(col => println(col.collect().toList))
  streamingContext.start()
  streamingContext.awaitTermination()
}
