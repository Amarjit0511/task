package com.amarjit.task

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object Consumer {

  def main(args: Array[String]): Unit = {

    // Creating a SparkSession
    val spark = SparkSession.builder()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("TaskFive")
      .master("local[*]")
      //.config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    // Kafka Configuration
    val kafkaBootstrapServer = "localhost:9092"
    val topicName = "two-topic"

    // Consuming data from kafka in a streaming manner

    val readDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServer)
      .option("subscribe", topicName)
      .option("startingOffsets", "earliest")
      .option("includeHeaders", "true")
      .option("sep", ",")

      //This was added
      .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") // Specify the value deserializer
      .load()
      // This was added
      .selectExpr("CAST(value AS STRING)")

    readDF.writeStream
      .format("console")
      .outputMode("append")
      .option("startingOffsets", "earliest")
      .trigger(Trigger.ProcessingTime("5 seconds")) // Trigger interval
      .option("maxFilesPerTrigger", 10)
      .option("truncate", "false")
      .start()
      .awaitTermination()

  }
}
