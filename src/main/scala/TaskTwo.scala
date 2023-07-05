package com.xenonstack.amarjit

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
object TaskTwo {
  def main(args:Array[String]):Unit={

    // Creating a SparkSession
    val spark = SparkSession
      .builder()
      .appName("TaskTwo")
      .master("local[*]")
      .getOrCreate()

    // Defining the CSV Schema (necessary in Spark Structured Streaming
    val schema = StructType(Seq(
      StructField("DateTime", StringType),
      StructField("LV_ActivePower", DoubleType),
      StructField("WindSpeed", DoubleType),
      StructField("Theoretical_Power_Curve", DoubleType),
      StructField("WindDirection", DoubleType)
    ))

    // Reading the data in streaming fashion
    val csvStream :DataFrame = spark.readStream
      .schema(schema)
      .csv("/Users/amarjitkumar/Downloads/T1.csv")

    // Kafka Connection
    val kafkaBootstrapServer = "localhost:9092"
    val topicName = "review-topic"

    // Writing data to kafka
    val writeToTopic = csvStream
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServer)
      .option("topic", topicName)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    // Reading from topic
    val readFromTopic = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServer)
      .option("topic", topicName)
      .option("includeHeaders", "true")
      .load()
  }


}
