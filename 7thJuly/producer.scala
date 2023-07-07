package com.amarjit.task

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Producer {

  def main(args: Array[String]): Unit = {

    // Creating a SparkSession
    val spark = SparkSession.builder()
      .appName("Producer Task")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    // Kafka Configuration
    val kafkaBootstrapServer = "localhost:9092"
    val topicName = "two-topic"

    // Defining a custom schema as for a streaming DataFrame it is necessary
    val customSchema = StructType(Seq(
      StructField("Date/Time", StringType),
      StructField("LV ActivePower (kW)", DoubleType),
      StructField("Wind Speed (m/s)", DoubleType),
      StructField("Theoretical_Power_Curve (KWh)", DoubleType),
      StructField("Wind Direction (°)", DoubleType)
    ))

    // Defining the CSV path
    val csvPath = "file:///Users/amarjitkumar/Downloads/task*.csv"

    // Reading the CSV file in a streaming fashion
    val csvDF = spark.readStream.format("csv")
      .option("header", "true") // If CSV file has a header
      .option("inferSchema", "false")
      .schema(customSchema)
      .load(csvPath)

    // Publishing data to Kafka topic in a streaming fashion
    val topicDF = csvDF
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServer)
      .option("topic", topicName)
      .option("includeHeaders", "true")
      .option("sep", ",")
      .option("checkpointLocation", "/Users/amarjitkumar/Downloads/Checkpoint1/")
      .trigger(Trigger.ProcessingTime("5 seconds")) // Trigger interval
      .start()

      topicDF.awaitTermination()
  }
}
