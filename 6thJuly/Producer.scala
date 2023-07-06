package com.xenonstack.amarjit
package Week3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Producer {

  def main(args: Array[String]): Unit = {

    // Creating a SparkSession
    val spark = SparkSession.builder()
      .appName("TaskFive")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    // Kafka Configuration
    val kafkaBootstrapServer = "localhost:9092"
    val topicName = "three-topic"

    // Defining a custom schema as for a streaming DataFrame it is necessary
    val customSchema = StructType(Seq(
      StructField("Date/Time", StringType),
      StructField("LV ActivePower (kW)", DoubleType),
      StructField("Wind Speed (m/s)", DoubleType),
      StructField("Theoretical_Power_Curve (KWh)", DoubleType),
      StructField("Wind Direction (°)", DoubleType)
    ))

    // Defining the CSV path : Note the * after T1
    val csvPath = "/home/xs334-amakum/Downloads/CSV/"

    // Reading the CSV file in a streaming fashion
    val csvDF = spark.readStream.format("csv")
      .option("header", "true") // If CSV file has a header
      .option("inferSchema", "false")
      .schema(customSchema)
      .load(csvPath)


    // Publishing data to kafka topic in a streaming fashion
    val topicDF = csvDF
      .selectExpr( "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServer)
      .option("topic", topicName)
      .option("includeHeaders", "true")
      .option("sep", ",")
      .option("checkpointLocation","/home/xs334-amakum/Downloads/Checkpoint1")
      .trigger(Trigger.ProcessingTime("1 seconds")) // Trigger interval
      .start()

    /*val consoleSink = csvDF.writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation","/home/xs334-amakum/Downloads/Checkpoint2")
      .start()
      .awaitTermination()*/
    /*topicDF.awaitTermination()*/

  }
}

