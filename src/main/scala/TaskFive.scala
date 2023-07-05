package com.xenonstack.amarjit

// Importing required libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.types._
object TaskFive {
  def main(args: Array[String]): Unit={

    // Creating a SparkSession
    val spark = SparkSession.builder()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("TaskFive")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Kafka Configuration
    val kafkaBootstrapServer = "localhost:9092"
    val topicName = "reviewtest-topic"

    // Defining a custom schema as for a streaming DataFrame it is necessary
    val customSchema = StructType(Seq(
      StructField("Date/Time", StringType),
      StructField("LV ActivePower (kW)", DoubleType),
      StructField("Wind Speed (m/s)", DoubleType),
      StructField("Theoretical_Power_Curve (KWh)", DoubleType),
      StructField("Wind Direction (°)", DoubleType)
    ))

    // Defining the CSV path : Note the * after T1
    val csvPath = "file:///Users/amarjitkumar/Downloads/CSV/T1*.csv"

    // Reading the CSV file in a streaming fashion
    val csvDF = spark.readStream.format("csv")
      .option("header", "true") // If CSV file has a header
      .schema(customSchema)
      .load(csvPath)


    // Publishing data to kafka topic in a streaming fashion
    val topicDF = csvDF
      .selectExpr("CAST(`Date/Time` AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServer)
      .option("topic", topicName)
      .option("checkpointLocation", "/Users/amarjitkumar/Downloads/Checkpoint")
      .trigger(Trigger.ProcessingTime("1 seconds")) // Trigger interval
      .start()

    // Consuming data from kafka in a streaming manner

    val readDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServer)
      .option("subscribe", topicName)
      .option("startingOffsets", "latest")
      //This was added
      .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") // Specify the value deserializer
      .load()
      // This was added
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    // Explicit deserialization
    /*val readDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServer)
      .option("subscribe", topicName)
      .option("startingOffsets", "latest")
      .option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") // Specify the key deserializer
      .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") // Specify the value deserializer
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")*/

    // This was excluded and replaced by the above statement
    /*readDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]*/

    // Working perfectly till here along with topicDF.awaitTermination() just after this

    // Defining the schema for Delta format
    val deltaSchema = StructType(Seq(
      StructField("signal_date", DateType),
      StructField("signal_ts", TimestampType),
      StructField("signals", MapType(StringType, DoubleType)),
      StructField("create_date", DateType),
      StructField("create_ts", TimestampType)
    ))

    // Converting the consumed data to the Delta format
    val transformedDF = readDF
      .withColumn("jsonData", from_json($"value", customSchema))
      .select(
        $"key".cast(StringType).as("signal_date"),
        $"jsonData.*",
        current_date().as("create_date"),
        current_timestamp().as("create_ts")
      )
      .withColumn("signal_ts", to_timestamp($"signal_date", "dd MM yyyy HH:mm"))
      .withColumn("signals", map(
        lit("LV ActivePower (kW)"), $"LV ActivePower (kW)",
        lit("Wind Speed (m/s)"), $"Wind Speed (m/s)",
        lit("Theoretical_Power_Curve (KWh)"), $"Theoretical_Power_Curve (KWh)",
        lit("Wind Direction (°)"), $"Wind Direction (°)"
      ))
      .select("signal_date", "signal_ts", "signals", "create_date", "create_ts")

    // Writing the transformed data to Delta format
    val deltaPath = "/Users/amarjitkumar/Downloads/Table"
    transformedDF.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", "/Users/amarjitkumar/Downloads/Checkpoint/")
      .start(deltaPath)

    spark.streams.awaitAnyTermination()

    // Writing the data to Delta Table with a specific schema
    /*val deltaTablePath = "/Users/amarjitkumar/Downloads/Table/"

    readDF
      .selectExpr("CAST(value AS STRING) AS json")
      .selectExpr("from_json(json, 'signal_date DATE, signal_ts TIMESTAMP, signals MAP<STRING, STRING>') AS data")
      .select("data.*")
      .withColumn("create_date", current_date())
      .withColumn("create_ts", current_timestamp())
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("path", deltaTablePath)
      .option("checkpointLocation", "/Users/amarjitkumar/Downloads/Checkpoint/")
      .start()*/

    //topicDF.awaitTermination()


  }
}
