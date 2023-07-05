package com.xenonstack.amarjit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}

object Task {
  def main(args: Array[String]): Unit = {
    // Creating a spark session
    val spark = SparkSession.builder()
      .appName("ImplementationTaskTrialOne")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Loading the CSV path
    val csvPath = "/Users/amarjitkumar/Downloads/T1.csv"

    // Defining the schema
    val csvSchema = StructType(Seq(
      StructField("Date/Time", StringType),
      StructField("LV ActivePower (kW)", DoubleType),
      StructField("Wind Speed (m/s)", DoubleType),
      StructField("Theoretical_Power_Curve (KWh)", DoubleType),
      StructField("Wind Direction (°)", DoubleType)
    ))

    val checkpointPath = "/Users/amarjitkumar/Downloads"

    // Reading the data along with the header
    val csvStream = spark.readStream
      .option("header", "true")
      .schema(csvSchema)
      .csv(csvPath)

    // Kafka Connection
    val kafkaBootstrapServer = "localhost:9092"
    val topicName = "review-topic"

    // Producing data to topic in streaming fashion
    val writeToTopic = csvStream
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServer)
      .option("checkpointLocation", "/Users/amarjitkumar/Downloads")
      .option("topic", topicName)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    // Consuming data from topic in streaming fashion
    val dfKafka = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServer)
      .option("subscribe", topicName)
      .option("startingOffsets", "latest")
      .load()

    // Writing the data to Delta Table with a specific schema
    val deltaTablePath = "/Users/amarjitkumar/Downloads/Table"
    //val checkpointPath = "/Users/amarjitkumar/Downloads"
    // spark.conf.set("spark.sql.streaming.checkpointLocation", s"$deltaTablePath/checkpoint")

    dfKafka
      .selectExpr("CAST(value AS STRING) AS json")
      .selectExpr("from_json(json, 'signal_date DATE, signal_ts TIMESTAMP, signals MAP<STRING, STRING>') AS data")
      .select("data.*")
      .withColumn("create_date", current_date())
      .withColumn("create_ts", current_timestamp())
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("path", deltaTablePath)
      .option("checkpointLocation", checkpointPath)
      .start()

    // Read the data from Delta table
    val dfDelta = spark.read.format("delta").load(deltaTablePath)

    // Calculate the number of datapoints per day on distinct signal_ts
    val dfDeltaCount = dfDelta
      .groupBy(col("signal_date"))
      .agg(expr("count(DISTINCT signal_ts) AS datapoints_per_day"))

    // Calculating average value of all the signals per hour
    val dfDeltaAvg = dfDelta
      .withColumn("signal_hour", date_trunc("hour", col("signal_ts")))
      .groupBy(col("signal_hour"))
      .agg(
        avg(col("signals['LV ActivePower (kW)']")).as("avg_LV_ActivePower"),
        avg(col("signals['Wind Speed (m/s)']")).as("avg_Wind_Speed"),
        avg(col("signals['Theoretical_Power_Curve (KWh)']")).as("avg_Theoretical_Power_Curve"),
        avg(col("signals['Wind Direction ()']")).as("avg_Wind_Direction")
      )

    // Adding a column for generation_indicator based on LV ActivePower
    val dfDeltaInd = dfDeltaAvg
      .withColumn("generation_indicator",
        when(col("avg_LV_ActivePower") < 200, "Low")
          .when(col("avg_LV_ActivePower") >= 200 && col("avg_LV_ActivePower") < 600, "Medium")
          .when(col("avg_LV_ActivePower") >= 600 && col("avg_LV_ActivePower") < 1000, "High")
          .otherwise("Exceptional")
      )

    // Creating a DataFrame with the provided JSON
    val jsonSeq = Seq(
      ("LV ActivePower (kW)", "active_power_average"),
      ("Wind Speed (m/s)", "wind_speed_average"),
      ("Theoretical_Power_Curve (KWh)", "theo_power_curve_avg"),
      (" Wind Direction", "wind_direction_average")
    )
    val dfJson = spark.createDataFrame(jsonSeq).toDF("sig_name", "sig_mapping_name")

    // Performing a broadcast join to change signal names based on JSON data
    val dfDeltaFinal = dfDeltaInd.join(broadcast(dfJson),
      dfDeltaInd("generation_indicator") === dfJson("sig_name"),
      "left")
      .drop("generation_indicator", "sig_name")
      .withColumnRenamed("sig_mapping_name", "generation_indicator")

    // Display the final DataFrame
    dfDeltaFinal.show()

    // Start the streaming query
    spark.streams.awaitAnyTermination()
  }
}
