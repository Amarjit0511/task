package com.xenonstack.amarjit

// Importing required libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DataType

object TaskSeven {
  def main(args: Array[String]): Unit = {

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
    val topicName = "reviewtestthree-topic"

    // Defining a custom schema as for a streaming DataFrame it is necessary
    val customSchema = StructType(Seq(
      StructField("Date/Time", StringType),
      StructField("LV ActivePower (kW)", DoubleType),
      StructField("Wind Speed (m/s)", DoubleType),
      StructField("Theoretical_Power_Curve (KWh)", DoubleType),
      StructField("Wind Direction (°)", DoubleType)
    ))
    //customSchema.printTreeString() -- NOT SUPPORTED

    // Defining the CSV path : Note the * after T1
    val csvPath = "/Users/amarjitkumar/Downloads/CSV/T1*.csv"

    // Reading the CSV file in a streaming fashion
    val csvDF = spark.readStream.format("csv")
      .option("header", "true") // If CSV file has a header
      .option("inferSchema", "false")
      .schema(customSchema)
      .load(csvPath)

    // Publishing data to kafka topic in a streaming fashion
    val topicDF = csvDF
      .selectExpr("CAST(`Date/Time` AS STRING) AS key", "concat(*) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServer)
      .option("topic", topicName)
      .option("checkpointLocation", "/Users/amarjitkumar/Downloads/CheckpointKafka/")
      .option("value.serializer", "org.apache.kafka.common.serialization.StringSerializer") // Specify the value serializer
      .trigger(Trigger.ProcessingTime("10 seconds")) // Trigger interval
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

    val consoleQuery = readDF.writeStream
      .format("console")
      .outputMode("append")
      .start()


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

    //transformedDF.show() --Not allowed because this is a streaming query

    // Writing the transformed data to Delta format
    val deltaPath = "/Users/amarjitkumar/Downloads/Tables"
    transformedDF.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", "/Users/amarjitkumar/Downloads/CheckpointDelta/")
      .start(deltaPath)

    // The code is working perfect till here with spark.streams.awaitAnyTermination() just after it.
    // Also, note that for the code to run perfectly, start afresh

    // Reading the data from delta table
    val deltaDF = spark.read.format("delta").load(deltaPath)

    spark.conf.set("spark.sql.repl.eagerEval.maxNumToStringFields", "400")

    // Display the entire column
    deltaDF.show(truncate = false)

    // Calculate the number of datapoints per day on distinct signal_ts
    val datapointsPerDay = deltaDF
      .groupBy(date_trunc("day", col("signal_ts")).alias("date"))
      .agg(countDistinct("signal_ts").alias("datapoints_per_day"))

    datapointsPerDay.show()


    // Calculating the average value of all the signals per hour
    val averageSignalsPerHour = deltaDF
      .groupBy(window(col("signal_ts"), "1 hour").alias("hour"))
      .agg(
        avg("signals.LV ActivePower (kW)").alias("average_LV_ActivePower"),
        avg("signals.Wind Speed (m/s)").alias("average_Wind_Speed"),
        avg("signals.Theoretical_Power_Curve (KWh)").alias("average_Theoretical_Power_Curve"),
        avg("signals.Wind Direction (°)").alias("average_Wind_Direction"))

    averageSignalsPerHour.show()

    // Adding the generation_indicator column to the DataFrame
    val updatedDF = deltaDF.withColumn("generation_indicator", when(col("signals.LV ActivePower (kW)") < 200, "Low")
      .when(col("signals.LV ActivePower (kW)") < 600, "Medium")
      .when(col("signals.LV ActivePower (kW)") < 1000, "High")
      .otherwise("Exceptional"))

    updatedDF.show()

    // Creating a DataFrame with the provided JSON:
    val json =
      """
    [
      {
        "sig_name": "LV ActivePower (kW)",
        "sig_mapping_name": "active_power_average"
      },
      {
        "sig_name": "Wind Speed (m/s)",
        "sig_mapping_name": "wind_speed_average"
      },
      {
        "sig_name": "Theoretical_Power_Curve (KWh)",
        "sig_mapping_name": "theo_power_curve_average"
      },
      {
        "sig_name": "Theoretical_Power_Curve (KWh)",
        "sig_mapping_name": "theo_power_curve_average"
      },
      {
        "sig_name": "Wind Direction (°)",
        "sig_mapping_name": "wind_direction_average"
      }
    ]
    """

    val jsonDF = spark.read.json(Seq(json).toDS())

    jsonDF.show()





    //spark.streams.awaitAnyTermination()
  }

}
