package com.xenonstack.amarjit

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
object TaskThree {
  def main(args: Array[String]): Unit={

    val spark = SparkSession
      .builder
      .appName("TaskThree")
      .master("local[*]")
      .getOrCreate()

    val socketDF = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    socketDF.isStreaming // Returns True for DataFrames that have streaming sources

    socketDF.printSchema

    // Read all the csv files written atomically in a directory
    val userSchema = new StructType()
      .add("Date/Time", "string")
      .add("LV ActivePower", "double")
      .add("WindSpeed", "double")
      .add("Theoretical_Power_Curve", "double")
      .add("WindDirection", "double")

    val csvDF = spark
      .readStream
      .option("sep", ";")
      .schema(userSchema) // Specify schema of the csv files
      .csv("/Users/amarjitkumar/Downloads/T1.csv") // Equivalent to format("csv").load("/path/to/directory")


  }


}
