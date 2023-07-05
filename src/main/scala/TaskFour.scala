package com.xenonstack.amarjit

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object TaskFour {
  def main(args: Array[String]): Unit={

    val spark = SparkSession
      .builder
      .appName("TaskFour")
      .master("local[*]")
      .getOrCreate()


  }

}
