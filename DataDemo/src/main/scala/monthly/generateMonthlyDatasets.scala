package main.scala.monthly
import main.scala.features.GenerateDataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.log4j.{Level, Logger}


object GenerateMonthlyDataset{
  def main(args: Array[String]) {
    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder
      .appName("Monthly Data Download")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .getOrCreate()

    // Parameters
    val country = if (args.length > 0) args(0).toString else ""
    val training_name = if (args.length > 1) args(1).toString else "training_set"
    val expansion_name = if (args.length > 2) args(2).toString else "expansion_set"
    val path_gt = if (args.length > 3) args(3).toString else "/datascience/devicer/processed/AR_xd-0_partner-_pipe-0_2019-04-09T18-18-41-066436_grouped/"
    
    val training = GenerateDataset.getTrainingData(spark, path_gt, country, training_name)
    //val expansion = GenerateDataset.getExpansionData(spark, path_gt, country, expansion_name)
  }
}
