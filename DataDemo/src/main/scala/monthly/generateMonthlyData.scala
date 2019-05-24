package main.scala.monthly
import main.scala.features.GenerateTriplets
import main.scala.features.DataGoogleAnalytics
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

object GenerateMonthlyData{
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("Monthly Data Download")
      .getOrCreate()

    // Parameters
    val ndays = if (args.length > 0) args(0).toInt else 30
    val country = if (args.length > 1) args(1).toString else ""
    val training_name = if (args.length > 2) args(2).toString else "training_set"
    val expansion_name = if (args.length > 3) args(3).toString else "expansion_set"
    val path_gt = if (args.length > 4) args(4).toString else "/datascience/devicer/processed/AR_xd-0_partner-_pipe-0_2019-04-09T18-18-41-066436_grouped/"

    // Path con data del devicer
    val filename_path = "/data/metadata/20190316-paths-counts.tsv"
    val filename_domain = "/data/metadata/20190316-domains-counts.tsv"

    val triplets_segments = GenerateTriplets.generate_triplets_segments(spark, ndays)
    //val triplets_keywords = GenerateTriplets.generate_triplets_keywords(spark, ndays)
    println("LOGGER: Triplets segments generated")

    val ga_domain = DataGoogleAnalytics.get_data_google_analytics(spark, ndays, filename_domain)
    //val ga_path = DataGoogleAnalytics.get_data_google_analytics_path(spark, ndays, filename_path)    
    println("LOGGER: GA data generated")
    
    /val training = GenerateDataset.getTrainingData(spark, path_gt, country, training_name)
    //val expansion = GenerateDataset.getExpansionData(spark, path_gt, country, expansion_name)
  }
}
