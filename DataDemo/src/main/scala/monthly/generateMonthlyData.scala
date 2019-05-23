package main.scala.monthly
import main.scala.features.GenerateTriplets
import main.scala.features.DataGoogleAnalytics
import main.scala.features.GenerateDatasets

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
    val ndays = 30
    val country = "MX"
    val name_training = "training_set"
    val name_expansion = "expansion_set"

    // Path con data del devicer
    val path = "/datascience/devicer/processed/AR_xd-0_partner-_pipe-0_2019-04-09T18-18-41-066436_grouped/"
    val filename_path = "/data/metadata/20190316-paths-counts.tsv"
    val filename_domain = "/data/metadata/20190316-domains-counts.tsv"


    val triplets_segments = GenerateTriplets.generate_triplets_segments(spark, ndays)
    val triplets_keywords = GenerateTriplets.generate_triplets_keywords(spark, ndays)

    val ga_domain = DataGoogleAnalytics.get_data_google_analytics(spark, ndays, filename_domain)
    val ga_path = DataGoogleAnalytics.get_data_google_analytics_path(spark, ndays, filename_path)    

    val training = GenerateDatasets.getTrainingData(spark, path, country, name_training)
    val expansion = GenerateDatasets.getExpansionData(spark, path, country, name_expansion)
  }
}
