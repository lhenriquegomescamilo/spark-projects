package main.scala.monthly
import main.scala.features.GenerateTriplets
import main.scala.features.DataGoogleAnalytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}

object GenerateMonthlyFeatures{
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("Monthly Features Download")
      .getOrCreate()

    // Parameters
    val ndays = if (args.length > 0) args(0).toInt else 30

    // Path con data del devicer
    val filename_path = "/data/metadata/20190316-paths-counts.tsv"
    val filename_domain = "/data/metadata/20190316-domains-counts.tsv"

    val triplets_segments = GenerateTriplets.generate_triplets_segments(spark, ndays)
    //val triplets_keywords = GenerateTriplets.generate_triplets_keywords(spark, ndays)
    println("LOGGER: Triplets segments generated")

    val ga_domain = DataGoogleAnalytics.get_data_google_analytics(spark, ndays, filename_domain)
    //val ga_path = DataGoogleAnalytics.get_data_google_analytics_path(spark, ndays, filename_path)    
    println("LOGGER: GA data generated")

  }
}
