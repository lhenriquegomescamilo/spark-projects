package main.scala
import main.scala.crossdevicer.AudienceCrossDevicer

import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.joda.time.{Days, DateTime}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.log4j.{Level, Logger}

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object UniqueCalculator {

def getTiersCountry(
      spark: SparkSession,
      country: String,
      nDays: String,
      since: String) = {

    val segments = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/misc/taxo_gral.csv")
      .select("seg_id")
      .collect()
      .map(_(0).toString.toInt)
      .toSeq

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_triplets/segments"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day,country)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val tierUDF = udf(
      (count: Int) =>
        if (count <= 50000) "1"
        else if (count >= 500000) "3"
        else "2"
    )

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select("day","feature", "count")
      .withColumnRenamed("feature", "segment")
      .filter(col("segment").isin(segments: _*))
      .groupBy("segment")
      .agg(sum(col("count")).as("count"))
      .withColumn("tier", tierUDF(col("count")))
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .partitionBy("day","country","tier")
      .save("/datascience/reports/unique_users/tiers")
  }

  def getUsersWithTier(spark: SparkSession) {
    // select n segments from each tier
    val segments = List(
      "24621",
      "24666",
      "24692",
      "1350",
      "743",
      "224",
      "104619",
      "99638",
      "48334",
      "432",
      "1087",
      "1341",
      "99644",
      "48398",
      "463"
    )
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(0)
    val days = (0 until 10).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_triplets/segments"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=AR".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val udfTier = udf(
      (segment: String) =>
        if (List("24621", "24666", "24692", "1350", "743").contains(segment))
          "tier_1"
        else if (List("224", "104619", "99638", "48334", "432")
                   .contains(segment)) "tier_2"
        else "tier_3"
    )
    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select("device_id", "feature", "count")
      .withColumnRenamed("feature", "segment")
      .filter(col("segment").isin(segments: _*))
      .withColumn("tier", udfTier(col("segment")))
      .select("device_id", "segment", "tier")
      .distinct()
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/users_report_uniques")
  }
  def get_pii_matching_user_report(spark: SparkSession) {
    val pii = spark.read.load("/datascience/pii_matching/pii_tuples/")
    val df = spark.read
      .format("csv")
      .load("/datascience/custom/users_report_uniques")
      .withColumnRenamed("_c0", "device_id")
      .withColumnRenamed("_c1", "segment")
      .withColumnRenamed("_c2", "tier")

    pii
      .join(df, Seq("device_id"), "inner")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/report_user_unique_pii")

  }
 
  def temp(spark: SparkSession) {
    val nids = spark.read
      .load("/datascience/custom/report_user_unique_pii")
      .select("nid_sh2", "tier")
      .distinct
    val pii = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .select("device_id", "nid_sh2")
      .distinct

    nids
      .join(pii, Seq("nid_sh2"), "inner")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/devices_originales")

  }

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     MAIN METHOD     //////////////////////
    *
    */




 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark = SparkSession.builder
    .appName("UniqueCalculator")
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .getOrCreate()



  }
}