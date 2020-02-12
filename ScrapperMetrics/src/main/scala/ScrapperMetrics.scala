package main.scala
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.log4j.{Level, Logger}

/**
  * The idea of this script is to Ingest Urls daily to local servers for Scrapper.
  */
object ScrapperMetrics {

  def get_metrics(spark: SparkSession) {

    val actual_date = DateTime.now.toString("yyyyMMdd")

    val count = spark.read.format("parquet").load("/datascience/data_keywords/day=20200209").count()
    val domains = spark.read.format("parquet").load("/datascience/data_keywords/day=20200209").select("domain").distinct().count()
    val devices = spark.read.format("parquet").load("/datascience/data_keywords/day=20200209").select("device_id").distinct().count()
    val keywords = spark.read.format("parquet").load("/datascience/data_keywords/day=20200209").select("content_keys").distinct().count()

    val df = List((count,domains,devices,keywords,actual_date)).toDF("count", "domains","devices","keywords","day")
    df.write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .partitionBy("day")
        .save("/datascience/scrapper_metrics/")
    }

  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(Args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("ScrapperMetrics")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    get_metrics(spark)

  }
}
