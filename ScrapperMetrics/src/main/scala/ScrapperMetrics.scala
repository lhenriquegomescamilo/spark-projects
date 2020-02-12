package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import spark.implicits._
import org.apache.spark._
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.functions.{
  upper,
  count,
  col,
  abs,
  udf,
  regexp_replace,
  split,
  lit,
  explode,
  length,
  to_timestamp,
  from_unixtime,
  date_format,
  sum
}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{
 StructType,
 StructField,
 StringType,
 IntegerType
}
import org.apache.spark.sql.{Column, Row}
import scala.util.Random.shuffle

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
