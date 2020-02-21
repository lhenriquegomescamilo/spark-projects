package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.apache.spark._
import org.joda.time.DateTime
import org.apache.hadoop.fs.{ FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.apache.spark.sql.{SaveMode, DataFrame}
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
object ProcessRaw {
  def process_raw(spark: SparkSession) {

    val date = DateTime.now().toString("yyyyMMdd")
    spark.read.format("csv")
          .option("sep","\t")
          .option("header","true")
          .load("/datascience/scraper/raw/to_process/*")
          .selectExpr("*", "parse_url(url, 'HOST') as domain")
          .withColumn("day",lit(date))
          .write
          .format("parquet")
          .option("compression","gzip")
          .mode("overwrite")
          .partitionBy("day")
          .save("/datascience/scraper/daily_dump/")

    // Remover files
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://rely-hdfs")
    var fs = FileSystem.get(conf)
    fs.delete(new Path("/datascience/scraper/raw/to_process/*"), true)

    }

  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(Args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("Process Raw Dump")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    process_raw(spark)

  }
}
