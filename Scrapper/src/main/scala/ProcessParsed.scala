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
object ProcessParsed {
  def process_parsed(spark: SparkSession) {

    val date = DateTime.now().toString("yyyyMMdd")
    val hour = DateTime.now().getHourOfDay()
    spark.read.format("csv")
          .option("sep","\t")
          .option("header","true")
          .load("/datascience/scraper/parsed/to_process/*")
          .withColumn("day",lit(date))
          .withColumn("hour",lit(hour))
          .orderBy(col("url").asc)
          .write
          .format("parquet")
          .mode("append")
          .partitionBy("day","hour")
          .save("/datascience/scraper/parsed/processed")

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
      .appName("Process Parsed Dump")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    process_parsed(spark)

  }
}
