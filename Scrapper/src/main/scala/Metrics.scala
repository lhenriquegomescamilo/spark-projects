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
object Metrics {

  def get_metrics(spark: SparkSession,conf:Configuration) {

    val actual_date = DateTime.now().minusDays(2).toString("yyyyMMdd")

    val count = spark.read.format("parquet")
                    .load("/datascience/data_keywords/day=%s".format(actual_date))
                    .count()
    val domains = spark.read
                        .format("parquet")
                        .load("/datascience/data_keywords/day=%s".format(actual_date))
                        .select("domain")
                        .distinct()
                        .count()
    val devices = spark.read.format("parquet")
                        .load("/datascience/data_keywords/day=%s".format(actual_date))
                        .select("device_id")
                        .distinct()
                        .count()
    val keywords = spark.read.format("parquet")
                        .load("/datascience/data_keywords/day=%s".format(actual_date))
                        .select("content_keys")
                        .distinct()
                        .count()

    
    var fs = FileSystem.get(conf)
    var os = fs.create(new Path("/datascience/scrapper_metrics/%s.json".format(actual_date)))

    val content = """{"count": %s, "domains": %s, "devices": %s, "keywords": %s}""".format(count.toString,
                                                                                        domains.toString,
                                                                                        devices.toString,
                                                                                        keywords.toString)

    os.write(content.getBytes)
    fs.close()

    }

  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(Args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("Scrapper Metrics")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://rely-hdfs")
    get_metrics(spark,conf)

  }
}
