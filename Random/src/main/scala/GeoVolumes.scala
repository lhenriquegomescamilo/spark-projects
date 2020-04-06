package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.input_file_name

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object GeoVolumes {

  def get_safegraph(
      spark: SparkSession,
      nDays: Int,
      since: Int,
      country: String
  ) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt)
      .map(end.minusDays(_))
      .map(_.toString(format))

    // Now we obtain the list of hdfs files to be read
    val path = "/datascience/geo/safegraph/"
    val hdfs_files = days
      .map(day => path + "day=%s/country=%s/".format(day, country))
      .filter(
        path => fs.exists(new org.apache.hadoop.fs.Path(path))
      )
      .map(day => day + "*.snappy.parquet")

    // Finally we read, filter by country, rename the columns and return the data
    val df_safegraph = spark.read
      .option("header", "true")
      .parquet(hdfs_files: _*)

    df_safegraph

  }

  def getStats(spark: SparkSession, country: String) = {
    val data = get_safegraph(spark, 30, 1, country)

    data
      .unionAll(data.withColumn("day", lit("monthly")))
      .withColumn("country", lit(country))
      .groupBy("day")
      .agg(
        approxCountDistinct("ad_id", 0.02) as "device_unique",
        count("ad_id") as "detections"
      )
      .write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save("/datascience/geo/country_stats/country=%s".format(country))
  }

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    val countries =
      List("AR", "argentina", "CO", "CL", "EC", "BR", "MX", "mexico", "UY", "PY", "PE")

    countries.foreach(country => getStats(spark, country))
  }
}
