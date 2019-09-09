package main.scala

import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.conf.Configuration

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object MatchingEquifaxLatam {
  def getDataUrls(spark: SparkSession, nDays: Int, from: Int): DataFrame = {
    // Setting the days that are going to be loaded
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(from)
    val days = (0 until nDays).map(start.minusDays(_)).map(_.toString(format))

    // Now we gather all the paths
    val paths = days
      .map(
        day => "/datascience/data_demo/data_urls/day=%s*".format(day)
      )

    // Finally we load all the data
    val data = spark.read
      .option("basePath", "/datascience/data_demo/data_urls/")
      .parquet(paths: _*)

    data
  }

  def get_data_user_agent(
      spark: SparkSession,
      ndays: Int,
      since: Int
  ): DataFrame = {
    // Spark configuration
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until ndays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_useragents"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    df
  }

  def main(args: Array[String]) {
    // Spark configuration
    val spark = SparkSession.builder
      .appName("Data URLs: Dataset triplets <User, URL, count>")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    // val urls = getDataUrls(spark, 30, 4)
    val equifax_match = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/aud_banco_ciudad_cookies_matched.csv")
      .withColumnRenamed("cookie", "device_id")

    // urls
    //   .filter("country IN ('AR', 'CL', 'MX', 'PE', 'CO')")
    //   .join(equifax_match, Seq("device_id"))
    //   .select("device_id", "pii", "pii_type", "url", "country")
    //   .write
    //   .format("csv")
    //   .mode("overwrite")
    //   .save("/datascience/custom/aud_banco_ciudad_cookies_con_urls")

    val user_agents = get_data_user_agent(spark, 30, 4)

    user_agents
      .filter("country IN ('AR', 'CL', 'MX', 'PE', 'CO')")
      .join(equifax_match, Seq("device_id"))
      .drop("url")
      .write
      .format("csv")
      .mode("overwrite")
      .save("/datascience/custom/aud_banco_ciudad_cookies_con_useragents")
  }
}
