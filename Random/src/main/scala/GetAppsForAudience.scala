package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.hadoop.conf.Configuration

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object GetAppsForAudience {

  def get_data_audiences(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      country: String
  ): DataFrame = {
    /// Configuraciones de spark
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    /// Obtenemos la data de los ultimos ndays
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(since)

    val days =
      (0 until ndays).map(start.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_audiences_streaming/"
    val hdfs_files = days
      .flatMap(
        day =>
          (0 to 23).map(
            hour => path + "/hour=%s%s/country=%s".format(day, hour, country)
          )
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val audiences =
      spark.read.option("basePath", path).parquet(hdfs_files: _*)

    audiences.select("device_id", "app_installed").na.drop
  }

  /**
    * GET URL DATA FOR A GIVEN AUDIENCE
    */
  def getAppsForAudience(spark: SparkSession, path: String, country: String) = {
    val audience = spark.read
      .option("sep", "\t")
      .format("csv")
      .load(path)
      .withColumnRenamed("_c1", "device_id")
      .withColumnRenamed("_c2", "ids")
      .drop("_c0")

    val apps =
      get_data_audiences(spark, 30, 1, country)

    audience
      .join(apps, Seq("device_id"))
      .write
      .format("parquet")
      .mode("overwrite")
      .save("/datascience/custom/%s_apps".format(path.split("/").last))
  }

  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    getAppsForAudience(
      spark = spark,
      "/datascience/geo/reports/GCBA/carteles_GCBA_devices_type",
      "AR"
    )

  }
}
