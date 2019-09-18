package main.scala

import org.apache.spark.sql.{SparkSession, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}

object UntaggedURLs {

  def processDay(spark: SparkSession, day: String) = {
    val data = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/data/eventqueue/%s/".format(day))

    data
      .select("url", "tagged", "share_data", "event_type", "country")
      .filter(
        "event_type IN ('pv', 'batch', 'data') AND tagged IS NULL AND share_data == '1' AND url IS NOT NULL"
      )
      .select("url", "country")
      .withColumn("day", lit(day.replace("/", "")))
      .write
      .format("parquet")
      .partitionBy("day", "country")
      .mode("append")
      .save("/datascience/data_url_classifier/untagged_urls/")
  }

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    val since = 1
    val nDays = 1

    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    days.map(processDay(spark, _))
  }
}
