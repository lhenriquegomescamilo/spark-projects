package main.scala.pipeline

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.ProcessingTime

object Streaming {

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Eventqueue Streaming")
        .config("spark.sql.streaming.pollingDelay", 1000)
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    val columns =
      """device_id, id_partner, event_type, device_type, segments, first_party, all_segments, url, referer, 
                     search_keyword, tags, track_code, campaign_name, campaign_id, site_id, 
                     placement_id, advertiser_name, advertiser_id, app_name, app_installed, 
                     version, country, activable"""
        .replace("\n", "")
        .replace(" ", "")
        .split(",")
        .toList
    val event_types = List(
      "tk",
      "pv",
      "data",
      "batch",
      "sync",
      "xp",
      "retroactive",
      "xd",
      "xd_xp"
    )

    var finalSchema = columns.foldLeft(new StructType())(
      (schema, col) => schema.add(col, "string")
    )

    val ints =
      "id_partner activable"
        .split(" ")
        .toSeq
    val array_strings = "tags app_installed".split(" ").toSeq
    val array_ints =
      "segments first_party all_segments"
        .split(" ")

    val data = spark.readStream
      .option("sep", "\t")
      .option("header", "true")
      .schema(finalSchema)
      .format("csv")
      .load("/data/eventqueue/2019/06/06/")

    val withArrayStrings = array_strings.foldLeft(data)(
      (df, c) => df.withColumn(c, split(col(c), "\u0001"))
    )
    val withInts = ints.foldLeft(withArrayStrings)(
      (df, c) => df.withColumn(c, col(c).cast("int"))
    )
    val finalDF = array_ints
      .foldLeft(withInts)(
        (df, c) =>
          df.withColumn(c, split(col(c), "\u0001"))
            .withColumn(c, col(c).cast("array<int>"))
      )
      .filter(
        length(col("device_id")) > 0 && col("event_type").isin(event_types: _*)
      )

    val query = finalDF
      .withColumn("day", lit("20190606"))
      .coalesce(8)
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("checkpointLocation", "/datascience/checkpoint/")
      .partitionBy("day", "country")
      .option("path", "/datascience/data_audiences_streaming/")
      .trigger(ProcessingTime("1260 seconds"))
      .start()

    query.awaitTermination()
  }
}
