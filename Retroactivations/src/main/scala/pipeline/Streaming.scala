package main.scala.pipeline

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

object Streaming {

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("Eventqueue Streaming")
      .config("spark.sql.streaming.pollingDelay", 4000).getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    val data = spark.readStream
      .option("sep", "\t")
      .option("header", "true")
      .format("csv")
      .load("/data/eventqueue/2019/06/06/")

    val ints =
      "created id_partner id_segment_source share_data tagged click_count conversion_count impression_count activable job_id"
        .split(" ")
        .toSeq
    val doubles =
      "latitude longitude accuracy altitude altaccuracy".split(" ").toSeq
    val array_strings = "tags app_data app_installed".split(" ").toSeq
    val array_ints =
      "segments clusters first_party second_party third_party all_clusters all_segments all_segments_xd gt removed_segments platforms"
        .split(" ")
    val longs = "ip".split(" ").toSeq

    val withArrayStrings = array_strings.foldLeft(data)(
      (df, c) => df.withColumn(c, split(col(c), "\u0001"))
    )
    val withInts = ints.foldLeft(withArrayStrings)(
      (df, c) => df.withColumn(c, col(c).cast("int"))
    )
    val withDoubles = doubles.foldLeft(withInts)(
      (df, c) => df.withColumn(c, col(c).cast("double"))
    )
    val withLongs = longs.foldLeft(withDoubles)(
      (df, c) => df.withColumn(c, col(c).cast("long"))
    )
    val finalDF = array_ints.foldLeft(withLongs)(
      (df, c) =>
        df.withColumn(c, split(col(c), "\u0001"))
          .withColumn(c, col(c).cast("array<int>"))
    )

    val query = finalDF
      .coalesce(1)
      .withColumn("day", lit("20190606"))
      .writeStream
      .outputMode("append")
      .format("parquet")
      .partitionBy("day", "country")
      .option("path", "/datascience/data_audiences_streaming/")
      .start()

    query.awaitTermination()
  }
}
