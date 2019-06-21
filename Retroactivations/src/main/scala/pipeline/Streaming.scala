package main.scala.pipeline

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.ProcessingTime
import java.sql.Date
import java.time.DateTimeException
import org.joda.time.DateTime

object Streaming {

  def streamCSVs(spark: SparkSession) = {
    val all_columns =
      """timestamp,time,user,device_id,device_type,web_id,android_id,ios_id,event_type,data_type,nav_type,
                         version,country,user_country,ip,created,id_partner,id_partner_user,id_segment_source,share_data,
                         segments,clusters,first_party,second_party,third_party,all_clusters,all_segments,all_segments_xd,gt,
                         user_agent,browser,url,secondary_url,referer,sec,tagged,tags,title,category,sub_category,search_keyword,
                         vertical,mb_sh2,mb_sh5,ml_sh2,ml_sh5,nid_sh2,nid_sh5,track_code,track_type,advertiser_id,advertiser_name,
                         campaign_id,campaign_name,placement_id,site_id,click_count,conversion_count,impression_count,app_data,
                         app_installed,app_name,wifi_name,latitude,longitude,accuracy,altitude,altaccuracy,p223,p240,d2,d9,d10,
                         d11,d13,d14,d17,d18,d19,d20,d21,d22,url_subdomain,url_domain,url_path,referer_subdomain,referer_domain,
                         referer_path,d23,removed_segments,activable,platforms,job_id"""
        .replace("\n", "")
        .replace(" ", "")
        .split(",")
        .toList
    val columns =
      """device_id, id_partner, event_type, device_type, segments, first_party, all_segments, url, referer, 
                     search_keyword, tags, track_code, campaign_name, campaign_id, site_id, time,
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

    var finalSchema = all_columns.foldLeft(new StructType())(
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

    val day = DateTime.now.toString("yyyy/MM/dd/")

    val data = spark.readStream
      .option("sep", "\t")
      .option("header", "true")
      .option("maxFilesPerTrigger", 4)
      .schema(finalSchema)
      .format("csv")
      .load("/data/eventqueue/%s".format(day))
      .select(columns.head, columns.tail: _*)
      .withColumn(
        "datetime",
        to_utc_timestamp(regexp_replace(col("time"), "T", " "), "utc")
      )
      .withColumn("hour", date_format(col("datetime"), "yyyyMMddHH"))

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
      // .coalesce(8)
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("checkpointLocation", "/datascience/checkpoint/")
      .partitionBy("hour", "country")
      .option("path", "/datascience/data_audiences_streaming/")
      // .trigger(ProcessingTime("1260 seconds"))
      .start()
      .awaitTermination()
  }

  def streamKafka(spark: SparkSession) = {
    import org.apache.spark.sql.streaming._
    import org.apache.spark.sql._
    import org.apache.spark.sql.types._
    import spark.implicits._

    val all_columns =
      """timestamp,time,user,device_id,device_type,web_id,android_id,ios_id,event_type,data_type,nav_type,
                         version,country,user_country,ip,created,id_partner,id_partner_user,id_segment_source,share_data,
                         segments,clusters,first_party,second_party,third_party,all_clusters,all_segments,all_segments_xd,gt,
                         user_agent,browser,url,secondary_url,referer,sec,tagged,tags,title,category,sub_category,search_keyword,
                         vertical,mb_sh2,mb_sh5,ml_sh2,ml_sh5,nid_sh2,nid_sh5,track_code,track_type,advertiser_id,advertiser_name,
                         campaign_id,campaign_name,placement_id,site_id,click_count,conversion_count,impression_count,app_data,
                         app_installed,app_name,wifi_name,latitude,longitude,accuracy,altitude,altaccuracy,p223,p240,d2,d9,d10,
                         d11,d13,d14,d17,d18,d19,d20,d21,d22,url_subdomain,url_domain,url_path,referer_subdomain,referer_domain,
                         referer_path,d23,removed_segments,activable,platforms,job_id"""
        .replace("\n", "")
        .replace(" ", "")
        .split(",")
        .toList

    val ints =
      "id_partner activable"
        .split(" ")
        .toSeq
    val array_strings = "tags app_installed".split(" ").toSeq
    val array_ints =
      "segments first_party all_segments"
        .split(" ")

    val df = spark.readStream
      .format("kafka")
      .option(
        "kafka.bootstrap.servers",
        "172.18.50.94:9092,172.18.50.95:9092,172.18.50.96:9092,172.18.50.97:9092"
      )
      .option("subscribe", "event_queue")
      .option("kafka.max.partition.fetch.bytes", "52428800")
      .load()

    val csvData = df
      .selectExpr("CAST(value AS STRING)")
      .as[(String)]
      .withColumn("value", split(col("value"), "\t"))

    val frame = all_columns.zipWithIndex
      .foldLeft(csvData)(
        (df, t) => df.withColumn(t._1, col("value").getItem(t._2))
      )
      .withColumn(
        "datetime",
        to_utc_timestamp(regexp_replace(col("time"), "T", " "), "utc")
      )
      .withColumn("hour", date_format(col("datetime"), "yyyyMMddHH"))

    val withArrayStrings = array_strings.foldLeft(frame)(
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
        length(col("device_id")) > 0
      )

    val query = frame.writeStream
      .outputMode("append")
      .format("parquet")
      .option("checkpointLocation", "/datascience/checkpoint/")
      .partitionBy("hour", "country")
      .option("path", "/datascience/data_eventqueue/")
      // .trigger(ProcessingTime("1260 seconds"))
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Eventqueue Streaming")
        // .config("spark.sql.streaming.pollingDelay", 1000)
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    streamCSVs(spark)
  }
}
