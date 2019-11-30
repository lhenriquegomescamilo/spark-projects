package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.ProcessingTime
import java.sql.Date
import java.time.DateTimeException
import org.joda.time.DateTime

object Streaming {

  /**
    * This function takes all the information from the CSV files in the /data/eventqueue folder and generates their corresponding parquet files.
    * It keeps a listener so that for every new file added to the folder it processes it and stores it as a parquet version.
    * The maximum number of files processed in each iteration is 4.
    * It only keeps a selected number of columns.
    *
    * @param spark: Spark session that will be used to load and write the data.
    * @param from: Number of days to be skipped to read the data.
    *
    * As a result this function writes the data in /datascience/data_audiences_streaming/ partitioned by Country and Day.
    */
  def streamCSVs(
      spark: SparkSession,
      from: Integer,
      parallel: Int = 0
  ) = {
    // This is the list of all the columns that each CSV file has.
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

    // This is the list of selected columns.
    val columns =
      """device_id, id_partner, event_type, device_type, first_party, all_segments, country, time"""
        .replace("\n", "")
        .replace(" ", "")
        .split(",")
        .toList

    val blacklist = Seq(1008, 1139, 411, 1, 119, 444, 1072)

    // This is the list of event types that will be considered.
    val event_types = List(
      "tk",
      "pv",
      "data",
      "batch",
      "xp",
      "retroactive",
      "xd",
      "xd_xp"
    )

    // This is the schema that will be used for the set of all columns
    var finalSchema = all_columns.foldLeft(new StructType())(
      (schema, col) => schema.add(col, "string")
    )

    // This is the list of columns that are integers
    val ints =
      "id_partner"
        .split(" ")
        .toSeq

    // List of columns that are arrays of integers
    val array_ints =
      "first_party all_segments"
        .split(" ")

    // Current day
    val day = DateTime.now.minusDays(from).toString("yyyy/MM/dd/")

    // Here we read the pipeline
    val data = spark.read
      .option("sep", "\t")
      .option("header", "true")
      .schema(finalSchema) // Defining the schema
      .format("csv")
      .load("/data/eventqueue/%s".format(day))
      .select(columns.head, columns.tail: _*) // Here we select the columns to work with
      // Now we change the type of the column time to timestamp
      .withColumn(
        "datetime",
        to_utc_timestamp(regexp_replace(col("time"), "T", " "), "utc")
      )
      // Calculate the hour
      .withColumn("hour", date_format(col("datetime"), "yyyyMMddHH"))
      .drop("datetime")

    println("LOGGER:\n\tData: %s".format(data))

    // We do the same with the columns that are integers
    val withInts = ints.foldLeft(data)(
      (df, c) => df.withColumn(c, col(c).cast("int"))
    )

    // Finally, we repeat the process with the columns that are array of integers
    val finalDF = array_ints
      .foldLeft(withInts)(
        (df, c) =>
          df.withColumn(c, split(col(c), "\u0001"))
            .withColumn(c, col(c).cast("array<int>"))
      )

    // Here we do the filtering, where we keep the event types previously specified
    val filtered =
      finalDF
        .filter(
          length(col("device_id")) > 0 && col("event_type")
            .isin(event_types: _*) && !col("id_partner").isin(blacklist: _*)
        )

    println("LOGGER:\n\tFinal DF: %s".format(finalDF))

    // In the last step we write the batch that has been read into /datascience/data_audiences_streaming/ or /datascience/data_partner_streaming/
    val outputPath = "/datascience/data_reporter" + (if (parallel > 0)
                                                                "_%s".format(
                                                                  parallel
                                                                )
                                                              else "")

    filtered.createOrReplaceTempView("temp_data_5")

    spark.table("temp_data_5").orderBy(asc("device_id"))
    filtered.write
      .mode("append")
      .format("parquet")
      .partitionBy("hour", "id_partner")
      .save(outputPath)

  }

  type OptionMap = Map[Symbol, String]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--pipeline" :: value :: tail =>
        nextOption(map ++ Map('pipeline -> value.toString), tail)
      case "--from" :: value :: tail =>
        nextOption(map ++ Map('from -> value.toString), tail)
      case "--type" :: value :: tail =>
        nextOption(map ++ Map('type -> value.toString), tail)
      case "--parallel" :: value :: tail =>
        nextOption(map ++ Map('parallel -> value.toString), tail)
    }
  }

  def main(args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val from = if (options.contains('from)) options('from).toInt else 0
    val parallel =
      if (options.contains('parallel)) options('parallel).toString.toInt else 0

    val spark =
      SparkSession.builder
        .appName("Eventqueue Streaming")
        // .config("spark.sql.streaming.pollingDelay", 1000)
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)
    println(
      "LOGGER:\n\tFrom: %s".format(from)
    )

    streamCSVs(spark, from, parallel)
  }
}
