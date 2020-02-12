package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object DunnhumbyEnrichment {

  /**
    * This method returns a DataFrame with the data from the partner data pipeline, for the interval
    * of days specified. Basically, this method loads the given path as a base path, then it
    * also loads the every DataFrame for the days specified, and merges them as a single
    * DataFrame that will be returned.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param partnerIds: List of id partners from which we are going to load the data.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.
    *
    * @return a DataFrame with the information coming from the data read.
**/
  def getDataIdPartners(
      spark: SparkSession,
      partnerIds: List[String],
      nDays: Int = 30,
      since: Int = 1,
      pipe: String = "batch"
  ): DataFrame = {
    println("DEVICER LOG: PIPELINE ID PARTNERS")
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path =
      if (pipe == "batch") "/datascience/data_partner/"
      else "/datascience/data_partner_streaming/"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files =
      if (pipe == "batch")
        partnerIds
          .flatMap(
            partner =>
              days
                .map(
                  day => path + "id_partner=" + partner + "/day=%s".format(day)
                )
          )
          .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      else
        partnerIds
          .flatMap(
            partner =>
              days
                .flatMap(
                  day =>
                    (0 until 24).map(
                      hour =>
                        path + "hour=%s%02d/id_partner=%s"
                          .format(day, hour, partner)
                    )
                )
          )
          .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df =
      if (hdfs_files.length > 0) {
        spark.read.option("basePath", path).parquet(hdfs_files: _*)
      } else
        spark.createDataFrame(
          spark.sparkContext.emptyRDD[Row],
          StructType(Array(StructField("empty", StringType, true)))
        )

    df
  }

  /**
    * This method returns a DataFrame with the data from the audiences data pipeline, for the interval
    * of days specified. Basically, this method loads the given path as a base path, then it
    * also loads the every DataFrame for the days specified, and merges them as a single
    * DataFrame that will be returned.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.
    *
    * @return a DataFrame with the information coming from the data read.
  **/
  def getDataAudiences(
      spark: SparkSession,
      nDays: Int = 30,
      since: Int = 1
  ): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_audiences_streaming/"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .flatMap(
        day => (0 to 23).map(hour => path + "/hour=%s%02d".format(day, hour))
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)
    fs.close()

    df
  }

  def getUAEnrichedAudiences(
      spark: SparkSession,
      audience_df: DataFrame
  ): DataFrame = {
    val browser_segments = List(-1) ::: (563 to 568).toList
    val dev_types_segments = List(-1) ::: (560 to 562).toList
    val operating_sys_segments = List(-1) ::: (569 to 574).toList

    val browsers = (browser_segments zip List(
      "",
      "Chrome",
      "Firefox",
      "Internet Explorer",
      "Safari",
      "Android browser",
      "Opera"
    )).toMap
    val dev_types =
      (dev_types_segments zip List("", "Desktop", "Mobile", "Tablet")).toMap
    val operating_sys = (operating_sys_segments zip List(
      "",
      "Windows",
      "iOS",
      "Android",
      "OS X",
      "Linux",
      "Windows Phone"
    )).toMap

    val udfGetBrowser = udf(
      (segments: Seq[Int]) =>
        browsers(
          (segments :+ -1)
            .filter(browser_segments.contains(_))
            .toList
            .sortWith(_ > _)(0)
        )
    )

    val udfGetDevice = udf(
      (segments: Seq[Int]) =>
        dev_types(
          (segments :+ -1)
            .filter(dev_types_segments.contains(_))
            .toList
            .sortWith(_ > _)(0)
        )
    )

    val udfGetOS = udf(
      (segments: Seq[Int]) =>
        operating_sys(
          (segments :+ -1)
            .filter(operating_sys_segments.contains(_))
            .toList
            .sortWith(_ > _)(0)
        )
    )

    audience_df
      .withColumn("browser", udfGetBrowser(col("all_segments")))
      .withColumn("device_type", udfGetDevice(col("all_segments")))
      .withColumn("os", udfGetOS(col("all_segments")))
  }

  /**
    * This method returns the following list of columns for all the Dunnhumby
    * accounts and each row in the eventqueue.
    * - advertiser_id
    * - campaign_id
    * - device_id
    * - placement_id
    * - time
    * - browser
    * - device_type
    * - os
    * - ml_sh2
    * - nid_sh2
    */
  def getEnrichment(
      spark: SparkSession,
      nDays: Int,
      since: Int,
      countries: String = "",
      filter: String = "",
      partner: Boolean = true,
      piiDateFrom: String = "20190916"
  ) {
    // First of all we obtain the data from the id partner and filter, if necessary,
    // to keep only the relevant date interval
    val raw =
      if (partner)
        getDataIdPartners(
          spark,
          List("831", "1332", "1334", "1336"),
          nDays,
          since,
          "streaming"
        )
      else
        getDataAudiences(spark, nDays, since).filter(
          "id_partner IN (831, 1332, 1334, 1336)"
        )
    val data = if (dateRange.length > 0) raw.filter(filter) else raw

    // List of segments to keep
    val segments = (560 to 576)
      .map("array_contains(all_segments, %s)".format(_))
      .mkString(" OR ")

    // Define the query that we will use
    val query =
      "campaign_id IS NOT NULL AND campaign_id != '${CAMPAIGN_ID}' AND (%s)"
        .format(
          segments
        )

    // List of columns to keep
    val select =
      "time,all_segments,campaign_id,device_id,placement_id,advertiser_id,id_partner"
        .split(",")
        .toList

    // This function will be used to remove duplicated PIIs.
    val removeDuplicates = udf((piis: Seq[String]) => piis.distinct.toSeq)

    // Now we obtain the list of PIIs for the given set of countries and date range.
    val pii = spark.read
      .format("parquet")
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("day >= %s AND country in (%s)".format(piiDateFrom, countries))
      .groupBy("device_id")
      .agg(
        collect_list(col("ml_sh2")) as "ml_sh2",
        collect_list(col("nid_sh2")) as "nid_sh2"
      )
      .withColumn("ml_sh2", removeDuplicates(col("ml_sh2")))
      .withColumn("nid_sh2", removeDuplicates(col("nid_sh2")))
      .withColumn("ml_sh2", concat_ws(",", col("ml_sh2")))
      .withColumn("nid_sh2", concat_ws(",", col("nid_sh2")))

    // Now we finally perform the join between the data that has been read,
    // and the PIIs for that country.
    val joint = data
      .filter(query)
      .select(select.head, select.tail: _*)
      .join(pii, Seq("device_id"), "left")

    // Final list of columns to keep for the report
    val final_select =
      "advertiser_id,campaign_id,device_id,placement_id,time,browser,device_type,os,ml_sh2,nid_sh2,id_partner"
        .split(",")
        .toList

    // Get the dataframe with the user-agent related information.
    val ua_enriched = getUAEnrichedAudiences(spark, joint)

    ua_enriched
      .drop("all_segments")
      .select(final_select.head, final_select.tail: _*)
      .repartition(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .mode("overwrite")
      .save(
        "/datascience/dunnhumby/enrichment/day=%s"
          .format(DateTime.now.minusDays(since).toString("yyyyMMdd"))
      )
  }

  /** This method takes the previously downloaded enrichment and divides
    * it into several folders, one for each DunnHumby account.
    */
  def splitInPartners(spark: SparkSession, since: Int) = {
    // Load the enrichment
    val data = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load(
        "/datascience/dunnhumby/enrichment/day=%s"
          .format(DateTime.now.minusDays(since).toString("yyyyMMdd"))
      )
      .cache()

    // Keep only the enrichment columns
    val final_select =
      "device_id,time,advertiser_id,campaign_id,placement_id,browser,device_type,os,ml_sh2,nid_sh2"
        .split(",")
        .toList

    // For every account, create a new folder
    for (partner <- List(831, 1332, 1334, 1336)) {
      data
        .filter("id_partner = %s".format(partner))
        .drop("id_partner")
        .select(final_select.head, final_select.tail: _*)
        .repartition(1)
        .write
        .format("csv")
        .option("sep", "\t")
        .option("header", "true")
        .mode("overwrite")
        .save(
          "/datascience/dunnhumby/enrichment/day=%s/partner=%s"
            .format(DateTime.now.minusDays(since).toString("yyyyMMdd"), partner)
        )
    }
  }

  type OptionMap = Map[Symbol, Int]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--from" :: value :: tail =>
        nextOption(map ++ Map('from -> value.toString), tail)
      case "--nDays" :: value :: tail =>
        nextOption(map ++ Map('nDays -> value.toString), tail)
    }
  }

  def main(args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val from = if (options.contains('from)) options('from).toInt else 1
    val nDays =
      if (options.contains('nDays)) options('nDays).toString.toInt else 1

    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    getEnrichment(
      spark,
      nDays,
      from,
      "'BR', 'CO'", // Countries for PIIs
      "country IN ('BR', 'CO')" // Filter to be used for the pipeline data
    )
    splitInPartners(spark, from)
  }

}
