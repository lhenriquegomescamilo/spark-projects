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
    *
    */
  def getEnrichment(
      spark: SparkSession,
      piiDateFrom: String,
      nDays: Int,
      since: Int,
      countries: String = "",
      dateRange: String = ""
  ) {
    // First of all we obtain the data from the id partner and filter, if necessary,
    // to keep only the relevant date interval
    // val raw = getDataIdPartners(spark, List("831"), nDays, since, "streaming")
    val raw = getDataAudiences(spark, nDays, since).filter("id_partner IN (831, 1332)")
    val data = if (dateRange.length > 0) raw.filter(dateRange) else raw

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
      "time,all_segments,campaign_id,device_id,placement_id,advertiser_id"
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
      "advertiser_id,campaign_id,device_id,placement_id,time,browser,device_type,os,ml_sh2,nid_sh2"
        .split(",")
        .toList

    // Get the dataframe with the user-agent related information.
    val ua_enriched = getUAEnrichedAudiences(spark, joint)

    ua_enriched
      .drop("all_segments")
      .select(final_select.head, final_select.tail: _*)
      .write
      .format("csv")
      .option("sep", "\t")
      .mode("overwrite")
      .save("/datascience/custom/dunnhumby_enrichment_piis")
  }

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    getEnrichment(
      spark,
      "20190916",
      63,
      23,
      "'BR'",
      "country = 'BR' AND (datetime >= '2019-11-12 00:00:00' AND datetime <= '2020-01-13 00:00:00') AND campaign_id IN (31903894, 31903895, 31903893, 31903892, 32785302, 32785303)"
    )
  }
}
