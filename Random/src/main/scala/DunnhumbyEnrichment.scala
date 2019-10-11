package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.DataFrame
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

  def getEnrichment(
      spark: SparkSession,
      crm_segments: String,
      dateFrom: String
  ) {
    val data = getDataIdPartners(spark, List("831"), 30, 1, "streaming")
    val crm_files = crm_segments
      .split(",")
      .map("array_contains(all_segments, %s)".format(_))
      .mkString(" OR ")
    val segments = (560 to 576)
      .map("array_contains(all_segments, %s)".format(_))
      .mkString(" OR ")

    val query = "array_contains(segments, 144633) AND (%s) AND (%s)".format(
      crm_files,
      segments
    )
    val select =
      "time,all_segments,campaign_id,device_id,placement_id,advertiser_id"
        .split(",")
        .toList

    val pii = spark.read
      .format("parquet")
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("day >= %s".format(dateFrom))
      .filter("country in('BR')")
      .groupBy("device_id")
      .agg(
        collect_list(col("ml_sh2")) as "ml_sh2",
        collect_list(col("nid_sh2")) as "nid_sh2"
      )
      .withColumn("ml_sh2", concat_ws(",", col("ml_sh2")))
      .withColumn("nid_sh2", concat_ws(",", col("nid_sh2")))

    data
      .filter(query)
      .select(select.head, select.tail: _*)
      .join(pii, Seq("device_id"), "left")
      .write
      .format("parquet")
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
      "161639,157799,157769,157747,156869,156865,148997,148995",
      "20190901"
    )
  }
}
