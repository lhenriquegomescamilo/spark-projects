package main.scala

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object DumpBase {
  def getDataFromPartner(spark: SparkSession, id_partner: String): DataFrame = {
    val data = spark.read
      .format("parquet")
      .load(
        "/datascience/data_partner_streaming/hour=*/id_partner=%s"
          .format(id_partner)
      )

    data
  }

  def getDataAudiences(
      spark: SparkSession,
      nDays: Int = 30,
      since: Int = 1,
      id_partner: String,
      country: String
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
      .map(day => path + "/hour=%s*/country=%s".format(day, country))
    // .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)
    fs.close()

    df.filter("id_partner = %s".format(id_partner))
  }

  def dumpData(spark: SparkSession, id_partner: String) = {
    val data = getDataAudiences(spark, 45, 46, id_partner, "MX") //getDataFromPartner(spark, id_partner)

    val array_columns = "segments first_party all_segments".split(" ")

    val processed = data
      .withColumn("segments", concat_ws(",", col("segments")))
      .withColumn("first_party", concat_ws(",", col("first_party")))
      .withColumn("all_segments", concat_ws(",", col("all_segments")))
      .withColumn("tags", concat_ws(",", col("tags")))
      .withColumn("app_installed", concat_ws(",", col("app_installed")))

    processed.write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .mode("overwrite")
      .save("/datascience/custom/dump_%s_2".format(id_partner))
  }

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)
    dumpData(spark, "464")
  }
}
