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

  def dumpData(spark: SparkSession, id_partner: String) = {
    val data = getDataFromPartner(spark, id_partner)

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
      .save("/datascience/custom/dump_%s".format(id_partner))
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
