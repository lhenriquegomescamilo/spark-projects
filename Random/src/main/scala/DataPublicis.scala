package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._

import java.security.MessageDigest
import java.util
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import java.time.DateTimeException
import java.sql.Savepoint

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object DataPublicis {

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
      .map(day => path + "/hour=%s*".format(day))
    // .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)
    fs.close()

    df
  }

  def getDataPublicis(spark: SparkSession) {
    val dataBase =
      getDataAudiences(
        spark,
        30,
        1
      ).filter(
          """country = 'MX' AND
             event_type = 'pv' AND 
             url NOT LIKE '%parser%' AND
             url NOT LIKE '%bonafontentucasa.com.mx%' AND
             url NOT LIKE '%infiniti.mx%' AND
             url NOT LIKE '%aeromexico.com%' AND
             url NOT LIKE '%mopar.com.mx%' AND
             url NOT LIKE '%tresemme.com.mx%' AND
             (array_contains(all_segments, 165) OR 
              array_contains(all_segments, 413) OR 
              array_contains(all_segments, 3013) OR 
              array_contains(all_segments, 465) OR 
              array_contains(all_segments, 395))"""
        )
        .select("device_id", "datetime", "referer", "url")
        .withColumn(
          "url_domain",
          regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
        )
        .withColumn("url_domain", regexp_replace(col("url_domain"), "/.*", ""))
        .withColumn(
          "referer_domain",
          regexp_replace(col("referer"), "http.*://(.\\.)*(www\\.){0,1}", "")
        )
        .withColumn(
          "referer_domain",
          regexp_replace(col("referer_domain"), "/.*", "")
        )

    dataBase
      .repartition(20)
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/publicis_sample")

  }

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    getDataPublicis(spark)

  }
}
