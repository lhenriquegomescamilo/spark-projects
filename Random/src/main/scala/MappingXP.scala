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
object MappingXP {

  def processCountry(
      spark: SparkSession,
      country: String
  ): DataFrame = {
    val data = spark.read
      .format("csv")
      .option("sep", "\t")
      .load("/datascience/data_lookalike/expansion/country=%s/".format(country))

    val segmentMap = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/data_lookalike/mapping_xp_segments.csv")
      .collect()
      .map(row => (row(0).toString, row(1).toString))
      .toMap

    val mappingUDF = udf(
      (segments: String) =>
        segments
          .split(",")
          .map(
            segment =>
              if (segmentMap.contains(segment)) segmentMap(segment)
              else segment
          )
          .mkString(",")
    )
    data
      .withColumn("_c2", mappingUDF(col("_c1")))
      .withColumn("device_type", lit("web"))
      .select("device_type", "_c0", "_c2")
      .write
      .format("csv")
      .option("sep", "\t")
      .mode("overwrite")
      .save("/datascience/data_lookalike/expansion/%s/".format(country))
  }

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    val countries = List("MX", "AR", "CL", "CO", "PE", "US")

    countries.map(c => processCountry(c))
  }
}
