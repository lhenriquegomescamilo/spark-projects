package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.expressions.Window

object USHomes {
  def getApproximatePlacePerId(spark: SparkSession) = {
    val w = Window.partitionBy(col("estid")).orderBy(col("count").desc)

    spark.read
      .format("parquet")
      .load(
        "/data/providers/sharethis/processed/"
      )
      .withColumn(
        "zipplus4",
        concat(
          col("de_geo_pulseplus_postal_code"),
          col("de_geo_pulseplus_postal_ext")
        )
      )
      .filter("zipplus4 != ''")
      .select("estid", "zipplus4", "standardTimestamp")
      .withColumn("approx_zip4", substring(col("zipplus4"), 0, 8))
      .withColumn("hour", hour(col("standardTimestamp")))
      .filter("hour >= 19 OR hour <= 8")
      .groupBy("estid", "approx_zip4")
      .agg(count("hour") as "count", collect_list("zipplus4") as "points")
      .filter("count > 1")
      .withColumn("rn", row_number.over(w))
      .where(col("rn") === 1)
      .drop("rn")
      .write
      .format("parquet")
      .mode("overwrite")
      .save("/datascience/custom/us_homes_approx")
  }

  def getHomes(spark: SparkSession) = {
    val w = Window.partitionBy(col("estid")).orderBy(col("count").desc)

    spark.read
      .format("parquet")
      .load("/datascience/custom/us_homes_approx")
      .withColumn("zip4", explode(col("points")))
      .groupBy("estid", "zip4")
      .count()
      .withColumn("rn", row_number.over(w))
      .where(col("rn") === 1)
      .drop("rn")
      .write
      .format("parquet")
      .mode("overwrite")
      .save("/datascience/custom/us_homes")
  }

  def getEstidMap(spark: SparkSession) = {
    val estid_map = spark.read
      .format("parquet")
      .load("/datascience/sharethis/estid_map")
      .filter("country = 'US'")
    val homes =
      spark.read.format("parquet").load("/datascience/custom/us_homes")
    val joint = homes
      .join(estid_map, Seq("estid"))
      .withColumn("device_id", explode(col("device_id")))

    joint.write
      .format("parquet")
      .mode("overwrite")
      .save("/datascience/custom/us_homes_rely_ids")
  }

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    getApproximatePlacePerId(spark)
    getHomes(spark)
    getEstidMap(
      spark = spark
    )

    spark.read
      .format("parquet")
      .load(
        "/datascience/custom/us_homes"
      )
      .groupBy("zipplus4")
      .agg(approxCountDistinct("estid", 0.02) as "device_unique")
      .write
      .format("csv")
      .mode("overwrite")
      .save("/datascience/custom/device_unique_per_zip4")
  }
}
