package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.hadoop.conf.Configuration
import org.apache.spark.ml.classification.{
  RandomForestClassificationModel,
  RandomForestClassifier
}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.classification.{
  GBTClassificationModel,
  GBTClassifier
}

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object ByTwoData {

  def processDay(spark: SparkSession, day: String) = {
    println("LOGGER: processing day %s".format(day))

    // List of columns present in the json files
    val columns = List(
      "estid",
      "standardTimestamp",
      "url",
      "ip",
      "deviceType",
      "os",
      "browserFamily",
      "de_geo_isp_name",
      "de_geo_pulseplus_conn_type",
      "de_geo_pulseplus_latitude",
      "de_geo_pulseplus_longitude",
      "de_geo_pulseplus_city_code",
      "de_geo_pulseplus_city",
      "android_id",
      "ios_idfa",
      "connected_tv"
    )

    // Here we process the data
    spark.read
      .format("com.databricks.spark.csv")
      .load("/datascience/sharethis/loading/%s*.json".format(day))
      // Renaming the columns
      .withColumnRenamed("_c0", "estid")
      .withColumnRenamed("_c1", "standardTimestamp")
      .withColumnRenamed("_c2", "url")
      .withColumnRenamed("_c3", "ip")
      .withColumnRenamed("_c4", "deviceType")
      .withColumnRenamed("_c5", "os")
      .withColumnRenamed("_c6", "browserFamily")
      .withColumnRenamed("_c7", "de_geo_isp_name")
      .withColumnRenamed("_c8", "de_geo_pulseplus_conn_type")
      .withColumnRenamed("_c9", "de_geo_pulseplus_latitude")
      .withColumnRenamed("_c10", "de_geo_pulseplus_longitude")
      .withColumnRenamed("_c11", "de_geo_pulseplus_city_code")
      .withColumnRenamed("_c12", "de_geo_pulseplus_city")
      .withColumnRenamed("_c13", "android_id")
      .withColumnRenamed("_c14", "ios_idfa")
      .withColumnRenamed("_c15", "connected_tv")
      // Modifying the types
      // .withColumn("ip", col("ip").cast("int"))
      // Dropping unnecessary columns
      .drop("de_geo_pulseplus_conn_type", "de_geo_isp_name", "de_geo_pulseplus_city_code")
      .coalesce(100)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("sep", "\t")
      .save("/datascience/sharethis/bytwo/day=%s".format(day))
    println("LOGGER: day %s processed successfully!".format(day))
  }

  def getDataAudiences(
      spark: SparkSession,
      nDays: Int = 30,
      since: Int = 1
  ) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/sharethis/loading"

  }

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("Run matching estid-device_id").getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    val days = List("20190803", "20190802", "20190801", "20190731", "20190730", "20190729", "20190728", "20190727", "20190726", "20190725")
    days.foreach(day => processDay(spark, day))
  }

}
