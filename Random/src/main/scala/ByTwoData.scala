package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
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

  def processDayNew(spark: SparkSession, day: String) = {
    println("LOGGER: processing day %s".format(day))

    val columns = List("standardTimestamp",
        "url",
        "ip",
        //"url_domain",
        "mappedEvent",
        "userAgent",
        "deviceType",
        "os",
        "browserFamily",
        //"geo_iso",
        "de_geo_pulseplus_city",
        "de_geo_pulseplus_postal_code",
        "de_geo_pulseplus_postal_ext",
        "de_geo_pulseplus_latitude",
        "de_geo_pulseplus_longitude",
        "nlsn_id",
        "ttd_id",
        "lotame_id",
        "mediamath_id",
        "eyeota_id",
        "adnxs_id",
        "estid",
        "de_geo_asn",
        "de_geo_asn_name",
        "channel",
        "android_id:",
        "ios_idfa:",
        "day")

    val meta = spark.read.format("csv").load("/data/providers/sharethis/schema.meta").collect().map(_(0).toString)
    val headers = meta.map(_.split(":")(0))
    val multivalue = meta.filter(_.contains("|")).map(_.split(":")(0))

    val schema = headers.foldLeft(new StructType())(
          (schema, col) => schema.add(col, "string")
        )
        
    val data = spark.read
          .format("com.databricks.spark.csv")
          .schema(schema)
          .load("/data/providers/sharethis/raw/%s*.json".format(day))
        
    val withMultivalues = multivalue.foldLeft(data)(
          (df, c) => df.withColumn(c, split(col(c), "\\|"))
        )

    val df = withMultivalues
            //.withColumn("url_domain", col("url"))
            .withColumn("adnxs_id", col("external_id").getItem(0))
            .withColumn("ttd_id", col("external_id").getItem(1))
            .withColumn("mediamath_id", col("external_id").getItem(2))
            .withColumn("lotame_id", col("external_id").getItem(3))
            .withColumn("nlsn_id", col("external_id").getItem(4))
            .withColumn("eyeota_id", col("external_id").getItem(5))
            .withColumn("day", regexp_replace(split(col("standardTimestamp"), "T").getItem(0), "-", ""))

    val by_columns = df.select(columns.head, columns.tail: _*).na.fill("")
    
    by_columns.coalesce(100)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "false")
      .option("sep", "\t")
      .save("/datascience/sharethis/bytwo/day=%s".format(day))
    
    println("LOGGER: day %s processed successfully!".format(day))
  }

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

  def download_data(spark: SparkSession, nDays: Int, from: Int): Unit = {
    // Now we get the list of days to be downloaded
    val format = "yyyyMMdd"
    val end   = DateTime.now.minusDays(from)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    days.foreach(day => processDayNew(spark, day))
  }

  type OptionMap = Map[Symbol, Int]

  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--nDays" :: value :: tail =>
        nextOption(map ++ Map('nDays -> value.toInt), tail)
      case "--from" :: value :: tail =>
        nextOption(map ++ Map('from -> value.toInt), tail)
    }
  }

  def main(args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val nDays = if (options.contains('nDays)) options('nDays) else 1
    val from = if (options.contains('from)) options('from) else 1

    val spark = SparkSession.builder
        .appName("Run matching estid-device_id")
        //.config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .getOrCreate()

    //val days = List("20190803", "20190802", "20190801", "20190731", "20190730", "20190729", "20190728", "20190727", "20190726", "20190725")
    //val days = List("20190811", "20190812", "20190813", "20190814", "20190815", "20190816", "20190817", "20190818")

    download_data(spark, nDays, from)
  }

}
