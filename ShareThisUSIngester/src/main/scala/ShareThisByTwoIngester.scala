package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.{DateTime, Days}
//import org.joda.time.format.DateTimeFormat
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.spark.sql.{SaveMode, DataFrame}
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.{Encoders, SparkSession}
//import org.apache.hadoop.conf.Configuration

object ShareThisByTwoIngester {

  def processDayNew(spark: SparkSession, day: String) = {

    val columns = List("standardTimestamp",
        "url",
        "ip",
        //"url_domain",
        "mappedEvent",
        "userAgent",
        "deviceType",
        "os",
        "browserFamily",
        "geo_iso",
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
          (df, c) => df.withColumn(c, split(col(c), "|"))
        )

    val df = withMultivalues
            .withColumn("adnxs_id", col("external_id").getItem(0))
            .withColumn("ttd_id", col("external_id").getItem(1))
            .withColumn("mediamath_id", col("external_id").getItem(2))
            .withColumn("lotame_id", col("external_id").getItem(3))
            .withColumn("nlsn_id", col("external_id").getItem(4))
            .withColumn("eyeota_id", col("external_id").getItem(5))
            .withColumn("day", lit(day))
            .drop("external_id")

    //val by_columns = df.select(columns.head, columns.tail: _*).na.fill("")
    
    df.coalesce(100).write
      .mode("append")
      .format("parquet")
      .partitionBy("day")
      .save("/data/providers/sharethis/new/")
  }

  /*def getDataAudiences(
      spark: SparkSession,
      nDays: Int = 30,
      since: Int = 1
  ) = {
    // First we obtain the configuration to be allowed to wa`tch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/sharethis/loading"

  }*/

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
        .appName("ShareThisByTwoIngester")
        //.config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .getOrCreate()

    download_data(spark, nDays, from)
  }

}
