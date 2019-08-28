package main.scala
import spark.implicits._
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.{Encoders, SparkSession}
//import org.apache.hadoop.conf.Configuration

object ByTwoOutput {

  def processDayNew(spark: SparkSession,
                    day: String,
                    columns: Seq[String]) = {

    val data_input = spark.read
        .format("parquet")
        .load("/data/providers/sharethis/processed/day=%s".format(day))
        
    val filter = data_input
            .filter($"country" === "US")

    val final_data = filter
            .withColumn("android_id", concat_ws("|", $"android_id"))
            .withColumn("ios_idfa", concat_ws("|", $"ios_idfa"))
            .withColumn("connected_tv", concat_ws("|", $"connected_tv"))
            .select(columns.head, columns.tail: _*)
    
    final_data.write
        .format("csv")
        .mode("overwrite")
        .option("header", "false")
        .option("sep", "\t")
        .option("compression", "bzip2")
        .save("/datascience/sharethis/bytwo/day=%s".format(day))
    
  }

  def download_data(spark: SparkSession, nDays: Int, from: Int, columns: Seq[String]): Unit = {
    // Now we get the list of days to be downloaded
    val format = "yyyyMMdd"
    val end   = DateTime.now.minusDays(from)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    days.foreach(day => processDayNew(spark, day, columns))
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
        .appName("ShreThis Input")
        //.config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .getOrCreate()

    val columns = List("standardTimestamp",
        "url",
        "ip",
        "mappedEvent",
        "userAgent",
        "deviceType",
        "os",
        "browserFamily",
        "de_geo_pulseplus_city",
        "de_geo_pulseplus_postal_code",
        "de_geo_pulseplus_postal_ext",
        "de_geo_pulseplus_latitude",
        "de_geo_pulseplus_longitude",
        "de_geo_pulseplus_conn_type",
        "de_geo_pulseplus_conn_speed",
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
        "android_id",
        "ios_idfa",
        "connected_tv",
        "searchQuery",
        "refDomain",
        "day")

    download_data(spark, nDays, from, columns)
  }

}
