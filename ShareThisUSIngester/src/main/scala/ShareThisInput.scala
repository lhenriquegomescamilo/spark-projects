package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.{Encoders, SparkSession}
//import org.apache.hadoop.conf.Configuration

object ShareThisInput {

  def processDayNew(spark: SparkSession,
                    day: String,
                    columns: Seq[String]) = {
    import spark.implicits._
    val input_data = spark.read
        .format("json")
        .load("/data/providers/sharethis/json/dt=%s/".format(day))

    val input_estid = spark.read
        .format("parquet")
        .load("/datascience/sharethis/estid_map/")
        .select($"estid".alias("map_estid"), $"device_id")

    val joint = input_data
        .join(input_estid, $"estid"===$"map_estid", "left")

    val data_columns = joint
        .select(columns.head, columns.tail: _*)
        .withColumn("country", lit("US"))
        .withColumn("day", lit(day))
        .na.fill("")
        .orderBy($"estid")
    
    data_columns.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .save("/data/providers/sharethis/processed/day=%s".format(day))
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
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .getOrCreate()

    val columns = List("standardTimestamp",
          "estid",
          "url",
          "ip",
          "deviceType",
          "os",
          "browserFamily",
          "userAgent",
          "mappedEvent",
          "adnxs_id",
          "ttd_id",
          "mediamath_id",
          "lotame_id",
          "nlsn_id",
          "eyeota_id",
          "de_geo_pulseplus_latitude",
          "de_geo_pulseplus_longitude",
          "de_geo_pulseplus_city_code",
          "de_geo_pulseplus_city",
          "de_geo_pulseplus_postal_code",
          "de_geo_pulseplus_postal_ext",
          "de_geo_pulseplus_conn_type",
          "de_geo_pulseplus_conn_speed",
          "de_geo_isp_name",
          "de_geo_asn",
          "de_geo_asn_name",
          "channel",
          "android_id",
          "ios_idfa",
          "connected_tv",
          "searchQuery",
          "refDomain")
          //"device_id")

    download_data(spark, nDays, from, columns)
  }

}
