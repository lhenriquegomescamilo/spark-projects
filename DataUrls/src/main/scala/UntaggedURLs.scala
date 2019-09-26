package main.scala
import main.scala.datasets.{UrlUtils}
import org.apache.spark.sql.{SparkSession, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}

object UntaggedURLs {

  def processDay(spark: SparkSession, day: String) = {
    val data = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/data/eventqueue/%s/".format(day))

    val data_filtered = data
      .select("url", "tagged", "share_data", "event_type", "country")
      .filter(
        "event_type IN ('pv', 'batch', 'data') AND tagged IS NULL AND share_data == '1' AND url IS NOT NULL AND country IN ('AR', 'MX', 'CL', 'CO', 'BR', 'PE', 'US')"
      )
      .select("url", "country")
      .distinct()

    val data_processed =
      UrlUtils.processURL(dfURL = data_filtered, field = "url")

    data_processed
      .withColumn("day", lit(day.replace("/", "")))
      .write
      .format("parquet")
      .partitionBy("day", "country")
      .mode("append")
      .save("/datascience/data_url_classifier/untagged_urls/")

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
    val options = nextOption(Map(), args.toList)
    val nDays = if (options.contains('nDays)) options('nDays) else 1
    val since = if (options.contains('from)) options('from) else 1

    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    days.map(processDay(spark, _))
  }
}
