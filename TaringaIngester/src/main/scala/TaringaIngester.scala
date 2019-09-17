package main.scala
import org.apache.spark.sql.SparkSession
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object TaringaIngester {
  def process_day(spark: SparkSession, day: String) {

    val countries = List("AR", "MX", "CL", "CO")

    for (c <- countries) {
      spark.read
        .load(
          "/datascience/data_audiences_streaming/hour=%s*/country=%s"
            .format(day, c)
        )
        .filter("url LIKE '%taringa%'") // country IN ('AR', 'CL', 'MX', 'CO') AND
        .withColumn("day", lit(day))
        .withColumn("all_segments", concat_ws(",", col("all_segments")))
        .select("device_id", "all_segments", "url", "datetime", "day")
        .write
        .format("csv")
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .save("/datascience/taringa_ingester/data_%s_%s".format(day, c))
    }

  }

  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder
      .appName("Data GCBA Process")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .getOrCreate()

    /// Parseo de parametros
    val since = if (args.length > 0) args(0).toInt else 1
    val ndays = if (args.length > 1) args(1).toInt else 1

    val format = "YYYYMMdd"
    val start = DateTime.now.minusDays(since + ndays)
    val end = DateTime.now.minusDays(since)

    val daysCount = Days.daysBetween(start, end).getDays()
    val days =
      (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

    days.map(day => process_day(spark, day))
  }
}
