package main.scala
import org.apache.spark.sql.SparkSession
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import java.time.LocalDateTime

object TaringaIngester {
  def process_hour(spark: SparkSession, day: String, hour:String) {
    val date = day.concat(hour)
    
    spark.read.load("/datascience/data_partner_streaming/hour=%s/id_partner=146".format(date))
              .withColumn("day", lit(date))
              .withColumn("all_segments", concat_ws(",", col("all_segments")))
              .select("device_id", "all_segments", "url", "datetime", "day","country")
              .filter("country = 'AR' or country = 'MX' or country = 'CL' or country = 'CO'")
              .write
              .format("parquet")
              .partitionBy("day", "country")
              .mode(SaveMode.Overwrite)
              .save("/datascience/taringa_ingester/")
  }

  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder
      .appName("Data from Taringa")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .getOrCreate()

    /// Parseo de parametros
    val since = if (args.length > 0) args(0).toInt else 1
    val ndays = if (args.length > 1) args(1).toInt else 1

    val format = "YYYYMMdd"
    val day = DateTime.now.toString(format)
    // Process last hour
    val hour = (LocalDateTime.now.getHour - 1).toString

    process_hour(spark,day,hour)
  }
}
