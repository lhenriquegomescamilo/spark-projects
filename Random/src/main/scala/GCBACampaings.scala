package main.scala

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object GCBACampaings {

  def processURL(url: String): String = {
    val columns = List(
      "r_mobile",
      "r_mobile_type",
      "r_app_name",
      "r_campaign",
      "r_lat_long",
      "id_campaign"
    )
    var res = ""

    try {
      if (url.toString.contains("?")) {
        val params = url
          .split("\\?", -1)(1)
          .split("&")
          .map(p => p.split("=", -1))
          .map(p => (p(0), p(1)))
          .toMap

        if (params.contains("r_mobile") && params("r_mobile").length > 0 && !params(
              "r_mobile"
            ).contains("[")) {
          res = columns
            .map(col => if (params.contains(col)) params(col) else "")
            .mkString(",")
        }
      }
    } catch {
      case _: Throwable => println("Error")
    }

    res
  }

  def get_report_gcba(spark: SparkSession) = {
    val myUDF = udf((url: String) => processURL(url))

    /// Configuraciones de spark
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    /// Obtenemos la data de los ultimos ndays
    val nDays = 4
    val from = 2
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(from)

    val days =
      (0 until nDays).map(start.minusDays(_)).map(_.toString(format))
    days.foreach(println)
    val path = "/datascience/data_partner_streaming"
    val paths = days
      .flatMap(
        day =>
          (0 until 24).map(
            hour =>
              path + "/hour=%s%02d/"
                .format(day, hour)
          )
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    days.foreach(println)
    val dfs = paths
      .map(
        x =>
          spark.read
            .option("basePath", "/datascience/data_partner_streaming/")
            .parquet(x)
            .filter(
              "id_partner IN (349, 1134) AND event_type = 'tk' AND (array_contains(segments, 180111) OR array_contains(segments, 180135))"
            )
            .select("url")
            .withColumn("values", myUDF(col("url")))
            .filter(length(col("values")) > 0)
      )

    /// Concatenamos los dataframes
    val dataset = dfs.reduce((df1, df2) => df1.unionAll(df2))

    dataset.write
      .format("csv")
      .option("sep", ";")
      .mode("overwrite")
      .save("/datascience/custom/gcba_campaigns_marcha")
  }

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    get_report_gcba(
      spark
    )
  }
}
