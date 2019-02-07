package main.scala

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}

object ByTwoGeoData {

  def sampleSanti(spark: SparkSession, day: String) {
    spark.read
      .format("parquet")
      .load("/datascience/geo/US/day=%s".format(day))
      .coalesce(100)
      .write
      .format("com.databricks.spark.csv")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/geo/US/%s/".format(day))
    println("\nLOGGER: DAY %s HAS BEEN PROCESSED!\n\n".format(day))
  }

  def main(Args: Array[String]) {
    val spark = SparkSession.builder.appName("ByTwo data").getOrCreate()

    val format = "yyyyMMdd"
    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy")
    val start = formatter.parseDateTime("15/11/2018")
    val days = (0 until 50).map(n => start.plusDays(n)).map(_.toString(format))

    days.map(day => sampleSanti(spark, day))
  }
}
