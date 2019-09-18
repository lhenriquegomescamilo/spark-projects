package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._

object TestDemoPublicis {

  def getDataPublicis(spark: SparkSession, day: String): DataFrame = {
    val udfSegments = udf( (segments: Seq[Row]) => segments.map(row => row(0).toString) )

    spark.read
      .format("json")
      .load("/datascience/data_publicis/memb/inc/dt=%s/".format(day))
      .withColumn("segids", udfSegments(col("segids")))
  }

  def getStats(spark: SparkSession, day: String) = {
    val publicis = getDataPublicis(spark, day)

    publicis.cache()
    println(publicis.count())

    println(
      publicis
        .filter("array_contains(segids, '2') OR array_contains(segids, '3')")
        .count()
    )
  }

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(2)
    val days = (0 until 20).map(end.minusDays(_)).map(_.toString(format))

    days.map(getStats(spark, _))
  }
}
