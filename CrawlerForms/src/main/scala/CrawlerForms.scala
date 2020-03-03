package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.Days
import org.apache.spark._
import org.joda.time.DateTime
import org.apache.hadoop.fs.{ FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.jsoup.Jsoup
import org.joda.time.DateTime
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{
 StructType,
 StructField,
 StringType,
 IntegerType
}
import org.apache.spark.sql.{Column, Row}


/**
  * The idea of this script is to Ingest Urls daily to local servers for Scrapper.
  */
object CrawlerForms {

  def get_forms(spark: SparkSession,day:String) {

    val udfForm = udf((html: String) => org.jsoup.Jsoup.parse(html).select("form").select("input").attr("type").toString)

    spark.read.load("/datascience/scraper/raw/processed/day=%s/".format(day))
              .filter("html is not null")
              .withColumn("form",udfForm(col("html")))
              .filter("form = 'email'")
              .select("url")
              .selectExpr("*", "parse_url(url, 'HOST') as domain")
              .withColumn("day",lit(day))
              .write
              .format("parquet")
              .mode("append")
              .partitionBy("day")
              .save("/datascience/forms/")
    }

  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(Args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("Crawler Forms")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

  
    val day = DateTime.now().minusDays(1).toString("yyyyMMdd")
    get_forms(spark,day)

  }
}
