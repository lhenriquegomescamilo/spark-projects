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

    // Config
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val today = DateTime.now().toString(format)
    var start = DateTime.now.minusDays(1+7)
    var end = DateTime.now.minusDays(1)
    var daysCount = Days.daysBetween(start, end).getDays()
    var days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))
    val path = "/datascience/scraper/raw/processed"

    // Now we obtain the list of hdfs folders to be read
    var hdfs_files = days
      .map(day => path + "/day=%s/".format(day)) 
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path)))

    spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .filter("html is not null")
      .withColumn("form",udfForm(col("html")))
      .filter("form = 'email'")
      .select("url")
      .selectExpr("*", "parse_url(url, 'HOST') as domain")
      .withColumn("day",lit(today))
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
