package main.scala.datasets

import main.scala.datasets.UrlUtils
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.spark.sql.{SaveMode, DataFrame, SparkSession}

object UrlUserTriplets {

  def getDataUrls(spark: SparkSession, nDays: Int, from: Int): DataFrame = {
    // Setting the days that are going to be loaded
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(from)
    val days = (0 until nDays).map(start.minusDays(_)).map(_.toString(format))

    // Now we gather all the paths
    val paths = days
      .map(
        day => "/datascience/data_demo/data_urls/day=%s*".format(day)
      )

    // Finally we load all the data
    val data = spark.read
      .option("basePath", "/datascience/data_demo/data_urls/")
      .parquet(paths: _*)

    data
  }

  def generate_triplets(spark: SparkSession, nDays: Int, from: Int) = {
    // Load the data from data_urls pipeline
    val data_urls = getDataUrls(spark, nDays, from)
      .select("device_id", "url", "country")
      .distinct()

    val data_referer = getDataUrls(spark, nDays, from)
      .select("device_id", "referer", "country")
      .withColumnRenamed("referer", "url")
      .distinct()

    // Now we process the URLs the data
    val processed = UrlUtils
      .processURL(data_urls, field = "url")
      .unionAll(
        UrlUtils
          .processURL(data_referer, field = "url")
      )

    // Then we add the domain as a new URL for each user
    val withDomain = processed
      .withColumn(
        "domain",
        regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
      )
      .withColumn("domain", regexp_replace(col("domain"), "/.*", ""))
      .withColumn("url", array(col("url"), col("domain")))
      .withColumn("url", explode(col("url")))
      .select("device_id", "url", "country")
      .distinct()

    // Finally we save the data
    withDomain
      .write
      .format("parquet")
      .partitionBy("country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_triplets/urls/raw/")
  }

  def get_indexes(spark: SparkSession) = {
    spark.read
      .format("parquet")
      .load("/datascience/data_triplets/urls/raw/")
      .groupBy("country", "url")
      .count()
      .filter("count >= 2")
      .withColumn("url_idx", monotonicallyIncreasingId)
      .select("url_idx", "url")
      .write
      .format("parquet")
      .partitionBy("country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_triplets/urls/url_index/")

    spark.read
      .format("parquet")
      .load("/datascience/data_triplets/urls/raw/")
      .select("country", "device_id")
      .distinct()
      .withColumn("device_idx", monotonicallyIncreasingId)
      .select("device_idx", "device_id")
      .write
      .format("parquet")
      .partitionBy("country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_triplets/urls/device_index/")
  }

  type OptionMap = Map[Symbol, Int]

  /**
    * This method parses the parameters sent.
    */
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
    val nDays = if (options.contains('nDays)) options('nDays) else 30
    val from = if (options.contains('from)) options('from) else 1

    // Spark configuration
    val spark = SparkSession.builder
      .appName("Data URLs: Dataset triplets <User, URL, count>")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    generate_triplets(spark, nDays, from)
    get_indexes(spark)
  }
}
