package main.scala.datasets

import main.scala.datasets.UrlUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.{Days, DateTime}
import org.apache.spark.sql.{SaveMode, DataFrame, SparkSession, Row}

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
      .select("device_id", "url", "country", "referer")
      .withColumn("url", array(col("url"), col("referer")))
      .drop("referer")
      .withColumn("url", explode(col("url")))
      .distinct()

    // Now we process the URLs the data
    val processed = UrlUtils
      .processURL(data_urls, field = "url")

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
    withDomain.write
      .format("parquet")
      .partitionBy("country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_triplets/urls/raw/")
  }

  /**
    * This function takes as input a dataframe and returns another dataframe
    * with an id associated to every row. This id is monotonically increasing
    * and with steps of 1 by 1.
    */
  def dfZipWithIndex(
      df: DataFrame,
      offset: Int = 0,
      colName: String = "id",
      inFront: Boolean = true
  ): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(
        ln =>
          Row.fromSeq(
            (if (inFront) Seq(ln._2 + offset) else Seq())
              ++ ln._1.toSeq ++
              (if (inFront) Seq() else Seq(ln._2 + offset))
          )
      ),
      StructType(
        (if (inFront) Array(StructField(colName, LongType, false))
         else Array[StructField]())
          ++ df.schema.fields ++
          (if (inFront) Array[StructField]()
           else Array(StructField(colName, LongType, false)))
      )
    )
  }

  /**
    * This function reads the raw data downloaded previously, and creates
    * two indexes: one for devices and the other one for URLs. Finally, it
    * generates a properly triplets-like dataset only with indexes.
    */
  def get_indexes(spark: SparkSession) = {
    // Load the raw data
    val raw_data = spark.read
      .format("parquet")
      .load("/datascience/data_triplets/urls/raw/")

    // Obtain an id for every URL. Here we filter out those URLs that have only one person visiting it.
    dfZipWithIndex(
      raw_data
        .groupBy("country", "url")
        .count()
        .filter("count >= 2"),
      offset = 0,
      colName = "url_idx"
    ).select("url_idx", "url", "country")
      .write
      .format("parquet")
      .partitionBy("country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_triplets/urls/url_index/")

    // Get the index for the devices
    dfZipWithIndex(
      raw_data
        .select("country", "device_id")
        .distinct(),
      offset = 0,
      colName = "device_idx"
    ).select("device_idx", "device_id", "country")
      .write
      .format("parquet")
      .partitionBy("country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_triplets/urls/device_index/")


    // Finally we use the indexes to store triplets partitioned by country
    val url_idx = spark.read
      .format("parquet")
      .load("/datascience/data_triplets/urls/url_index/")
      .drop("country")

    val device_idx = spark.read
      .format("parquet")
      .load("/datascience/data_triplets/urls/device_index/")
      .drop("country")

    raw_data
      .join(device_idx, Seq("device_id"))
      .join(url_idx, Seq("url"))
      .select("device_idx", "url_idx", "country")
      .write
      .format("parquet")
      .partitionBy("country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_triplets/urls/indexed/")
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

    // generate_triplets(spark, nDays, from)
    get_indexes(spark)
  }
}
