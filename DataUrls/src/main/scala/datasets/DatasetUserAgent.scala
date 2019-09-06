package main.scala.datasets
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.sql.functions.{
  upper,
  count,
  col,
  abs,
  udf,
  regexp_replace,
  split,
  lit,
  explode,
  length,
  to_timestamp,
  from_unixtime,
  date_format,
  sum
}

object DatasetUserAgent {

  def get_data_user_agent(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      country: String
  ): DataFrame = {
    // Spark configuration
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until ndays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_useragents"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    df
  }

  def get_url_user_agent(spark: SparkSession,ndays: Int,since: Int,country: String,gtDF: DataFrame,joinType:String): DataFrame =  {

    // Get data from user agent pipeline <device_id, brand,model,browser,os,os_min_version,os_max_version,user_agent,url,event_type>
    val df = get_data_user_agent(spark = spark, ndays = ndays, since = since, country = country)

    // Calculating triplets dataframes < url, brand, count >, < url, model, count >, < url, browser, count >
    // and < url, os, count >
    val triplets_brand = df
      .groupBy("url", "brand")
      .agg(count("device_id").as("count"))
      .withColumnRenamed("brand", "feature")
    val triplets_model = df
      .groupBy("url", "model")
      .agg(count("device_id").as("count"))
      .withColumnRenamed("model", "feature")
    val triplets_browser = df
      .groupBy("url", "browser")
      .agg(count("device_id").as("count"))
      .withColumnRenamed("browser", "feature")
    val triplets_os = df
      .groupBy("url", "os")
      .agg(count("device_id").as("count"))
      .withColumnRenamed("os", "feature")

    // Concatenating all triplets dataframes and processing the url
    val features_ua = triplets_brand
      .union(triplets_model)
      .union(triplets_browser)
      .union(triplets_os)
      .withColumn(
        "url",
        regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
      )
      .withColumn(
        "url",
        regexp_replace(col("url"), "(\\?|#).*", "")
      )

    // Joining dataset with GT urls
    val joint = gtDF.join(features_ua,Seq("url"),joinType)
                    .withColumn("country",lit(country))
                    .dropDuplicates()
    
    joint.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .partitionBy("country")
      .save("/datascience/data_url_classifier/dataset_user_agent/")
    
    joint
  }

  def main(args: Array[String]) {
    // Spark configuration
    val spark = SparkSession.builder
      .appName("Data URLs: Dataset User Agent")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .getOrCreate()

    // Parseo de parametros
    val ndays = if (args.length > 0) args(0).toInt else 10
    val since = if (args.length > 1) args(1).toInt else 1
    val country = if (args.length > 2) args(2).toString else ""
    val segments = List(129, 59, 61, 250, 396, 150, 26, 32, 247, 3013, 3017)

    val gtDF = spark.read.load("/datascience/data_url_classifier/gt/country=AR/")

    get_url_user_agent(spark, country = country, since = since, ndays = ndays, gtDF = gtDF, joinType = "inner")
  }
}
