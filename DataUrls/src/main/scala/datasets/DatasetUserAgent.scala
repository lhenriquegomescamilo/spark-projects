package main.scala.datasets
import main.scala.datasets.{UrlUtils}
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

    val df_filtered = UrlUtils.processURL(dfURL = df, field = "url")

    df_filtered
  }

  def get_url_user_agent(spark: SparkSession,ndays: Int,since: Int,country: String,gtDF: DataFrame,joinType:String,name:String): DataFrame =  {

    // Defining top 100 urls to take
    val top_ua = spark.read.format("csv").option("header","true").load("/datascience/custom/top_user_agent.csv")
    val top_ua_b = broadcast(top_ua)

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

    // Concatenating all triplets dataframes
    val features_ua = triplets_brand
      .union(triplets_model)
      .union(triplets_browser)
      .union(triplets_os)

    // Joining features with top user agent features
    val join_ua = features_ua.join(top_ua.drop("count"),Seq("feature"),"inner")
                              .select("url","feature","count")

    // Joining dataset with GT urls
    val joint = gtDF.join(join_ua,Seq("url"),joinType)
                    .dropDuplicates()
                    .select("url","feature","count")

    // Adding all features as a fake df in order to get all column names in the final df
    val final_df = joint.union(top_ua.withColumnRenamed("url_fake","url"))
                        .withColumn("feature",regexp_replace(col("feature") ," ", "_")) // Removing spaces

    // Groupby and pivot by user agent
    final_df.groupBy("url")
          .pivot("feature")
          .agg(sum("count"))
          .na.fill(0)
          .withColumn("country",lit(country))
          .write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .partitionBy("country")
          .save("/datascience/data_url_classifier/%s".format(name))
    
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
    get_url_user_agent(spark, country = country, since = since, ndays = ndays, gtDF = gtDF, joinType = "inner",name="dataset_user_agent_training")
  }
}
