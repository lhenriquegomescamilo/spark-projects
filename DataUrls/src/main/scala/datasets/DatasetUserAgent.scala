package main.scala.datasets
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import spark.implicits._
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

  def get_url_user_agent(spark: SparkSession,ndays: Int,since: Int,country: String,gtDF: DataFrame,joinType:String,name:String): DataFrame =  {

    // Defining top 100 urls to take
    val top_ua = List("Android","Apple","C","C Plus","CAM-L03","Chrome","Chrome Mobile","Chrome Mobile WebView","Chrome Mobile iOS",
                      "E (4) Plus","Edge","Facebook","Firefox","Firefox Mobile","G (4)","G (5)","G (5) Plus","G (5S) Plus","G3","Generic",
                      "Generic_Android","H340AR","H440AR","Huawei","IE","IE Mobile","K120","K350","K430","LG","Lenovo","Linux","M250",
                      "M700","Mac OS X","Mobile Safari","Mobile Safari UI/WKWebView","Motorola","Nexus 5","Nokia","Opera","Other","Pinterest",
                      "SM-A105M","SM-A305G","SM-A505G","SM-A520F","SM-A720F","SM-G531M","SM-G532M","SM-G570M","SM-G610M","SM-G930F","SM-G935F",
                      "SM-G950F","SM-G955F","SM-G9600","SM-G9650","SM-J111M","SM-J200M","SM-J260M","SM-J320M","SM-J400M","SM-J410G","SM-J415G",
                      "SM-J510MN","SM-J600G","SM-J610G","SM-J700M","SM-J701M","SM-J710MN","SM-J810M","Safari","Samsung","Samsung Internet",
                      "Smartphone","Ubuntu","Windows","Windows Phone","X230","X240","XiaoMi","iOS","iPad","iPhone","iPhone10,2","iPhone10,3"
                      ,"iPhone10,5","iPhone11,8","iPhone7","iPhone8,1","iPhone8,2","iPhone8,4","iPhone9,1","iPhone9,2","iPhone9,3","iPhone9,4",
                      "moto e5","moto e5 play")

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
      ).filter(col("feature").isin(top_ua: _*))

    // Joining dataset with GT urls
    val joint = gtDF.join(features_ua,Seq("url"),joinType)
                    .dropDuplicates()

    // Adding all features as a fake df in order to get all column names in the final df
    val fake_df = top_ua.map(name => ("www.google.com",name,1)).toDF("url", "feature","count")
    val final_df = joint.union(fake_df)

    // Groupby and pivot by user agent
    final_df.groupBy("url")
          .pivot("feature")
          .agg(sum("count"))
          .na.fill(0)
          .withColumn("country",lit(country))
          .write
          .format("csv") // Using csv because there are problems saving in parquet with spaces in column names
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

    //val gtDF = spark.read.load("/datascience/data_url_classifier/gt/country=AR/")
    //get_url_user_agent(spark, country = country, since = since, ndays = ndays, gtDF = gtDF, joinType = "inner")
  }
}
