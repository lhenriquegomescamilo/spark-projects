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

object DatasetReferer {

  def get_url_referer(spark: SparkSession,ndays: Int,since: Int,country: String,gtDF: DataFrame,
                      joinType:String, df_urls: DataFrame, name:String): DataFrame =  {
    
    // First we get the data from urls, we filter it and we group it (<url, referer, count>)
    val data_urls = df_urls
                      .filter("referer is not null")
                      .select("device_id","url","referer")
                      .withColumn("count", lit(1))
                      .groupBy("device_id","url", "referer")
                      .agg(sum("count").as("count"))

    // Then we join the data with the GT
    val joint = gtDF.join(data_urls, Seq("url"), joinType)
                    .select("device_id","url","referer","count")
                    .withColumn("country",lit(country))
                    .dropDuplicates()
    
    joint.write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .partitionBy("country")
          .save("/datascience/data_url_classifier/%s".format(name))

    joint
  }

  def main(args: Array[String]) {
    // Spark configuration
    val spark = SparkSession.builder
      .appName("Data URLs: Dataset Referer")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .getOrCreate()

    // Parseo de parametros
    val ndays = if (args.length > 0) args(0).toInt else 10
    val since = if (args.length > 1) args(1).toInt else 1
    val country = if (args.length > 2) args(2).toString else ""
    val segments = List(129, 59, 61, 250, 396, 150, 26, 32, 247, 3013, 3017)

    //val data_urls = get_data_urls(spark, ndays, since, country)
    //val gtDF = spark.read.load("/datascience/data_url_classifier/dataset_keywords/country=AR")
    //get_url_referer(spark, country = country, since = since, ndays = ndays, gtDF = gtDF, joinType = "left", df_urls = data_urls)
  }
}
