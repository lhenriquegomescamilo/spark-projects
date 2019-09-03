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

object DatasetReferer {

  def get_data_urls(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      country: String
  ): DataFrame = {
    /// Configuraciones de spark
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    /// Obtenemos la data de los ultimos ndays
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(since)

    val days =
      (0 until ndays).map(start.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_demo/data_urls/"
    val dfs = days
      .map(day => path + "/day=%s/country=%s".format(day, country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      .map(
        x =>
          spark.read
            .option("basePath", path)
            .parquet(x)
            .withColumn("day", lit(x.split("/").last.slice(4, 13)))
      )

    val urls = dfs
      .reduce((df1, df2) => df1.union(df2))
      .withColumn(
        "url",
        regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
      )

    urls
  }

  def get_url_gt(spark: SparkSession, ndays: Int, since: Int, country: String, segments:List[Int]): DataFrame = {
    val data_urls = get_data_urls(spark, ndays, since, country)

    val filtered = data_urls
      .select("url", "segments")
      .withColumn("segments", explode(col("segments")))
      .filter(
        col("segments")
          .isin(segments: _*)
      )

    filtered
  }

  def get_url_referer(spark: SparkSession,ndays: Int,since: Int,country: String,gtDF: DataFrame,joinType:String): DataFrame =  {
    // First we get the data from urls, we filter it and we group it (<url, referer, count>)
    val data_urls = get_data_urls(spark, ndays, since, country)
                                  .filter("referer is not null")
                                  .select("url","referer")
                                  .groupBy("url", "referer")
                                  .count()

    // Then we join the data with the GT
    val joint = gtDF.join(data_urls, Seq("url"), joinType)
        .select("url","referer","count")
        .orderBy(asc("url"))
        .withColumn("country",lit(country))
    
    joint.write
          .mode(SaveMode.Overwrite)
          .partitionBy("country")
          .save("/datascience/data_url_classifier/dataset_referer")

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

    val gtDF = get_url_gt(spark,ndays,since,country,segments)

    get_url_referer(spark, country = country, since = since, ndays = ndays, gtDF = gtDF, joinType = "inner")
  }
}