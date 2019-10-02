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

object DatasetSegmentsBranded {

  def get_triplets_segments(
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
    val path = "/datascience/data_triplets/segments/"
    val dfs = days
      .map(day => path + "/day=%s/country=%s".format(day, country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      .map(
        x =>
          spark.read
            .option("basePath", path)
            .parquet(x)
      )

    val segments = dfs.reduce((df1, df2) => df1.union(df2))

    segments
  }

  def get_segment_branded(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      country: String,
      gtDF: DataFrame,
      joinType: String,
      name: String
  ): DataFrame = {

    val branded_segments =
      List(20107, 20108, 20109, 20110, 20111, 20112, 20113, 20114, 20115, 20116,
        20117, 20118, 20119, 20120, 20121, 20122, 20123, 20124, 20125, 20126)

    // First we get the data from the segments (<device_id, segment>) and we take only branded segments
    val data_segments = get_triplets_segments(spark, ndays, since, country)
      .filter(col("feature").isin(branded_segments: _*))

    data_segments.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(
        "/datascience/data_url_classifier/dataset_user_segments_branded_%s"
          .format(name.split("_").last)
      )

    // Then we get the data from the url - user triplets (<device_id, url, count>)
    val data_url_user = spark.read
      .load("/datascience/data_triplets/urls/country=%s/".format(country)).drop("domain")
      
    val filtered_url_user = UrlUtils.processURL(dfURL = data_url_user, field = "url")

    // Then we join both datasets
    val joint = data_segments
      .join(filtered_url_user, Seq("device_id"), "inner")
      .withColumnRenamed("feature", "segment")

    // Finally we make the final join with the GT data
    val final_join = gtDF
      .join(joint, Seq("url"), joinType)
      .select("url", "segment", "count")
      .withColumn("country", lit(country))

    final_join.write
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
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    // Parseo de parametros
    val ndays = 30
    val since = 1
    val country = "AR"

    val gtDF = spark.read.load("/datascience/data_url_classifier/gt/country=AR")
    get_segment_branded(
      spark,
      country = country,
      since = since,
      ndays = ndays,
      gtDF = gtDF,
      joinType = "inner",
      name = "dataset_segments_branded_training"
    )
  }
}
