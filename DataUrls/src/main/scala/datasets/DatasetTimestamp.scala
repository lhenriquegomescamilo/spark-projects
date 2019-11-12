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

object DatasetTimestamp {

  def get_url_timestamp(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      country: String,
      gtDF: DataFrame,
      joinType: String,
      df_urls: DataFrame,
      name: String
  ): DataFrame = {
    // Generate dataset with columns weekday and hour
    val myUDF = udf(
      (weekday: String, hour: String) =>
        if (weekday == "Sunday" || weekday == "Saturday") "weekend" else "week"
    )

    val myUDFTime = udf(
      (hour: String) =>
        if (List("09", "10", "11", "12", "13").contains(hour)) "morning"
        else if (List("14", "15", "16", "17", "18", "19", "20", "21")
                   .contains(hour)) "afternoon"
        else "night"
    )

    val UDFFinal = udf(
      (daytime: String, wd: String) => "%s_%s".format(daytime, wd)
    )

    // First we get the data from urls (<url, time>)
    val data_urls = df_urls
      .select("url", "time")
      .withColumn("Hour", date_format(col("time"), "HH"))
      .withColumn("Weekday", date_format(col("time"), "EEEE"))
      .withColumn("wd", myUDF(col("Weekday"), col("Hour")))
      .withColumn("daytime", myUDFTime(col("Hour")))
      .withColumn("feature", UDFFinal(col("daytime"), col("wd")))
      .groupBy("url", "feature")
      .count()

    // Join with the GT dataframe
    val joint = gtDF
      .join(data_urls, Seq("url"), joinType)
      .select("url", "feature","count")
      .withColumn("country", lit(country))

    // Groupby and pivot by timestamp feature
    joint
      .groupBy("url")
      .pivot("feature")
      .agg(sum("count"))
      .na
      .fill(0)
      .withColumn("country", lit(country))
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
      .appName("Data URLs: Dataset Timestamp")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    // Parseo de parametros
    val ndays =  30
    val since =  1
    val country = "AR"
    val ndays_dataset = 30

    val data_urls = UrlUtils.get_data_urls(spark, ndays, since, country)

    val urls = spark.read
                          .format("csv")
                          .option("header","true")
                          .load("/datascience/custom/scrapped_urls.csv")
                          .select("url")

    val gtDF = UrlUtils.processURL(urls)

    get_url_timestamp(
      spark,
      country = country,
      since = since,
      ndays = ndays_dataset,
      gtDF = gtDF,
      joinType = "inner",
      df_urls = data_urls,
      name = "dataset_timestamp_contextual"
    )
  }
}
