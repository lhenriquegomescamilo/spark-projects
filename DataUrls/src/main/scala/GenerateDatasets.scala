package main.scala
import main.scala.datasets.{DatasetKeywordContent, DatasetReferer, DatasetTimestamp, DatasetUserAgent, DatasetSegmentsBranded}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}

object GenerateDatasetsUrls {

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
      .withColumn(
        "url",
        regexp_replace(col("url"), "(\\?|#).*", "")
      )

    urls
  }

  def get_data_untagged(spark: SparkSession, ndays: Int, since: Int, country: String): DataFrame = {
    // Configuraciones de spark
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    /// Obtenemos la data de los ultimos ndays
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(since)

    val days =
      (0 until ndays).map(start.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_url_classifier/untagged_urls/"
    val dfs = days
      .map(day => path + "/day=%s/country=%s".format(day, country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      .map(
        x =>
          spark.read
            .option("basePath", path)
            .parquet(x)
      )

    val untagged = dfs.reduce((df1, df2) => df1.union(df2)).select("url")

    untagged
  }


 def main(args: Array[String]) {
    // Spark configuration
    val spark = SparkSession.builder
      .appName("Data URLs: Generate All datasets")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .getOrCreate()

    // Parseo de parametros
    val ndays = if (args.length > 0) args(0).toInt else 10
    val since = if (args.length > 1) args(1).toInt else 1
    val country = if (args.length > 2) args(2).toString else ""
    val segments = List(129, 59, 61, 250, 396, 150, 26, 32, 247, 3013, 3017,  // Interest
                        396,2720,2737,3034,955,385,3420,3564,411,363,         // Intent
                        3582,3600,3580,462,3035,446,3779,916,3587,3040)       // Intent

    
//////////////////////////////////////// Training Data ////////////////////////////////////////
    val data_urls = get_data_urls(spark, ndays, since, country)

    var gtDF = data_urls.select("url", "segments")
                        .withColumn("segments", explode(col("segments")))
                        .filter(
                          col("segments")
                            .isin(segments: _*)
                        ).distinct()
    
    // Saving GT dataframe grouped by url and list of segments
    gtDF.groupBy("url")
        .agg(collect_list(col("segments")).as("segments"))
        .withColumn("segments", concat_ws(";", col("segments")))
        .withColumn("country",lit(country))
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .partitionBy("country")
        .save("/datascience/data_url_classifier/gt")
    
    gtDF = broadcast(gtDF.select("url"))
    gtDF.cache()
    
    var data_keywords_content = DatasetKeywordContent.get_url_content(spark,
                                                        country = country,
                                                        since = since,
                                                        ndays = ndays,
                                                        gtDF = gtDF,
                                                        joinType = "inner",
                                                        name = "dataset_keyword_content_training")

    var data_referer = DatasetReferer.get_url_referer(spark,
                                                        country = country,
                                                        since = since,
                                                        ndays = ndays,
                                                        gtDF = gtDF,
                                                        joinType = "inner",
                                                        df_urls = data_urls,
                                                        name = "dataset_referer_training" )

    var data_timestamp = DatasetTimestamp.get_url_timestamp(spark,
                                                        country = country,
                                                        since = since,
                                                        ndays = ndays,
                                                        gtDF = gtDF,
                                                        joinType = "inner",
                                                        df_urls = data_urls,
                                                        name = "dataset_timestamp_training")

    var data_user_agent = DatasetUserAgent.get_url_user_agent(spark,
                                                      ndays,
                                                     since,
                                                      country,
                                                      gtDF,
                                                      "inner",
                                                      name = "dataset_user_agent_training")

    var data_segments_branded = DatasetSegmentsBranded.get_segment_branded(spark,
                                                      ndays,
                                                     since,
                                                      country,
                                                      gtDF,
                                                      "inner",
                                                      name = "dataset_segments_branded_training")


  //////////////////////////////////////// Test Data ////////////////////////////////////////

  // First we get the untagged urls
  val untagged_df = broadcast(get_data_untagged(spark,ndays,since,country))
  untagged_df.cache()

  // Then we download each dataset making an inner join with the untagged urls

  data_keywords_content = DatasetKeywordContent.get_url_content(spark,
                                                    country = country,
                                                    since = since,
                                                    ndays = ndays,
                                                    gtDF = gtDF,
                                                    joinType = "inner",
                                                    name = "dataset_keyword_content_expansion")

  data_referer = DatasetReferer.get_url_referer(spark,
                                                    country = country,
                                                    since = since,
                                                    ndays = ndays,
                                                    gtDF = gtDF,
                                                    joinType = "inner",
                                                    df_urls = data_urls,
                                                    name = "dataset_referer_expansion")

  data_timestamp = DatasetTimestamp.get_url_timestamp(spark,
                                                    country = country,
                                                    since = since,
                                                    ndays = ndays,
                                                    gtDF = gtDF,
                                                    joinType = "inner",
                                                    df_urls = data_urls,
                                                    name = "dataset_timestamp_expansion")

  data_user_agent = DatasetUserAgent.get_url_user_agent(spark,
                                                  ndays,
                                                  since,
                                                  country,
                                                  gtDF,
                                                  "inner",
                                                  name = "dataset_user_agent_expansion")

  data_segments_branded = DatasetSegmentsBranded.get_segment_branded(spark,
                                                  ndays,
                                                  since,
                                                  country,
                                                  gtDF,
                                                  "inner",
                                                  name = "dataset_segments_branded_expansion")
  
  }
}
