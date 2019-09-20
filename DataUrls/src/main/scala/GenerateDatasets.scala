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

  def generate_expansion_datasets(spark:SparkSession,ndays:Int,since:Int,country:String,data_urls: DataFrame){
    
    // First we get the untagged urls
    val untagged_df = broadcast(get_data_untagged(spark,ndays,since,country))
    untagged_df.cache()

    // Then we download each dataset making an inner join with the untagged urls
    val data_keywords_content = DatasetKeywordContent.get_url_content(spark,
                                                      country = country,
                                                      since = since,
                                                      ndays = ndays,
                                                      gtDF = untagged_df,
                                                      joinType = "inner",
                                                      name = "dataset_keyword_content_expansion")

    val data_referer = DatasetReferer.get_url_referer(spark,
                                                      country = country,
                                                      since = since,
                                                      ndays = ndays,
                                                      gtDF = untagged_df,
                                                      joinType = "inner",
                                                      df_urls = data_urls,
                                                      name = "dataset_referer_expansion")

    val data_timestamp = DatasetTimestamp.get_url_timestamp(spark,
                                                      country = country,
                                                      since = since,
                                                      ndays = ndays,
                                                      gtDF = untagged_df,
                                                      joinType = "inner",
                                                      df_urls = data_urls,
                                                      name = "dataset_timestamp_expansion")

    val data_user_agent = DatasetUserAgent.get_url_user_agent(spark,
                                                    ndays,
                                                    since,
                                                    country,
                                                    untagged_df,
                                                    "inner",
                                                    name = "dataset_user_agent_expansion")

    val data_segments_branded = DatasetSegmentsBranded.get_segment_branded(spark,
                                                    ndays,
                                                    since,
                                                    country,
                                                    untagged_df,
                                                    "inner",
                                                    name = "dataset_segments_branded_expansion")
  }

  def generate_training_datasets(spark:SparkSession,ndays:Int,since:Int,country:String,data_urls: DataFrame){
    // Defining segments for GT
    val segments = List(26,   32,   36,   59,   61,   82,   85,   92,  104,  118,  129,
                        131,  141,  144,  145,  147,  149,  150,  152,  154,  155,  158,
                        160,  165,  166,  177,  178,  210,  213,  218,  224,  225,  226,
                        230,  245,  247,  250,  264,  265,  270,  275,  276,  302,  305,
                        311,  313,  314,  315,  316,  317,  318,  322,  323,  325,  326,
                        2635, 2636, 2660, 2719, 2743, 3010, 3011, 3012, 3013, 3014, 3015,
                        3016, 3017, 3018, 3019, 3020, 3021, 3022, 3055, 3076, 3077, 3086,
                        3087, 3913, 4097)     

    // Filtering data from url to get GT segments
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
     
    val data_urls = get_data_urls(spark, ndays, since, country)

    // Training datasets
    generate_training_datasets(spark,ndays,since,country,data_urls)
    // Expansion datasets
    //generate_expansion_datasets(spark,ndays,since,country,data_urls)
  }
}
