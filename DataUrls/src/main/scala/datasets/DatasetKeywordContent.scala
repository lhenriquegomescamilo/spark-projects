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

object DatasetKeywordContent {

  def getKeywordsByURL(
      spark: SparkSession,
      ndays: Int,
      since: Int
  ): DataFrame = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    /// Leemos la data de keywords de ndays hacia atras
    val format = "yyyy-MM-dd"
    val start = DateTime.now.minusDays(since + ndays)
    val end = DateTime.now.minusDays(since)

    val daysCount = Days.daysBetween(start, end).getDays()
    val days =
      (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

    val dfs = (0 until daysCount)
      .map(start.plusDays(_))
      .map(_.toString(format))
      .filter(
        day =>
          fs.exists(
            new Path("/datascience/selected_keywords/%s.csv".format(day))
          )
      )
      .map(day => "/datascience/selected_keywords/%s.csv".format(day))

    val df = spark.read
      .format("csv")
      .load(dfs: _*)
      .withColumnRenamed("_c0", "url")
      .withColumnRenamed("_c1", "count")
      .withColumnRenamed("_c2", "country_web")
      .withColumnRenamed("_c3", "content_keys")
      .withColumnRenamed("_c4", "scores")
      .withColumn("content_keys", split(col("content_keys"), " "))
      .withColumn(
        "url",
        regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
      )
      .drop("count", "scores")
      .dropDuplicates("url")

    df
  }

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

  def get_url_content(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      country: String,
      replicationFactor: Int = 4,
      gtDF: DataFrame,
      joinType:String
  ): DataFrame =  {

    // We add the composite key to the gt data in order to do an improved join
    val urls = gtDF.withColumn(
      "composite_key",
      concat(
        col("url"),
        lit("@"),
        // This last part is a random integer ranging from 0 to replicationFactor
        least(
          floor(rand() * replicationFactor),
          lit(replicationFactor - 1) // just to avoid unlikely edge case
        )
      )
    )

    // Get Dataset with content keywords and urls and add composite key
    val URLkeys = (0 until replicationFactor)
      .map(
        i =>
          getKeywordsByURL(spark, 10, since)
            .withColumn("composite_key", concat(col("url"), lit("@"), lit(i)))
      )
      .reduce((df1, df2) => df1.unionAll(df2))
      .drop("url")

    // Smart join between data GT (<url, segments>) and urls with content_keywords
    val joint = urls
      .join(URLkeys, Seq("composite_key"),joinType)
      .drop("composite_key","count")
      .withColumn("content_keys", explode(col("content_keys")))
      .withColumn("country", lit(country))
      .groupBy("url", "content_keys","segments","country")
      .agg(count("url").as("count"))

    joint.write
      .mode(SaveMode.Overwrite)
      .partitionBy("country")
      .save("/datascience/data_url_classifier/dataset_keywords")

    joint

  }

  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder
      .appName("Data URLs: Dataset Keyword Content")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .getOrCreate()

    // Parseo de parametros
    val ndays = if (args.length > 0) args(0).toInt else 10
    val since = if (args.length > 1) args(1).toInt else 1
    val country = if (args.length > 2) args(2).toString else ""
    val segments = List(129, 59, 61, 250, 396, 150, 26, 32, 247, 3013, 3017)

    val gtDF = get_url_gt(spark,ndays,since,country,segments)

    get_url_content(spark, country = country, since = since, ndays = ndays, gtDF = gtDF, joinType = "inner" )
  }
}
