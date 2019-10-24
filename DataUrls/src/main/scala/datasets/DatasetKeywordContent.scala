package main.scala.datasets
import main.scala.datasets.{UrlUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
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
      .drop("count", "scores")
      .dropDuplicates("url")

    val filtered_df = UrlUtils.processURL(dfURL = df, field = "url")

    filtered_df
  }

  def get_url_content(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      country: String,
      replicationFactor: Int = 4,
      gtDF: DataFrame,
      joinType:String,
      name:String
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
    var keywords_content = urls.join(URLkeys, Seq("composite_key"),joinType)
                                .drop("composite_key")
                                .withColumn("content_keys", explode(col("content_keys")))
                                .withColumn("country", lit(country))
                                .withColumn("count", lit(1))
                                .groupBy("url", "content_keys","country")
                                .agg(sum("count").as("count"))
                                .select("url","content_keys","country","count")
                                .dropDuplicates()

    // Checkpoint
    keywords_content.write
                    .format("parquet")
                    .mode(SaveMode.Overwrite)
                    .partitionBy("country")
                    .save("/datascience/data_url_classifier/keywords_content_tmp")

    keywords_content = spark.read
                            .load("/datascience/data_url_classifier/keywords_content_tmp")

    keywords_content.cache()

    // Extracting keywords from path and add them to the dataset
    val keywords_path = keywords_content.select("url")
                              .withColumn("url", lower(col("url")))
                              .withColumn("url_path", regexp_replace(col("url"), """^[^/]*/""", ""))
                              .withColumn("url_keys", split(col("url_path"), "[^a-z0-9]"))
                              .withColumn("content_keys", explode(col("url_keys")))
                              .filter(col("content_keys").rlike("[a-z]{2,}"))
                              .withColumn("country", lit(country))
                              .withColumn("count", lit(1))
                              .select("url","content_keys","country","count")

    val keywords_union = keywords_content.union(keywords_path)
                              
    keywords_union.write
                  .format("parquet")
                  .mode(SaveMode.Overwrite)
                  .partitionBy("country")
                  .save("/datascience/data_url_classifier/%s".format(name))

    keywords_union
  }

  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder
      .appName("Data URLs: Dataset Keyword Content")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .getOrCreate()

    // Parseo de parametros
    val ndays = 10
    val since = 1
    val country = "AR"

    val gtDF = spark.read.load("/datascience/data_url_classifier/gt/country=AR/")
    get_url_content(spark, country = country, since = since, ndays = ndays, gtDF = gtDF, joinType = "inner", name = "dataset_keyword_content_training")
  }
}
