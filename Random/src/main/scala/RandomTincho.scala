package main.scala
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
import org.apache.log4j.{Level, Logger}

object RandomTincho {

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
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val urls = spark.read
                    .option("basePath", path)
                    .parquet(hdfs_files: _*)
                    .select("url")
    urls
  }

  def get_selected_keywords(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      country: String
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
      .option("header","true")
      .load(dfs: _*)
      .filter("country = '%s'".format(country))
      .withColumnRenamed("kw", "kws")
      .withColumnRenamed("url_raw", "url")
      .withColumn("kws", split(col("kws"), " "))
      .select("url","kws")
      .dropDuplicates("url")


    df
  }

  def get_gt_new_taxo(spark: SparkSession, ndays:Int, since:Int, country:String) = {
    
    // Get selected keywords <url, [kws]>
    val selected_keywords = get_selected_keywords(spark, ndays = ndays, since = since, country = country)
    selected_keywords.cache()

    // Get Data urls <url>
    val data_urls = get_data_urls(spark, ndays = ndays, since = since, country = country)
    data_urls.cache()

    // Get queries <seg_id, query (array contains), query (url like)>
    var queries = spark.read.load("/datascience/custom/new_taxo_queries_join")
    
    var dfs: DataFrame = null
    var first = true
    // Iterate queries and get urls
    for (row <- queries.rdd.collect){  
      var segment = row(0).toString
      var query = row(1).toString
      var query_url_like = row(2).toString

      // Filter selected keywords dataframe using query with array contains
      selected_keywords.filter(query)
                        .withColumn("segment",lit(segment))
                        .select("url","segment")
                        .write
                        .format("parquet")
                        .mode("append")
                        .save("/datascience/data_url_classifier/GT_new_taxo_queries")

      // Filter data_urls dataframe using query with array url LIKE                        
      data_urls.filter(query_url_like)
                  .withColumn("segment",lit(segment))
                  .select("url","segment")
                  .write
                  .format("parquet")
                  .mode("append")
                  .save("/datascience/data_url_classifier/GT_new_taxo_queries")
    }

    // Groupby by url and concatenating segments with ;
    spark.read.load("/datascience/data_url_classifier/GT_new_taxo_queries")
          .groupBy("url")
          .agg(collect_list(col("segment")).as("segment"))
          .withColumn("segment", concat_ws(";", col("segment")))
          .write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .save("/datascience/data_url_classifier/GT_new_taxo")
  }


 def get_segments_pmi(spark:SparkSession, ndays:Int, since:Int){

   val files = List("/datascience/misc/cookies_chesterfield.csv",
                 "/datascience/misc/cookies_marlboro.csv",
                 "/datascience/misc/cookies_phillip_morris.csv",
                 "/datascience/misc/cookies_parliament.csv")

   /// Configuraciones de spark
   val sc = spark.sparkContext
   val conf = sc.hadoopConfiguration
   val fs = org.apache.hadoop.fs.FileSystem.get(conf)

   val format = "yyyyMMdd"
   val start = DateTime.now.minusDays(since)

   val days = (0 until ndays).map(start.minusDays(_)).map(_.toString(format))
   val path = "/datascience/data_triplets/segments/"
   val dfs = days.map(day => path + "day=%s/".format(day) + "country=AR")
     .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
     .map(
       x =>
         spark.read
           .option("basePath", "/datascience/data_triplets/segments/")
           .parquet(x)
     )

   var data = dfs.reduce((df1, df2) => df1.union(df2))

   for (filename <- files){
     var cookies = spark.read.format("csv").load(filename)
                                         .withColumnRenamed("_c0","device_id")

     data.join(broadcast(cookies),Seq("device_id"))
         .select("device_id","feature","count")
         .dropDuplicates()
         .write.format("parquet")
         .mode(SaveMode.Overwrite)
         .save("/datascience/custom/segments_%s".format(filename.split("/").last.split("_").last))
   }
 }
 def test_tokenizer(spark:SparkSession){

    var gtDF = spark.read
                    .load("/datascience/data_url_classifier/gt_new_taxo_filtered")
                    .withColumn("url", lower(col("url")))
                    .withColumn("url_path", regexp_replace(col("url"), """^[^/]*/""", ""))
                    .withColumn("url_keys", split(col("url_path"), "[^a-z0-9]"))
                    .withColumn("keyword", explode(col("url_keys")))
                    .filter(col("keyword").rlike("[a-z]{2,}"))
                    .groupBy("url","segment").agg(collect_list(col("keyword").as("keywords")))
                    .write
                    .format("parquet")
                    .mode(SaveMode.Overwrite)
                    .save("/datascience/data_url_classifier/gt_new_taxo_tokenized")
 }

  def main(args: Array[String]) {
     
    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder
        .appName("Random de Tincho")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .getOrCreate()
    
    test_tokenizer(spark)
  }

}
