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
import org.apache.log4j.{Level, Logger}

object DatasetKeywordsURL{

  /**
  * This method returns a DataFrame with the data from the data urls pipeline, for the interval
  * of days specified. Basically, this method loads the given path as a base path, then it
  * also loads the every DataFrame for the days specified, and merges them as a single
  * DataFrame that will be returned.
  *
  * @param spark: Spark Session that will be used to load the data from HDFS.
  * @param nDays: number of days that will be read.
  * @param since: number of days ago from where the data is going to be read.
  *
  * @return a DataFrame with the information coming from the data read.
  **/
  def getDataUrls(
      spark: SparkSession,
      country: String,
      nDays: Int = 30,
      since: Int = 1
  ): DataFrame = {

    /// Configuraciones de spark
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    /// Obtenemos la data de los ultimos ndays
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(since)

    val days =
      (0 until nDays).map(start.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_demo/data_urls/"
    val hdfs_files = days.map(day => path + "/day=%s/country=%s".format(day,country))
                  .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    df
  }

  /**
   * This function gets the data from the URLs. Basically it stores the list of URLs per device id separated by ';'.
   * 
   * @param spark: Spark session that will be used to load the data.
   * @param gtDF: dataframe where the ground truth users are stored. This dataframe must have a column called 'device_id' 
   * and another called 'label'.
   * @param country: country for which the triplets of data is going to be loaded.
   * @param joinType: type of join that will be performed. It can be either 'inner' or 'left' or 'left_anti'.
   * @param name: name for the folder where the dataset will be stored.
   * 
   * It stores the data (tuples where the first column is the device_id and the second one is the list of urls separated by ';') in
                          /datascience/data_demo/name={name}/{country}/triplets.
  */

  def getDatasetFromURLs(
        spark: SparkSession,
        gtDF: DataFrame,
        country: String,
        joinType: String,
        name: String,
        ndays:Int,
        format_type:String
    ) = {
      
      // Data from data urls
      val df = getDataUrls(spark,country,ndays)
        .filter("event_type IN ('pv', 'batch')")
        .select("device_id", "url")

      // Remove qs and generic urls
      val df_processed = UrlUtils.processURL(df,"url")
                                  .select("device_id", "url")
                                  .withColumn("url", lower(col("url")))
                                  .withColumn("url_path", regexp_replace(col("url"), """^[^/]*/""", ""))
                                  .withColumn("url_keys", split(col("url_path"), "[^a-z0-9]"))
                                  .withColumn("keyword", explode(col("url_keys")))
                                  .filter(col("keyword").rlike("[a-z]{2,}"))
                                  .filter("keyword != 'html'")
                                  .select("device_id","url","keyword")
                                  .distinct()

      // Checkpoint to execute url process and tokenization
      df_processed.write
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .save(
            "/datascience/data_demo/name=%s/country=%s/keywords_tmp".format(name, country)
          )
      
      val processed_join = spark.read
                                .load("/datascience/data_demo/name=%s/country=%s/keywords_tmp".format(name, country))
      
      processed_join.cache()

      // Calculating top 5000 keywords
      val top_keywords = processed_join.groupBy("keyword")
                                      .agg(count(col("url")).as("count"))
                                      .orderBy(desc("count"))
                                      .limit(5000) // Top 5000 keywords


      // Filter and get top keywords                          
      val filtered_df = processed_join.join(broadcast(top_keywords),Seq("keyword"),"inner")
                                        .select("device_id","keyword")
                                        .groupBy("device_id")
                                        .agg(collect_list(col("keyword")).as("keyword"))
                                        .withColumn("keyword", concat_ws(";", col("keyword")))
                                        

      // Join with GT and save
      val join = gtDF.join(filtered_df, Seq("device_id"), joinType )
                      .select("device_id","keyword")
                      .orderBy(asc("device_id"))
                      .write
                      .mode(SaveMode.Overwrite)
                      .format(format_type)
                      .save(
                        "/datascience/data_demo/name=%s/country=%s/keywords".format(name, country)
                      )
    }
  
  def main(args: Array[String]) {

     // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    /// Configuracion spark
    val spark = SparkSession.builder
      .appName("Generate Data urls")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .getOrCreate()

    val segments = spark.read
                    .format("csv")
                    .load(
                      "/datascience/data_demo/name=expansion_MX_genero_10/country=MX/segment_triplets"
                    ).withColumnRenamed("_c0","device_id")
    getDatasetFromURLs(spark,segments,"MX","left","expansion_MX_genero_10",30,"csv")
  }
}
