package main.scala.keywiser
import main.scala.crossdevicer.AudienceCrossDevicer

import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.joda.time.{Days, DateTime}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.log4j.{Level, Logger}

/**
  * The idea of this script is to generate audiences based on keywords obtained from url content. 
  */
object TermSearch {

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR LOADING DATA     //////////////////////
    *
    */

  def getSelectedKeywords(
      spark: SparkSession,
      nDays: Integer,
      since: Integer
  ): DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    val path = "/datascience/scraper/selected_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
    .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
    .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val df_selected = spark.read.format("csv")
    .option("basePath", path)
    .load(hdfs_files: _*)
    .toDF("url_raw","hits","country","kw","TFIDF","domain","stem_kw","day")
    .withColumnRenamed("url_raw","url")
    .select("url","kw")
    .withColumn("kw", split(col("kw"), " "))
    .withColumn("kw", explode(col("kw")))

    df_selected
  }

 def getDataUrls(
      spark: SparkSession,
      country: String,
      nDays: Integer,
      since: Integer
  ): DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_demo/data_urls"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day,country)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val data_urls = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .filter("share_data = 1")
      .select("url", "segments", "country", "day")
      .withColumn("segments", concat_ws(",", col("segments")))
    data_urls
  }

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR MERGING DATA     //////////////////////
    *
    */

  def getUrlsWithTerms(
      df_keys: DataFrame,
      df_selected: DataFrame): DataFrame = {

    val df_urls_terms = df_selected.join(broadcast(df_keys), Seq("kw"))
      .select("url")
      .withColumn("search_terms", lit(1))  

    df_urls_terms
  }

  def getUsers(
      df_urls_terms: DataFrame,
      data_urls: DataFrame): DataFrame = {

    val df_final =  data_urls.join(df_urls_terms,Seq("url"),"left").na.fill(0)  

    df_final
  }
  
/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     MAIN METHOD     //////////////////////
    *
    */

  def MainProcess(
      spark: SparkSession) = {
      
    import spark.implicits._
    
    val keywords = "coronavirus,covid-19,covid19,cov19,cov-19,pandemia,contagio,contagiarse,respiradores,lavandina,infectados"
    val trimmedList: List[String] = keywords.split(",").map(_.trim).toList
    val df_keys = trimmedList.toDF().withColumnRenamed("value", "kw")

    val df_selected = getSelectedKeywords(spark,10,1)

    val df_urls_terms = getUrlsWithTerms(df_keys,df_selected)

    val data_urls = getDataUrls(spark,"AR",7,1)    

    val df_final = getUsers(df_urls_terms,data_urls)

    df_final.write
      .format("parquet")
      .partitionBy("day")
      .mode(SaveMode.Overwrite)
      .save("/datascience/misc/covid_users")

    // SE le puede agregar nombres de behaviour  
    /**
    val taxo = spark.read.format("csv").option("header",true).load("/datascience/geo/Reports/Equifax/DataMixta/RelyTaxonomy_06_02_2020.csv")
        .filter(col("clusterParent").isin(0,1,2))
        .select("segmentId","name")
    

    df_final.withColumn("segmentId", explode(col("segments")))
        .join(taxo,Seq("segmentId"))
        .groupBy("url","device_id","day","search_terms").agg(collect_list("name").as("behaviour"))
        .withColumn("behaviour", concat_ws(",", col("behaviour")))




    */

  }


  type OptionMap = Map[Symbol, Int]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--nDays" :: value :: tail =>
        nextOption(map ++ Map('nDays -> value.toInt), tail)
      case "--since" :: value :: tail =>
        nextOption(map ++ Map('since -> value.toInt), tail)
    }
  }

  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(Args: Array[String]) {

    // Parse the parameters
    //val options = nextOption(Map(), Args.toList)
    //val nDays = if (options.contains('nDays)) options('nDays) else 30
    //val since = if (options.contains('since)) options('since) else 1

    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("TermSearch")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    MainProcess(spark)
    


  }

}