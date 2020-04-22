package main.scala
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.apache.commons.lang3.StringUtils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.log4j.{Level, Logger}

/**
  * The idea of this script is to Ingest Urls daily to local servers for Scrapper.
  */
object UrlIngester {

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR LOADING DATA     //////////////////////
    *
    */
  /**
    * This method returns a DataFrame with the data from the "data_demo" pipeline, for the interval
    * of days specified. Basically, it loads every DataFrame for the days specified, and merges them as a single
    * DataFrame that will be returned.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.
    *
    * @return a DataFrame with the information coming from the read data.
   **/
  def getDataUrls(
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
    val path = "/datascience/data_demo/data_urls"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .filter("share_data = 1")
      .select("url", "country", "day")
    df
  }

//////////////////////////////////////////////////////////////

  def getData(
      spark: SparkSession,
      nDays: Integer,
      date_current: String,
      path: String
  ): DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val formatter = DateTimeFormat.forPattern(format)
    val end = DateTime.parse(date_current, formatter)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)

    df
  }

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR TRANSFORMING DATA     //////////////////////
    *
    */
  /**
    * This method processes urls from a dataframe obtaining apt urls for further scrapping.
    *
    * @param dfURL: DataFrame with urls.
    *
    * @return a DataFrame with processed urls.
   **/
  def processURLHTTP(dfURL: DataFrame, field: String = "url"): DataFrame = {
    // First of all, we get the domains, and filter out those ones that are very generic

    val generic_domains = List(
      "google",
      "doubleclick",
      "facebook",
      "messenger",
      "yahoo",
      "android",
      "android-app",
      "bing",
      "instagram",
      "cxpublic",
      "content",
      "cxense",
      "criteo",
      "outbrain",
      "flipboard",
      "googleapis",
      "googlequicksearchbox",
      "0_media",
      "provider",
      "parser",
      "downloads",
      "xlxx",
      "xvideo2",
      "coffetube"
    )
    val query_generic_domains = generic_domains
      .map(dom => "domain NOT LIKE '%" + dom + "%'")
      .mkString(" AND ")
    val filtered_domains = dfURL
      .selectExpr("*", "parse_url(%s, 'HOST') as domain".format(field))
      .filter(query_generic_domains)
    // Now we filter out the domains that are IPs
    val filtered_IPs = filtered_domains
      .withColumn(
        "domain",
        regexp_replace(col("domain"), "^([0-9]+\\.){3}[0-9]+$", "IP")
      )
      .filter("domain != 'IP'")
    // Now if the host belongs to Retargetly, then we will take the r_url field from the QS
    val retargetly_domains = filtered_IPs
      .filter("domain LIKE '%retargetly%'")
      .selectExpr(
        "*",
        "parse_url(%s, 'QUERY', 'r_url') as new_url".format(field)
      )
      .filter("new_url IS NOT NULL")
      .withColumn(field, col("new_url"))
      .drop("new_url")
    // Then we process the domains that come from ampprojects
    val pattern =
      """^([a-zA-Z0-9_\-]+).cdn.ampproject.org/?([a-z]/)*([a-zA-Z0-9_\-\/\.]+)?""".r
    def ampPatternReplace(url: String): String = {
      var result = ""
      if (url != null) {
        val matches = pattern.findAllIn(url).matchData.toList
        if (matches.length > 0) {
          val list = matches
            .map(
              m =>
                if (m.groupCount > 2) m.group(3)
                else if (m.groupCount > 0) m.group(1).replace("-", ".")
                else "a"
            )
            .toList
          result = list(0).toString
        }
      }
      result
    }
    val ampUDF = udf(ampPatternReplace _, StringType)
    val ampproject_domains = filtered_IPs
      .filter("domain LIKE '%ampproject%'")
      .withColumn(field, ampUDF(col(field)))
      .filter("length(%s)>0".format(field))
    // Now we union the filtered dfs with the rest of domains
    val non_filtered_domains = filtered_IPs.filter(
      "domain NOT LIKE '%retargetly%' AND domain NOT LIKE '%ampproject%'"
    )
    val filtered_retargetly = non_filtered_domains
      .unionAll(retargetly_domains)
      .unionAll(ampproject_domains)
    // Finally, we remove the querystring and protocol
    filtered_retargetly
      .withColumn(
        field,
        regexp_replace(col(field), "://(.\\.)*", "://")
      )
      .withColumn(
        field,
        regexp_replace(col(field), "(\\?|#).*", "")
      )
      .drop("domain")
      .withColumn(field, lower(col(field)))
      .withColumn(
        field,
        regexp_replace(col(field), "@", "_")
      )
  }

  def get_processed_urls(spark:SparkSession,since:Int,ndays:Int): DataFrame = {
    // Get device_id, segment from segments triplets using 30 days
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until ndays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/scraper/selected_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/".format(day)) 
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path)))

    val df = spark.read
                  .format("csv")
                  .option("basePath", path)
                  .load(hdfs_files: _*)
                  .withColumnRenamed("_c0", "url")
                  .select("url")
                  .distinct
    df
  }

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR SAVING DATA     //////////////////////
    *
    */
  /**
    * This method saves file partitioned by date and country.
    *
    * @param data: DataFrame that will be saved.
    * @param path: path to save to.
    *
  **/
  def saveData(
      data: DataFrame,
      path: String
  ) = {

    data.write
      .format("parquet")
      .partitionBy("day", "country")
      .mode(SaveMode.Overwrite)
      .save(path)
  }

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     MAIN METHOD     //////////////////////
    *
    */
  /**
    * This method returns a DataFrame with the data from the "data_triplets" pipeline, for the interval
    * of days specified. Basically, it loads every DataFrame for the days specified, and merges them as a single
    * DataFrame that will be returned.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.
    *
    * @return a DataFrame with the information coming from the read data. Columns: "seg_id","id_partner" and "device_id"
   **/
  def get_urls_for_ingester(
      spark: SparkSession,
      nDays: Integer,
      since: Integer,
      urls_limit: Integer
  ) {

    val date_now = DateTime.now
    val date_since = date_now.minusDays(since)
    val date_current = date_since.toString("yyyyMMdd")

    println("INFO:\n\tDay: %s".format(date_current))

    /**  Load data */
    val db = getDataUrls(spark = spark, nDays = nDays, since = since)
    println("Count del dataframe inicial: %s".format(db.select("url").distinct.count))

    /** Preprocess URLS **/
    val udfStrip = udf((colValue: String) => {StringUtils.stripAccents(colValue)})

    val url_limit = 500
    val df = processURLHTTP(db).withColumn("len",length(col("url")))
                                .filter("len <= %s".format(url_limit))  // removes urls that are too long
                                .withColumn("url",udfStrip(col("url"))) // remove accents
                                .withColumn("url",regexp_replace(col("url"), "./", "/") //replace . before / for urls like https://www.ambito.com./

    /** Filter Urls processed within 7 days */
    val processed_urls = get_processed_urls(spark,0,7)
    val df_filtered = df.join(processed_urls,Seq("url"),"left_anti")
    
    df_filtered.cache()

    /** Process and store the Data for each country */
    val countries = "AR,BO,BR,CL,CO,EC,MX,PE,UY,VE,US".split(",").toList

    val savepath = "/datascience/url_ingester/data"

    val replicationFactor = 8

    for (country <- countries) {
      df_filtered.filter("country = '%s'".format(country))
        .withColumn(
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
        .groupBy("composite_key")
        .count
        .withColumn("split", split(col("composite_key"), "@"))
        .withColumn("url", col("split")(0))
        .groupBy("url")
        .agg(sum(col("count")).as("count"))
        .sort(desc("count"))
        .limit(urls_limit)
        .withColumn("country", lit(country))
        .withColumn("day", lit(date_current))
        .select("url", "country", "count", "day")
        .write
        .format("parquet")
        .partitionBy("day", "country")
        .mode(SaveMode.Overwrite)
        .save(savepath)
    }

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
      case "--urls_limit" :: value :: tail =>
        nextOption(map ++ Map('urls_limit -> value.toInt), tail)
    }
  }

  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(Args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), Args.toList)
    val nDays = if (options.contains('nDays)) options('nDays) else 1
    val since = if (options.contains('since)) options('since) else 1
    val urls_limit =
      if (options.contains('urls_limit)) options('urls_limit)
      else 1000000 
    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("UrlIngester")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    get_urls_for_ingester(
      spark = spark,
      nDays = nDays,
      since = since,
      urls_limit = urls_limit
    )
  }
}
