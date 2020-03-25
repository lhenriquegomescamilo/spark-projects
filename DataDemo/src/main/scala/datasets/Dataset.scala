package main.scala.datasets
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.hadoop.conf.Configuration

/**
  * The idea of this script is to generate the datasets that will be used to
  * train and predict the demographic attributes. It uses data from URLs and User Agents.
  */
object Dataset {

  /** This function preprocesses the URLs that belong to a DataFrame. It takes
    * as input a DataFrame and the name of the column that contains the URL. Based on that
    * it filters those URLs that are noise, that come from generic domains, that are IPs,
    * and extract the domain from it.
    *
    * As a result, it returns a new DataFrame with the URLs filtered and parsed.
    *
    * - dfURL: DataFrame containing at least one column holding the URLs.
    * - field: column name where the URL is stored.
    */
  def processURL(dfURL: DataFrame, field: String = "url"): DataFrame = {
    // First of all, we get the domains, and filter out those ones that are very generic
    val generic_domains = List(
      "google",
      "facebook",
      "yahoo",
      "android",
      "bing",
      "instagram",
      "cxpublic",
      "criteo",
      "outbrain",
      "flipboard",
      "googleapis",
      "googlequicksearchbox"
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
        regexp_replace(col(field), "http.*://(.\\.)*(www\\.){0,1}", "")
      )
      .withColumn(
        field,
        regexp_replace(col(field), "(\\?|#).*", "")
      )
      .drop("domain")
      .withColumn(field, lower(col(field)))
  }

  /** This function returns the URL daily dataset. This dataset contains all of the
    * organic URLs for a given country and the specified date range. The returned URLs
    * are previously parsed and filtered so that no noise is passed.
    *
    * - spark: SparkSession that will be used to load the dataset.
    * - ndays: number of days to be loaded.
    * - since: number of days to be skiped from today's date.
    * - country: URL source country.
    *
    * Returns a DataFrame with two columns: device_id and URL.
    */
  def get_data_urls(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      country: String
  ): DataFrame = {
    // Spark settings
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    // List of days to be loaded
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(since)

    // Loading the dataset
    val days =
      (0 until ndays).map(start.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_demo/data_urls/"
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val urls = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    processURL(dfURL = urls, field = "url").select("device_id", "url")
  }

  /** This function returns the User Agent daily dataset. This dataset contains
    * all of the user-agents for a given country and the specified date range.
    *
    * - spark: SparkSession that will be used to load the dataset.
    * - ndays: number of days to be loaded.
    * - since: number of days to be skiped from today's date.
    * - country: URL source country.
    *
    * Returns a DataFrame with two columns: device_id and user_agent.
    */
  def get_data_user_agents(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      country: String
  ): DataFrame = {
    // Spark settings
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    // List of days to be loaded
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(since)

    // Loading the dataset
    val days =
      (0 until ndays).map(start.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_useragents/"
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val user_agents =
      spark.read.option("basePath", path).parquet(hdfs_files: _*)

    user_agents.select("device_id", "user_agent")
  }

  /**
    * This function obtains the dataset that will be used for
    * the given country and the date range. It obtains the user_agent
    * and the list of URLs for each user.
    *
    * - spark: SparkSession that will be used to load the dataset.
    * - ndays: number of days to be loaded.
    * - since: number of days to be skiped from today's date.
    * - country: URL source country.
    */
  def getDataset(
      spark: SparkSession,
      country: String,
      nDays: Integer = 30,
      since: Integer = 1
  ) = {
    // First we get the user-agents, one per user.
    val user_agents =
      get_data_user_agents(spark, nDays, since, country)
        .select("device_id", "user_agent")
        .dropDuplicates("device_id")

    // Now we get the list of URLs for each user
    val urls =
      get_data_urls(spark, nDays, since, country)
        .select(
          "device_id",
          "url"
        )
        .distinct()
        .groupBy("device_id")
        .agg(collect_list("url") as "urls")

    val joint = user_agents
      .join(urls, Seq("device_id"), "left")
      .select("device_id", "user_agent", "urls")
      .withColumn("country", lit(country))
      .withColumn(
        "day",
        lit(DateTime.now.minusDays(since).toString("yyyyMMdd"))
      )

    joint.write
      .format("parquet")
      .partitionBy("country", "day")
      .mode("overwrite")
      .save("/datascience/data_demo/datasets/")
  }

  type OptionMap = Map[Symbol, Any]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--from" :: value :: tail =>
        nextOption(map ++ Map('from -> value.toString), tail)
      case "--nDays" :: value :: tail =>
        nextOption(map ++ Map('nDays -> value.toString), tail)
      case "--country" :: value :: tail =>
        nextOption(map ++ Map('country -> value), tail)
    }
  }

  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val from = if (options.contains('from)) options('from).toString.toInt else 1
    val nDays =
      if (options.contains('nDays)) options('nDays).toString.toInt else 30
    val country: String =
      if (options.contains('country)) options('country).toString else "AR"

    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    getDataset(spark, country, nDays, from)

  }
}
