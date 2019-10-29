package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.ListBuffer

/**
  * The idea of this script is to get data for pv ingestion alert. 
  */
object pvAlertData {

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR LOADING DATA     //////////////////////
    *
    */
   /**
    * This method returns a DataFrame with the data from the "data_audiencies_streaming" pipeline, for the interval
    * of days specified. Basically, it loads every DataFrame for the days specified, and merges them as a single
    * DataFrame that will be returned. It returns pv events.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.  
    *
    * @return a DataFrame with the information coming from the read data. Columns: "id_partner","url"
   **/

    def getDataAudiences(
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
        val path = "/datascience/data_audiences_streaming"

        // Now we obtain the list of hdfs folders to be read
        val hdfs_files =
            days
              .flatMap(
                day =>
                  (0 until 24).map(
                    hour =>
                      path + "/hour=%s%02d/"
                        .format(day, hour)
                  )
              )
              .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

        val df = spark.read
        .option("basePath", path)
        .parquet(hdfs_files: _*)
        .filter("event_type = 'pv'")
        .select("id_partner","url")
        .na.drop()

        df
    }
  
/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR TRANSFORMING DATA     //////////////////////
    *
    */
/**
    * This function parses a URL getting the URL domain.
    */

    val domains = List("ac", "ad", "ae", "af", "ag", "ai", "al", "am", "an", "ao", "aq", "ar", "as", "asia", "at", "au", "aw", "ax", "az", "ba", "bb", "bd", "be", "bf", "bg", "bh", "bi", "biz", "bj", "bm", "bn", "bo", "br", "bs", "bt", "bv", "bw", "by", "bz", "ca", "cat", "cc", "cd", "cf", "cg", "ch", "ci", "ck", "cl", "cm", "cn", "co", "com", "coop", "cr", "cu", "cv", "cx", "cy", "cz", "de", "dj", "dk", "dm", "do", "dz", "ec", "edu", "ee", "eg", "er", "es", "et", "eu", "fi", "fj", "fk", "fm", "fo", "fr", "ga", "gb", "gd", "ge", "gf", "gg", "gh", "gi", "gl", "gm", "gn", "gob", "gov", "gp", "gq", "gr", "gs", "gt", "gu", "gw", "gy", "hk", "hm", "hn", "hr", "ht", "hu", "id", "ie", "il", "im", "in", "info", "int", "io", "iq", "ir", "is", "it", "je", "jm", "jo", "jobs", "jp", "ke", "kg", "kh", "ki", "km", "kn", "kp", "kr", "kw", "ky", "kz", "la", "lb", "lc", "li", "lk", "lr", "ls", "lt", "lu", "lv", "ly", "ma", "mc", "md", "me", "mg", "mh", "mil", "mk", "ml", "mm", "mn", "mo", "mobi", "mp", "mq", "mr", "ms", "mt", "mu", "mv", "mw", "mx", "my", "mz", "na", "nc", "ne", "net", "nf", "ng", "ni", "nl", "no", "np", "nr", "nu", "nz", "om", "org", "pa", "pe", "pf", "pg", "ph", "pk", "pl", "pm", "pn", "pr", "pro", "ps", "pt", "pw", "py", "qa", "re", "ro", "rs", "ru", "rw", "sa", "sb", "sc", "sd", "se", "sg", "sh", "si", "sj", "sk", "sl", "sm", "sn", "so", "sr", "st", "su", "sv", "sy", "sz", "tc", "td", "tel", "tf", "tg", "th", "tj", "tk", "tl", "tm", "tn", "to", "tp", "tr", "tt", "tv", "tw", "tz", "ua", "ug", "uk", "us", "uy", "uz", "va", "vc", "ve", "vg", "vi", "vn", "vu", "wf", "ws", "xxx", "ye", "yt", "za", "zm", "zw")

    def parseURL(url: String): String = {

      // First we obtain the query string and the URL divided in 
      val split = url.split("\\?")

      val fields = split(0).split('/')

      // Now we can get the URL path and the section with no path at all
      val path = (if (url.startsWith("http")) fields.slice(3, fields.length)
      else fields.slice(1, fields.length))
      val non_path = (if (url.startsWith("http")) fields(2) else fields(0)).split("\\:")(0)

      // From the non-path, we can get the extension, the domain, and the subdomain
      val parts = non_path.split("\\.").toList
      var extension: ListBuffer[String] = new ListBuffer[String]()
      var count = parts.length
      if (count > 0) {
        var part = parts(count - 1)
        // First we get the extension
        while (domains.contains(part) && count > 1) {
          extension += part
          count = count - 1
          part = parts(count - 1)
        }

      // Now we obtain the domain and subdomain.
      val domain = if (count > 0) parts(count - 1) else ""
 
      domain

      } else ("")
      }


    val udfGetDomain = udf(
        (url: String) =>
        parseURL(url)
      )

   /**
    * This method parses urls, obtaining url domains, then groups by url_domain and id_partner and counts ocurrences.
    *
    * @param df: DataFrame obtained from data_audiencies_streaming.
    *
    * @return a DataFrame with "url_domain", "id_partner", "count".
   **/

    def getFinalDF(
        df: DataFrame
    ): DataFrame = {
    
        val df_final = df
                    .withColumn("url_domain",udfGetDomain(col("url")))
                    .select("url_domain","id_partner")
                    .groupBy("url_domain","id_partner").count()
            
            df_final
        }


  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR SAVING DATA     //////////////////////
    *
    */
  /**
    * This method saves the data generated to /datascience/reports/alerts/pv, the filename is the current date.
    *
    * @param data: DataFrame that will be saved.
    *
  **/

  def saveData(
      data: DataFrame,
      since: Integer
  ) = {

    val dir = "/datascience/reports/alerts/pv/data/"
    val format = "yyyy-MM-dd"
    val date_current = DateTime.now.minusDays(since).toString(format)
    val fileNameFinal = dir + date_current

        data
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .save(fileNameFinal)
    }
  
/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     MAIN METHOD     //////////////////////
    *
    */
  /**
    * Given ndays and since, this file gives the number of ocurrences per domain per partner (for event type == 'pv').
    *
    * @param spark: Spark session that will be used to read the data from HDFS.
    * @param ndays: number of days to query.
    * @param since: number of days since to query.
    *
    * As a result this method stores the file in /datascience/reports/alerts/pv/currentdate
  **/
  
  def getDataReport(
      spark: SparkSession,
      nDays: Integer,
      since: Integer) = {
       
    /** Read from "data_audiences_streaming" database */
    val df = getDataAudiences(
      spark = spark,
      nDays = nDays,
      since = since)

    /** Transform df to its final form */
    val df_final = getFinalDF(df = df)  

    /** Store df*/
    saveData(
      data = df_final,
      since = since
    )
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
    val options = nextOption(Map(), Args.toList)
    val nDays = if (options.contains('nDays)) options('nDays) else 1
    val since = if (options.contains('since)) options('since) else 1

    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("pvAlert")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .getOrCreate()
    
     getDataReport(
       spark = spark,
       nDays = nDays,
       since = since)    
  }
}
