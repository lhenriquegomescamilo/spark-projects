package main.scala.keywiser
import main.scala.crossdevicer.AudienceCrossDevicer

import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.joda.time.{Days, DateTime}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.ListBuffer

/**
  * The idea of this script is to generate audiences based on keywords obtained from url content. 
  */
object TermSearch {

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
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR LOADING DATA     //////////////////////
    *
    */

  def getSelectedKeywordsOLD(
      spark: SparkSession,
      nDays: Integer,
      since: Integer
  ): DataFrame = {


    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val format = "yyyy-MM-dd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/selected_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/%s.csv".format(day)) //for each day from the list it returns the day path.  
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val df = spark.read.format("csv")
      .option("header",true)
      .load(hdfs_files: _*)

      .withColumnRenamed("url_raw","url")
      .select("url","kw")
      .withColumn("kw", split(col("kw"), " "))
      .withColumn("kw", explode(col("kw")))     

    df  
  }


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
      //.filter("share_data = 1")
      .select("device_id","url", "segments", "country", "day")
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
    
    // make df from list of search terms
    val keywords = "coronavirus,covid,covid-19,covid19,cov19,cov-19,pandemia,contagio,contagiarse,respiradores,barbijo,infectados"
    val trimmedList: List[String] = keywords.split(",").map(_.trim).toList
    val df_keys = trimmedList.toDF().withColumnRenamed("value", "kw")

    // get data from selected keywords
    //val df_selected = getSelectedKeywords(spark,10,1) //al 24/03
    //val df_selected = getSelectedKeywords(spark,10,10) 
    //val df_selected = getSelectedKeywordsOLD(spark,10,38)
   
    val df_selected1 = getSelectedKeywordsOLD(spark,12,25)
    val df_selected2 = getSelectedKeywords(spark,25,0)
    val df_selected = df_selected1.union(df_selected2)

    // get data urls containing the aforementioned search terms
    val df_urls_terms_skws = getUrlsWithTerms(df_keys,df_selected)

    val country = "BR"
    val filename = "march_%s_filtered".format(country)

    //add more urls from druid
    val df_urls_druid =spark.read.format("csv")
    .option("header",true)
    .load("/datascience/misc/urls_coronadruid_%s_march.csv".format(country)) 
    .select("url")
    .withColumn("search_terms", lit(1)) 


    //filter by these domains
    val domains_string = "eluniversal,cronista,unotv,perfil,excelsior,ambito,minutouno,lavoz,elespectador,las2orillas,record,elheraldo,t13,13,losandes,larepublica,eldia,revistaforum,latina,semana,lacapital,elhorizonte,elsol,diariodecuyo,quepasasalta,diariodocentrodomundo,lared"
    val domains: List[String] = domains_string.split(",").map(_.trim).toList 

    val query_domains = domains
      .map(dom => "domain LIKE '%" + dom + "%'")
      .mkString(" OR ")

    // concat both previous url sources
    val df_urls_terms = df_urls_terms_skws.union(df_urls_druid)
    .withColumn("domain", udfGetDomain(col("url")))
    .filter(query_domains)
    

    // get data from data urls
    //val data_urls = getDataUrls(spark,"AR",7,1)  //al 24/03   
    //val data_urls = getDataUrls(spark,"AR",7,10)
    //val data_urls = getDataUrls(spark,"AR",7,38)
    val data_urls = getDataUrls(spark,country,25,0)

    // get final df
    val df_final = getUsers(df_urls_terms,data_urls)

    val path = "/datascience/misc/covid_users_%s".format(filename)
    df_final.write
      .format("parquet")
      .partitionBy("day")
      .mode(SaveMode.Overwrite)
      .save(path)

    // part 2
    val df =spark.read.format("parquet")
    .load(path)

    //write and reload:
    val path2 = "/datascience/misc/covid_users_flag_%s".format(filename)
    df.groupBy("device_id","day","search_terms").agg(approx_count_distinct(col("url"), 0.02).as("url_count"))
      .write.format("csv")
      .option("header",true)
      .mode(SaveMode.Overwrite)
      .save(path2)

    val db = spark.read.format("csv")
    .option("header",true)
    .load(path2)

    val path3 = "/datascience/misc/covid_users_count_total_%s".format(filename)    
    db.groupBy("device_id","day").agg(sum(col("url_count")).as("url_count_total"))
      .select("device_id","day","url_count_total")
      .write.format("csv")
      .option("header",true)
      .mode(SaveMode.Overwrite)
      .save(path3)

    val count_total =  spark.read.format("csv")
    .option("header",true)
    .load(path3)

    val df_joint = db.join(count_total,Seq("device_id","day"))
    .filter("search_terms==1")
    .withColumn("ratio",col("url_count")/col("url_count_total"))
    //.select("device_id","day","url_count","url_count_total","ratio")
    .select("device_id","day","url_count_total","ratio")

    val path4 = "/datascience/misc/covid_final_%s".format(filename)    
    df_joint.dropDuplicates("device_id","day") // ESTE DROP ELIMINA SI HUBIESE DUPLICADOS POR DIA.
    .write.format("csv")
      .option("header",true)
      .mode(SaveMode.Overwrite)
      .save(path4)

/**

    // part 3  
    val taxo = spark.read.format("csv")
    .option("header",true)
    .load("/datascience/geo/Reports/Equifax/DataMixta/RelyTaxonomy_06_02_2020.csv")
    .filter(col("clusterParent").isin(0,1,2))
    .select("segmentId","name")

    val df2 =spark.read.format("parquet")
    .option("header",true)
    .load(path)
    .withColumn("domain", udfGetDomain(col("url")))
    .withColumn("segments", split(col("segments"), ","))
    .withColumn("segmentId", explode(col("segments")))
    .join(taxo,Seq("segmentId"))
    .groupBy("domain","device_id","day").agg(collect_list("name").as("behaviour"))
    .withColumn("behaviour", concat_ws(",", col("behaviour")))
    .groupBy("device_id","day","behaviour").agg(collect_list("domain").as("domains")) //new
    .withColumn("domains", concat_ws(",", col("domains")))

    val df3 =spark.read.format("csv")
    .option("header",true)
    .load(path4)
  
    df2.join(df3,Seq("device_id","day"))
    .select("device_id","domains","day","url_count_total","ratio","behaviour")
    .write.format("csv")
      .option("header",true)
      .mode(SaveMode.Overwrite)
      .save("/datascience/misc/covid_end_%s".format(filename))
      //.save("/datascience/misc/covid_final_2_10") //.save("/datascience/misc/covid_final_2")

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