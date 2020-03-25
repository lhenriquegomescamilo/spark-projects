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
object Wordcloud {

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

  def udfZip = udf((words: Seq[String], tfidf: Seq[String]) => words zip tfidf)
  def udfGet = udf((words: Row, index:String ) => words.getAs[String](index))
  def udfCast = udf((tfidf: String) => tfidf.toFloat)

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

    val df = spark.read.format("csv")
    .option("basePath", path)
    .load(hdfs_files: _*)
    .toDF("url_raw","hits","country","kw","TFIDF","domain","stem_kw","day")
    .withColumnRenamed("url_raw","url")
    .withColumn("kw", split(col("kw"), " "))
    .withColumn("TFIDF", split(col("TFIDF"), " "))
    .withColumn("zipped",udfZip(col("kw"),col("TFIDF")))
    .withColumn("zipped", explode(col("zipped")))
    .withColumn("kw",udfGet(col("zipped"),lit("_1")))
    .withColumn("TFIDF",udfGet(col("zipped"),lit("_2")))
    .withColumn("TFIDF",udfCast(col("TFIDF")))
    .filter("TFIDF>0")
    .select("day","kw","TFIDF")

    df
  }

 
/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     MAIN METHOD     //////////////////////
    *
    */

  def MainProcess(
      spark: SparkSession) = {
      
    import spark.implicits._
    
    // get data from selected keywords
    val df = getSelectedKeywords(spark,10,1) 
    
    val path = "/datascience/misc/covid_wc"
    df.write.format("csv")
      .option("header",true)
      .mode(SaveMode.Overwrite)
      .save(path)   

    val db = spark.read.format("csv")
    .option("header",true)
    .load(path)  

    // number of documents where keyword appears (per day)
    val db_DF = db.withColumn("flag", lit(1))
    .groupBy("kw","day")
    .agg(sum(col("flag")).as("DF"))
    .filter("DF>1000")

    val path2 = "/datascience/misc/covid_wc_df"
    db_DF.write.format("csv")
      .option("header",true)
      .mode(SaveMode.Overwrite)
      .save(path2)

    val db_DF_y = spark.read.format("csv")
    .option("header",true)
    .load(path2)       

    // sum of tfidf per keyword per day, in order to calculate mean 
    val db_sum = db.groupBy("kw","day")
    .agg(sum(col("TFIDF")).as("TFIDF_sum"))

    val path3 = "/datascience/misc/covid_wc_sum"
    db_sum.write.format("csv")
      .option("header",true)
      .mode(SaveMode.Overwrite)
      .save(path3)

    val db_sum_y = spark.read.format("csv")
    .option("header",true)
    .load(path3)           

    val dg = db_DF_y.join(db_sum_y,Seq("kw","day"))
    .withColumn("score",col("TFIDF_sum")/col("DF"))

    val path4 = "/datascience/misc/covid_wc_final"
    dg.write.format("csv")
      .option("header",true)
      .mode(SaveMode.Overwrite)
      .save(path4)
    
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
      .appName("Wordcloud")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    MainProcess(spark)
  

  }

}