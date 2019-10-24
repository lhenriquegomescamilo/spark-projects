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

/**
  * The idea of this script is to get data for pv ingestion alert. 
  */
object pvAlert {

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
        val hdfs_files = days
        .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
        .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

        val df = spark.read
        .option("basePath", path)
        .parquet(hdfs_files: _*)
        .select("id_partner","url")

        df
    }

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR TRANSFORMING DATA     //////////////////////
    *
    */
   /**
    * This method parses urls, obtaining url domains, then groups by url_domain and id_partner and counts ocurrences.
    *
    * @param df: DataFrame obtained from data_audiencies_streaming.
    *
    * @return a DataFrame with "url_domain", "id_partner", "count".
   **/

    val udfCleanUrl = udf(
        (url: String) =>
        if(url.startsWith("m.")) url.substring(2)
        else if (url.startsWith("www.")) url.substring(4)
        else url
        )

    def getFinalDF(
        df: DataFrame
    ): DataFrame = {

        import spark.implicits._
    
        val df_final = df
                    .withColumn("url_domain", callUDF("parse_url", $"url", lit("HOST")))
                    .withColumn("url_domain",udfCleanUrl(col("url_domain")))
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
      data: DataFrame
  ) = {

    val dir = "/datascience/reports/alerts/pv/"
    val format = "yyyy-MM-dd'T'HH-m"
    val date_current = DateTime.now.toString(format)
    val fileName = "pvData"
    val fileNameFinal = dir + fileName + "_" + date_current

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
    val df_final = getFinalDF(
      df = df)  

    /** Store df*/
    saveData(
      data = df_final
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
    val since = if (options.contains('from)) options('from) else 1

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
