package main.scala
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.joda.time.{Days, DateTime}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.log4j.{Level, Logger}

/**
  * The idea of this script is to generate days of volumes by platform for platform Report. 
  */
object platformsData {

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR LOADING DATA     //////////////////////
    *
    */
   /**
    * This method returns a DataFrame with the data from the "eventqueue" pipeline, for the day specified.
    * A DataFrame that will be returned. 
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.  
    *
    * @return a DataFrame with the information coming from the read data. Columns: "device_id","third_party" and several platforms.
   **/

  def getDayEventQueue(
      spark: SparkSession,
      date_current: String
  ): DataFrame = {

    val columns = "device_id,third_party,d2,d10,d11,d13,d14".split(",").toList
    
    val df = spark.read
        .option("sep", "\t")
        .option("header", "true")
        .format("csv")
        .load("/data/eventqueue/%s".format(date_current))
        .select(columns.head, columns.tail: _*) // Here we select the columns to work with
        .filter("event_type != 'sync'") // filter sync, internal event
        .filter(col("d2").isNotNull || col("d10").isNotNull || col("d11").isNotNull || col("d13").isNotNull || col("d14").isNotNull) //get only relevant platforms

    df
  }

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR TRANSFORMING DATA     //////////////////////
    *
    */
   /**
    * This method transform data from the eventqueue, getting the unique users per platform per segment.
    * Returns a dataframe.
    *
    * @param data: DataFrame obtained from reading eventqueue.
    *
    * @return a DataFrame with "platform", "segment", "user_unique".
   **/

  //udf to process platform columns
  val udfPlatform = udf( (d2: String, d10: String, d11: String, d13: String, d14: String)
                        => Map("d2" -> d2, "d10" -> d10, "d11" -> d11, "d13" -> d13, "d14" -> d14)
                        .filter(t => t._2 != null && t._2.length>0)
                        .map(t => t._1).toSeq ) 

  def transformDF(
      data: DataFrame
  ): DataFrame = {

    val df = data
        //.withColumn("platforms", udfPlatform(col("d2"), col("d10"), col("d11"), col("d13"), col("d14")))
        .withColumn("platforms", array(col("d2"), col("d10"), col("d11"), col("d13"), col("d14")))
        .withColumn("platform", explode(col("platforms")))
        .filter("platform IS NOT NULL")
        .withColumn("segments", split(col("third_party"), "\u0001"))
        .withColumn("segment", explode(col("segments")))
        .select("device_id","segment","platform")
        .groupBy("platform", "segment").agg(countDistinct("device_id") as "user_unique")
    df
  }


  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR SAVING DATA     //////////////////////
    *
    */
  /**
    * This method saves the data generated to /datascience/reports/gain/, the filename is the current date.
    *
    * @param data: DataFrame that will be saved.
    *
  **/

  def saveData(
      df: DataFrame,
      date_current: String
  ) = {

  val dir = "/datascience/reports/platforms/data/"
  val fileNameFinal = dir + date_current

    df
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
    * Given ndays, since and a filename, this file gives the number of devices per partner per segment.
    *
    * @param spark: Spark session that will be used to read the data from HDFS.
    * @param ndays: number of days to query.
    * @param since: number of days since to query.
    *
    * As a result this method stores the file in /datascience/reports/gain/file_name_currentdate.csv.
  **/
 
  def getDataPlatforms(
      spark: SparkSession,
      since: Integer) = {
       
    /**Get current date */
    val format = "yyyy/MM/dd/"
    val date_current = DateTime.now.minusDays(since).toString(format)
    println("STREAMING LOGGER:\n\tDay: %s".format(date_current))
   
    /** Read from "eventqueue" database */
    val data = getDayEventQueue(spark = spark,
                              date_current = date_current)

    /**  Transform data */
    val df = transformDF(data = data)

    /** Store df */
    saveData(df = df,
             date_current = date_current)
  
  }    

  type OptionMap = Map[Symbol, Int]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
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
    val since = if (options.contains('since)) options('since) else 1

    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("PlatformsData")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .getOrCreate()
    
     getDataPlatforms(
       spark = spark,
       since = since)
    
  }
}
