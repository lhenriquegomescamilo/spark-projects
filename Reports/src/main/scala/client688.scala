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
object client688 {

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

    val columns = "device_id,ip,country,segments,first_party,timestamp".split(",").toList


   val path = "/data/eventqueue/%s".format(date_current)
     
   val df = spark.read.option("sep", "\t")
            .option("header", "true")
            .format("csv")
            .load(path)
            .filter("id_partner = 688")
            .select(columns.head, columns.tail: _*) // Here we select the columns to work with      

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


  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR SAVING DATA     //////////////////////
    *
    */
  /**
    * This method saves the data generated to /datascience/reports/platforms/data/, the filename is the current date.
    *
    * @param data: DataFrame that will be saved.
    *
  **/
  def saveData(
      df: DataFrame,
      date_current: String
  ) = {

    val dir = "/datascience/reports/custom/client_688/"

    df.withColumn("day", lit(date_current.replace("/", "")))
      .write
      .format("parquet")
      .partitionBy("day", "country")
      .mode(SaveMode.Overwrite)
      .save(dir)
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
    * As a result this method stores the file in /datascience/reports/platforms/data/day=yyyyMMdd
  **/
  def getClient688(spark: SparkSession, since: Integer) = {

    /**Get current date */
    val date_start = new DateTime(2019,11,18,0,0,0,0) //hardcoded since100 from 26feb
    val format = "yyyy/MM/dd/"
    val date_current = date_start.minusDays(since).toString(format)
    println("STREAMING LOGGER:\n\tDay: %s".format(date_current))

    /** Read from "eventqueue" database */
    val df = getDayEventQueue(spark, date_current)

    /** Store df */
    saveData(df, date_current)

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
      .appName("client688")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    getClient688(spark, since)

  }
}
