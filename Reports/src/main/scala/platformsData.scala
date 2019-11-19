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

    val columns = "device_id,third_party,d2,d10,d11,d13,d14,country,device_type"
      .split(",")
      .toList
    val countries =
      "AR,BO,BR,CL,CO,CR,EC,GT,HN,MX,PE,PR,SV,US,UY,VE".split(",").toList
    val devUDF = udf(
      (dev_type: String) =>
        if (dev_type == "web") 0
        else if (dev_type == "android") 1
        else 2
    )

    val df = spark.read
      .option("sep", "\t")
      .option("header", "true")
      .format("csv")
      .load("/data/eventqueue/%s".format(date_current))
      .select(columns.head, columns.tail: _*) // Here we select the columns to work with
      .filter("event_type != 'sync'") // filter sync, internal event
      .filter(
        col("d2").isNotNull || col("d10").isNotNull || col("d11").isNotNull || col(
          "d13"
        ).isNotNull || col("d14").isNotNull && col("country")
          .isin(countries: _*)
      ) //get only relevant platforms
      .withColumn(
        "device_type",
        devUDF(col("device_type"))
      )

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
  val udfPlatform = udf(
    (d2: String, d10: String, d11: String, d13: String, d14: String) =>
      Map("d2" -> d2, "d10" -> d10, "d11" -> d11, "d13" -> d13, "d14" -> d14)
        .filter(t => t._2 != null && t._2.length > 0)
        .map(t => t._1)
        .toSeq
  )

  def transformDF(
      spark: SparkSession,
      data: DataFrame
  ): DataFrame = {
    def getIntRepresentation =
      udf(
        (array: Seq[Integer]) =>
          array.zipWithIndex
            .map(t => t._1 * Math.pow(2, t._2))
            .reduce((n1, n2) => n1 + n2)
            .toInt
      )
    // def getIntRepresentation =
    //   udf((array: Seq[Integer]) => array.map(_.toString).mkString(""))

    def getAllPlatforms =
      udf((array: Seq[Integer]) => array.reduce((i1, i2) => i1 | i2).toInt)

    val df = data
      .withColumn("d2", when(col("d2").isNotNull, 1).otherwise(0))
      .withColumn("d10", when(col("d10").isNotNull, 1).otherwise(0))
      .withColumn("d11", when(col("d11").isNotNull, 1).otherwise(0))
      .withColumn("d13", when(col("d13").isNotNull, 1).otherwise(0))
      .withColumn("d14", when(col("d14").isNotNull, 1).otherwise(0))
      .withColumn(
        "platforms",
        array(col("d2"), col("d10"), col("d11"), col("d13"), col("d14"))
      )
      .withColumn("platforms", getIntRepresentation(col("platforms")))
      .withColumn("segments", split(col("third_party"), "\u0001"))
      .withColumn("segments", col("segments").cast("array<int>"))
      .select("device_id", "segments", "platforms", "country", "device_type")

    df.write
      .format("parquet")
      .mode("overwrite")
      .save("/datascience/reports/platforms/tmp/")

    val temp_data =
      spark.read.format("parquet").load("/datascience/reports/platforms/tmp/")

    val users = temp_data
      .groupBy("device_id")
      .agg(collect_list(col("platforms")) as "platforms")
      .withColumn("platforms", getAllPlatforms(col("platforms")))
      .select("device_id", "platforms")

    val segments = temp_data
      .select("device_id", "segments", "platforms", "country", "device_type")
      .withColumn("segment", explode(col("segments")))
      .select("device_id", "segment", "country", "device_type")
      .distinct() //, "platforms")
    // .dropDuplicates("device_id", "segment")

    val joint = users.join(segments, Seq("device_id"), "inner")
    // .orderBy("segment")

    // segments
    joint
  }

 /**
    * This method gets volumes for taxonomy segments.
    * It groups platformsData by country, segment and device_type and counts devices.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param df: DataFrame obtained from platforms/data.
    *
    * @return a DataFrame with volumes for each country, segment, device_type tuple.
   **/

  def getDailyVolumes(
      spark: SparkSession,
      date_current: String
  ): DataFrame = {

    val day = date_current.replace("/", "")
    val base_path = "/datascience/reports/platforms/data"
    val path = base_path + "/day=" + day

    val df = spark.read
      .option("basePath", path)
      .parquet(path)
      .select("device_id", "platforms", "segment", "device_type", "country")
      .groupBy("country", "segment","device_type")
      .count()
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
      dir: String,
      date_current: String
  ) = {

    val fileNameFinal = dir + date_current

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
  def getDataPlatforms(spark: SparkSession, since: Integer) = {

    /**Get current date */
    val format = "yyyy/MM/dd/"
    val date_current = DateTime.now.minusDays(since).toString(format)
    println("STREAMING LOGGER:\n\tDay: %s".format(date_current))

    /** Read from "eventqueue" database */
    val data = getDayEventQueue(spark = spark, date_current = date_current)

    /**  Transform data */
    val df = transformDF(spark, data = data)

    /** Store df */
    val dir1 = "/datascience/reports/platforms/data/"
    saveData(df = df, dir = dir1, date_current = date_current)

    /** Volumes Report */
    val df_volumes = getDailyVolumes(spark, date_current = date_current)

    /** Store df */
    val dir2 = "/datascience/reports/volumes/data/"
    saveData(df = df_volumes, dir = dir2, date_current = date_current)

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
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    getDataPlatforms(spark = spark, since = since)

  }
}
