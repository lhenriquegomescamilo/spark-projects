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
object RemovedSegmentsReport {

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR LOADING DATA     //////////////////////
    *
    */
  /**
    * This method returns a DataFrame with the data from the "platforms data" pipeline, for the day specified.
    * A DataFrame that will be returned.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.
    *
    * @return a DataFrame with the information coming from the read data.
   **/
  def getRemovedSegmentsData(
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
    val path = "/datascience/reports/removed_segments/data"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    println("INFO: FILES TO BE READ")
    hdfs_files.foreach(println)

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select(
        "device_id",
        "removed_segments",
        "country",
        "day"
      )

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
      path: String
  ) = {

    println("INFO: SAVING THE DATA")
    df.write
      .format("parquet")
      .partitionBy("day")
      .mode(SaveMode.Overwrite)
      .save(path)
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

  def getRemovedSegmentsReport(spark: SparkSession,
                       nDays: Integer,
                       since: Integer) = {

    import spark.implicits._

    val df =  getRemovedSegmentsData(spark = spark,
                                     nDays = nDays,
                                     since = since)

    val taxo_segs = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/misc/taxo_gral.csv")
      .select("seg_id")
      .collect()
      .map(_(0).toString.toInt)
      .toSeq

    val volumes = df
      .withColumn("removed_segments", split(col("removed_segments"), "\u0001"))    
      .withColumn("removed_segment", explode(col("removed_segments")))
      .drop("removed_segments")
      .filter(col("removed_segment").isin(taxo_segs: _*))
      .groupBy("removed_segment", "country")
      .agg(
        approx_count_distinct(col("device_id"), rsd = 0.03) as "device_unique_removed"
      )

    volumes
      .withColumn("day", lit(DateTime.now.minusDays(since).toString("yyyyMMdd")))
      .write
      .format("parquet")
      .partitionBy("day", "country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/reports/removed_segments/done")    
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
    val nDays = if (options.contains('nDays)) options('nDays) else 30
    val since = if (options.contains('since)) options('since) else 1

    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("RemovedSegmentsReport")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    getRemovedSegmentsReport(spark = spark,
                             nDays = nDays,
                             since = since)


  }
}
