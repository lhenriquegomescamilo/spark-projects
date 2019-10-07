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
  * The idea of this script is to generate reports. 
  */
object Reports {

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR LOADING DATA     //////////////////////
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

  def getDataTriplets(
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
    val path = "/datascience/data_triplets/segments_"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select("id_partner","seg_id","device_id")
      .withColumnRenamed("feature", "seg_id")
      .na
      .drop()

    df
  }

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR MERGING DATA     //////////////////////
    *
    */
   /**
    * This method joins "general taxonomy segment values" with data from the "data_triplets" pipeline,
    * obtaining "id_partner","seg_id" and "device_id" values for a given day for general taxo segments.
    * Then it drops duplicates and after that it groups by "device_id" and "id_partner" and counts,
    * obtaining the number of devices per partner per segment.
    *
    * @param df_keys: DataFrame obtained from json queries.
    * @param df_data_keywords: DataFrame obtained from getDataTriplets().
    *
    * @return a DataFrame with "device_type", "device_id", "kws", being "kws" a list of keywords.
   **/

  def getJointandGrouped(
      df_taxo: DataFrame,
      df_data_triplets: DataFrame
  ): DataFrame = {

    val df_joint = df_data_triplets
      .join(broadcast(df_taxo), Seq("seg_id"))
      .select("seg_id","id_partner", "device_id")
      .dropDuplicates()

    val df_grouped = df_joint
      .groupBy("id_partner", "seg_id")
      .count()
    df_grouped
  }

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR QUERYING DATA     //////////////////////
    *
    */
  /**
    * This method takes a list of queries and their corresponding segment ids, and generates a file where the first
    * column is the device_type, the second column is the device_id, and the last column is the list of segment ids
    * for that user separated by comma. Every column is separated by a space. The file is stored in the folder
    * /datascience/keywiser/processed/file_name. The file_name value is extracted from the file path given by parameter.
    * In other words, this method appends a file per query (for each segment), containing users that matched the query
    * then it groups segments by device_id, obtaining a list of segments for each device.
    *
    * @param spark: Spark session that will be used to write results to HDFS.
    * @param queries: List of Maps, where the key is the parameter and the values are the values.
    * @param data: DataFrame that will be used to extract the audience from, applying the corresponding filters.
    * @param file_name: File where we will store all the audiences.
    *
    * As a result this method stores the audience in the file /datascience/keywiser/processed/file_name, where
    * the file_name is extracted from the file path.
  **/

  def saveData(
      data: DataFrame,
      file_name: String
  ) = {

    data.cache()

    val fileName = "/datascience/reports/gain/" + file_name
    val format = "yyyy_MM_dd"
    val date_current = DateTime.now.toString(format)
    val fileDate = fileName + "_" + date_current + ".csv"

     data
      .write
      .format("csv")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save(fileDate)
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
    * @param file_name: file name to save file with.
    *
    * As a result this method stores the file in /datascience/reports/gain/file_name_currentdate.csv.
  **/
  def getDataReport(
      spark: SparkSession,
      ndays: String,
      since: String,
      file_name: String) = {

    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)
  
    // Flag to indicate if execution failed
    var failed = false      

    try {        
      /** Read from "data_triplets" database */
      val df_data_triplets = getDataTriplets(
        spark = spark,
        nDays = nDays,
        since = since
      )

      /** Read standard taxonomy segment_ids */
      val taxo_path = "/datascience/misc/standard_ids.csv"
      df_taxo =  spark.read.format("csv").option("header", "true").load(taxo_path)

      /**  Get number of devices per partner_id per segment */
      val data = getJointandGrouped(
        df_taxo = df_taxo,
        df_data_triplets = df_data_triplets)  
    
      // Here we store the audience applying the filters
      saveData(
        data = data,
        file_name = file_name
      )
  
    } catch {
      case e: Exception => {
        e.printStackTrace()
        failed = true
      }
    }        

  }


  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("Reports")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .getOrCreate()

    val file_name =  "test"

    getDataReport(
      spark = spark,
      ndays = 1,
      since = 1,
      file_name = file_name)
  }
}
