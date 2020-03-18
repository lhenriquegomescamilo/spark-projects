package main.scala.pipelines.TaxoInsights
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.joda.time.{Days, DateTime}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.log4j.{Level, Logger}


/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object TaxoInsightsAgg {


/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR LOADING DATA     //////////////////////
    *
    */
   /**
    * For loading data from data_demo/data_urls and data_triplets/segments
   **/   

  def getDataTaxo(
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

    // Now we obtain the list of hdfs folders to be read
    val path = "/datascience/taxo_insights/data"
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)

    df
  }

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\    MAIN METHOD    //////////////////////
    *
    */
   /**
    * This Method processes given day (previous day) for pipeline.
   **/          

def processDay(
      spark: SparkSession,
      nDays: Integer,
      since: Integer){

    val date_current = DateTime.now.minusDays(since).toString("yyyyMMdd")

    // 1) Read Data and sum counts
    val df = getDataTaxo(spark,nDays,since)
    .groupBy("domain","event_type","segment","id_partner","event_type","country")
    .agg(sum(col("devices_count"), 0.02).as("total_devices"))

    // 2) Order and Save
    df.orderBy("segment") //preguntar si esta bien
      .withColumn("day", lit(date_current))
      .write
      .format("parquet")
      .mode("append")
      .partitionBy("day", "country")
      .save("/datascience/taxo_insights/agg")
      
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
      .appName("TaxoInsightsAgg")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    processDay(spark,nDays,since)

  }
}