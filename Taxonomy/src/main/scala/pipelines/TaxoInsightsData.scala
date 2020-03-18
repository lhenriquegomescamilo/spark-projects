package main.scala.pipelines.TaxoInsights
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.log4j.{Level, Logger}


/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object TaxoInsightsData {


/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR LOADING DATA     //////////////////////
    *
    */
   /**
    * For loading data from data_demo/data_urls and data_triplets/segments
   **/   

  def getData(
      spark: SparkSession,
      nDays: Integer,
      since: String,
      path: String
  ): DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
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
      since: Integer
  ): DataFrame = {
    
    val map_events = Map(
          "batch" -> 0,
          "data" -> 1,
          "tk" -> 2,
          "pv" -> 3,
          "retroactive" -> 4)
    val mapUDF_events = udf((event_type: String) => map_events(event_type))

    // 1) Read Data URLS and transform
    val path_urls = "/datascience/data_demo/data_urls"
    val data_urls = getData(spark,nDays,since,path_urls)
    .select("device_id","url","id_partner","event_type","segments","country")
    .withColumn("segment", explode(col("segments")))
    .selectExpr("*", "parse_url(url, 'HOST') as domain")
    .withColumn("event_type",mapUDF_events(col("event_type"))) //este mapeo lo hago pero ya deberia estar hecho en data urls.
    .select("device_id","domain","id_partner","event_type","segment","country")

    // 2) Read Data Triplets and transform
    val path_triplets = "/datascience/data_triplets/segments"
    val data_triplets = getData(spark,nDays,since,path_triplets)
    .filter("activable== 1")
    .select("device_id","feature","id_partner","event_type","country")
    .withColumnRenamed("feature", "segment")
    .withColumn("domain",lit("not_url"))

    // 3) Concat dfs and aggregate devices
    val df = data_urls.union(data_triplets)
    .groupBy("domain","event_type","segment","id_partner","event_type","country")
    .agg(approx_count_distinct(col("device_id"), 0.02).as("devices_count"))

    // 4) Order and Save
    df.orderBy("segment") //preguntar si esta bien
      .write
      .format("parquet")
      .mode("append")
      .withColumn("day", lit(DateTime.now.minusDays(since)))
      .partitionBy("day", "country")
      .save("/datascience/taxo_insights/data")
      
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
      .appName("TaxoInsightsData")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    processDay(spark,nDays,since)

  }
}