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
object platformsReport {

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
  def getPlatformsData(
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
    val path = "/datascience/reports/platforms/data"

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
        "platforms",
        "segment",
        "device_type",
        "country",
        "day"
      )

    df
  }

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR TRANSFORMING DATA     //////////////////////
    *
    */
  /**
    * This method transform data from platforms data, obtaining volumes per platform.
    * Returns a dataframe.
    *
    * @param data: DataFrame obtained from reading platforms data.
    *
    * @return a DataFrame.
   **/
  def getVolumesPlatform(
      spark: SparkSession,
      data: DataFrame
  ): DataFrame = {

    import spark.implicits._

    println("INFO: GETTING VOLUMES")

    val getVec =
          udf(
            (n: String) =>
            (n.map(lines=>(lines+"").toInt))
            )

    val mapping_path = "/datascience/misc/mappingpl_2.csv"
    val mapping = spark.read
      .format("csv")
      .option("header", "true")
      .load(mapping_path)
      .withColumn("platforms", getVec(col("platforms")))
      .select(col("decimal").cast("int"), col("platforms"))
      .as[(Int, List[Int])]
      .collect
      .toMap

    val mapping_b = spark.sparkContext.broadcast(mapping)

    def udfMap = udf((n: Int) => (mapping_b.value.get(n)))

    println("INFO: MAPPING OBTAINED AND BROADCASTED")

    val df = data
      .withColumn("platforms", udfMap(col("platforms")))
      .withColumn("d2", col("platforms")(0))
      .withColumn("d10", col("platforms")(1))
      .withColumn("d11", col("platforms")(2))
      .withColumn("d13", col("platforms")(3))
      .withColumn("d14", col("platforms")(4))
      .dropDuplicates("device_id", "segment")
      .groupBy("segment", "country", "device_type")
      .agg(
        sum("d2") as "d2",
        sum("d10") as "d10",
        sum("d11") as "d11",
        sum("d13") as "d13",
        sum("d14") as "d14"
      )
    //.withColumn("platform", explode(col("platforms")))
    //.select("platform", "segment", "device_id")
    //.distinct()
    //.groupBy("platform", "segment")
    //.count()
    //.select("platform", "segment", "count")
    df.explain()
    df
  }

  /**
    * This method is a filter that keeps only general taxonomy segments.
    * It joins "general taxonomy segment values" from metabase with current dataframe.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param df: DataFrame obtained from platforms/data.
    *
    * @return a DataFrame filtered by general taxo segments.
   **/
  def filter_df(
      spark: SparkSession,
      db: DataFrame
  ): DataFrame = {

    /** Read standard taxonomy segment_ids */  
    val countries = "AR,BO,BR,CL,CO,CR,EC,GT,HN,MX,PE,PR,SV,US,UY,VE".split(",").toList
    val taxo_path = "/datascience/misc/taxo_gral.csv"
    val taxo_segs = spark.read
      .format("csv")
      .option("header", "true")
      .load(taxo_path)
      .select("seg_id")
      .collect()
      .map(_(0))
      .toList

    val df = db
      .filter(
        col("country").isin(countries: _*) && col("segment").isin(taxo_segs: _*)
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
  def getReports(
      spark: SparkSession,
      nDays: Integer,
      since: Integer
  ) = {

    /**Get current date */
    val format = "yyyyMMdd"
    val date_current = DateTime.now.minusDays(since).toString(format)
    println("INFO:\n\tDay: %s".format(date_current))

    /** Read from "platforms/data" */
    val db = getPlatformsData(spark = spark, nDays = nDays, since = since)

    /** Keep general taxonomy segments */
    val data = filter_df(spark, db = db)

    /**  Transform data for Platforms Report */
    val df = getVolumesPlatform(spark, data = data)
      .withColumn("day", lit(date_current))

    /** Store df */
    val dir = "/datascience/reports/platforms/"
    val path = dir + "done"

    saveData(df = df, path = path)

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
      .appName("PlatformsReportFast")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    getReports(spark = spark, nDays = nDays, since = since)

  }
}
