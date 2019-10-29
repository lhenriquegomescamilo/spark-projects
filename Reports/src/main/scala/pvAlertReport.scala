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
object pvAlertReport {

  /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR LOADING DATA     //////////////////////
    *
    */
   /**
    * This method returns a DataFrame with the data from the "pvAlertData" pipeline, for the interval
    * of days specified. Basically, it loads every DataFrame for the days specified, and merges them as a single
    * DataFrame that will be returned. It returns pv events for each id_partner and each url_domain, calculating average count.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.  
    * @param median_thr: threshold to consider a domain relevant for a partner. Default 1000.  
    *
    * @return a DataFrame with the information coming from the read data. Columns: "id_partner","domain","average"
   **/

    def getDataPV_average(
        spark: SparkSession,
        nDays: Integer,
        since: Integer,
        median_thr: Integer
    ): DataFrame = {

        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(conf)
        
        val sc = spark.sparkContext
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._

        // Get the days to be loaded.
        val from = since + 1    //+1 because it loads nDays before yesterday
        val format = "yyyy-MM-dd"
        val end = DateTime.now.minusDays(from)
        val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
        val path = "/datascience/reports/alerts/pv/data"

        // Now we obtain the list of hdfs folders to be read
        val hdfs_files = days.map(day => path + "/%s/".format(day))
            .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

        val df = spark.read
            .option("basePath", path)
            .parquet(hdfs_files: _*)
            .groupBy("id_partner", "url_domain")
        df.createOrReplaceTempView("df")
        val df_median = spark.sql("select url_domain,id_partner, percentile_approx(count, 0.5) as median from df group by url_domain,id_partner")
                        .filter(col("median") >= lit(median_thr))

        df_median
    }

    /**
    * This method returns a DataFrame with the data from the "pvAlertData" pipeline for the current day, specified with the since parameter.
    * It returns pv events for each id_partner and each url_domain, calculating average count.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.  
    * @param low_thr: threshold to consider a domain a movite for alert. Default 800.
    *
    * @return a DataFrame with the information coming from the read data. Columns: "id_partner","domain","average"
   **/   

   def getDataPV_current(
        spark: SparkSession,
        since: Integer,
        low_thr: Integer
    ): DataFrame = {

        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(conf)

        // Get the day to be loaded (current)
        val nDays = 1
        val format = "yyyy-MM-dd"
        val end = DateTime.now.minusDays(since)
        val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
        val path = "/datascience/reports/alerts/pv/data"

        // Now we obtain the list of hdfs folders to be read
        val hdfs_files = days.map(day => path + "/%s/".format(day))
            .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

        val df = spark.read
        .option("basePath", path)
        .parquet(hdfs_files: _*)
        .filter(col("count") < lit(low_thr))

        df
    }    

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR MERGING DATA     //////////////////////
    *
    */
   /**
    * This method merges data from current day to alert (asked with since) with data obtained from previous nDays.
    *
    * @param df_median: DataFrame with median count for domains for id_partner, of nDays.
    * @param df_current: DataFrame with count from current day.
    *
    * @return a DataFrame with alerted partners. Columns "url_domain", "id_partner", "count","average".
   **/

    def getJoint(
        df_current: DataFrame,
        df_median: DataFrame        
    ): DataFrame = {
   
        val df = df_current.join(df_median,Seq("id_partner","url_domain"),"left")
                 .na.drop()
                 
            df
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
      data: DataFrame,
      since: Integer
  ) = {

    val dir = "/datascience/reports/alerts/pv/alerted/"
    val format = "yyyy-MM-dd"
    val date_current = DateTime.now.minusDays(since).toString(format)
    val fileNameFinal = dir + date_current

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
    * Given ndays and since, this file gives alerted domains for any relevant partner.
    *
    * @param spark: Spark session that will be used to read the data from HDFS.
    * @param ndays: number of days to query and calculate median of counts for each domain for each partner.
    * @param since: number of days since to query. (ndays will be "since + 1" days)
    *
    * As a result this method stores the file in /datascience/reports/alerts/pv_alerted
  **/
  
  def getAlertedReport(
      spark: SparkSession,
      nDays: Integer,
      since: Integer,
      median_thr: Integer,
      low_thr: Integer
      ) = {
       
    /** Read from "pvData_pipeline" database */
    val df_median = getDataPV_average(
        spark = spark,
        nDays = nDays,
        since = since,
        median_thr = median_thr)

    val df_current = getDataPV_current(
        spark = spark,
        since = since,
        low_thr = low_thr)

    /** Transform df to its final form */
    val df = getJoint(
      df_current = df_current,
      df_median= df_median)  

    /** Store df*/
    saveData(
      data = df,
      since = since)

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
      case "--median_thr" :: value :: tail =>
        nextOption(map ++ Map('median_thr -> value.toInt), tail)
      case "--low_thr" :: value :: tail =>
        nextOption(map ++ Map('low_thr -> value.toInt), tail)    
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
    val median_thr = if (options.contains('median_thr)) options('median_thr) else 1500
    val low_thr = if (options.contains('low_thr)) options('low_thr) else 800

    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("pvAlert")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .getOrCreate()
    
     getAlertedReport(
       spark = spark,
       nDays = nDays,
       since = since,
       median_thr = median_thr,
       low_thr = low_thr)    
  }
}
