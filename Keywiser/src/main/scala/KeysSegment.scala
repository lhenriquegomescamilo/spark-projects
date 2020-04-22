package main.scala

import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.joda.time.{Days, DateTime}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.ListBuffer

import scala.math.log

/**
  * The idea of this script is to generate audiences based on keywords obtained from url content. 
  */
object KeysSegment {

   /**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR LOADING DATA     //////////////////////
    *
    */

   def getSelectedKeywords(
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

    val path = "/datascience/scraper/selected_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
    .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
    .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val df_selected = spark.read.format("csv")
    .option("basePath", path)
    .load(hdfs_files: _*)
    .toDF("url_raw","hits","country","kw","TFIDF","domain","stem_kw","day")
    .withColumnRenamed("url_raw","url")
    .select("url","kw")
    .withColumn("kw", split(col("kw"), " "))
    .withColumn("kw", explode(col("kw")))

    df_selected
  }

 def getDataUrls(
      spark: SparkSession,
      country: String,
      nDays: Integer,
      since: Integer
  ): DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_demo/data_urls"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day,country)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val data_urls = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      //.filter("share_data = 1")
      //.withColumn("segments", concat_ws(",", col("segments")))
    data_urls
  }


def getTFIDF(df_clean: DataFrame, spark:SparkSession ): DataFrame = {
    val docCount = df_clean.select("url").distinct.count

    val unfoldedDocs = df_clean.withColumn("count",lit(1))

    //TF: times token appears in document
    val tokensWithTf = unfoldedDocs.groupBy("url", "kw")
      .agg(count("count") as "TF")

    //IDF: logarithm of (Total number of documents divided by DF) . How common/rare a word is.
    def calcIdf =
      udf(
        (docCount: Int,DF: Long) =>
          log(docCount/DF)
      )

    //DF: number of documents where a token appears
    val tokensWithDfIdf = unfoldedDocs.groupBy("kw")
      .agg(approx_count_distinct(col("url"), 0.02).as("DF"))
      .withColumn("IDF", calcIdf(lit(docCount),col("DF")))
    
    //TF-IDF: score of a word in a document.
    //The higher the score, the more relevant that word is in that particular document.
    val tfidf_docs = tokensWithTf
      .join(tokensWithDfIdf, Seq("kw"), "inner")
      .withColumn("TFIDF", col("tf") * col("idf"))
      
    // Min-Max Normalization and filter by threshold
    // val (vMin, vMax) = tfidf_docs.agg(min(col("tf_idf")), max(col("tf_idf"))).first match {
    //   case Row(x: Double, y: Double) => (x, y)
    // }

    // val vNormalized = (col("tf_idf") - vMin) / (vMax - vMin) // v normalized to (0, 1) range

//    val tfidf_threshold  = 0

    tfidf_docs.select("url","kw","TFIDF")


  }  

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     MAIN METHOD     //////////////////////
    *
    */

  def MainProcess(
      spark: SparkSession) = {

    val from = 1
    val nDays = 30    

    /// Configuraciones de spark
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    /// Obtenemos la data de los ultimos ndays
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(from)

    val days =
      (0 until nDays).map(start.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_audiences_streaming"
    val dfs = days
      .flatMap(
        day =>
          (0 until 24).map(
            hour =>
              path + "/hour=%s%02d/"
                .format(day, hour)
          )
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      
    val df = spark.read
                  .option("basePath", "/datascience/data_audiences_streaming/")
                  .parquet(dfs: _*)
                  .filter("url is not null AND event_type IN ('retroactive', 'xd', 'xp')")
                  //.filter("id_partner IN (119, 1, 1139, 1122, 704)")
                  //.withColumn("day", lit(DateTime.now.minusDays(from).toString(format)))
                  .select("device_id", "url")
                  //.select("device_id", "url", "referer", "event_type","country","day","segments","time","share_data","id_partner")


    val devices = spark.read.format("csv")
    .option("delimiter","\t")
    .load("/datascience/audiences/crossdeviced/MX_Reporte_Clase_Alta_21_04_20_xd/")
    .toDF("device_type","device_id","segment")

    devices.join(df,Seq("device_id"),"left")
      .write
      .format("csv")
      .option("sep", "\t")      
      .mode(SaveMode.Overwrite)
      .save("/datascience/misc/kws_NSE_MX")    

    }      
      
    /**
    val urls_NSE_alto = getDataUrls(spark,"MX",30,0)
    .select("url", "segments")
    .withColumn("segment", explode(col("segments")))
    .filter("segment == 104014")
    .select("url")
    .dropDuplicates()

     urls_NSE_alto
    .write
    .format("csv")
    .option("sep", "\t")
    .mode("overwrite")
    .save("/datascience/misc/urls_NSE_alto_MX")    

    val urls_NSE_bajo = getDataUrls(spark,"MX",30,0)
    .select("url", "segments")
    .withColumn("segment", explode(col("segments")))
    .filter("segment == 104015")
    .select("url")
    .dropDuplicates()

     urls_NSE_bajo
    .write
    .format("csv")
    .option("sep", "\t")
    .mode("overwrite")
    .save("/datascience/misc/urls_NSE_bajo_MX")       

    val selected_keywords = getSelectedKeywords(spark,30,0)

    val kws_high = urls_NSE_alto.join(selected_keywords,Seq("url"),"inner")

    getTFIDF(kws_high,spark)
    .write
    .format("csv")
    .option("sep", "\t")
    .mode("overwrite")
    .save("/datascience/misc/kws_NSE_alto_MX")

    val kws_low = urls_NSE_bajo.join(selected_keywords,Seq("url"),"inner")

    getTFIDF(kws_low,spark)
    .write
    .format("csv")
    .option("sep", "\t")
    .mode("overwrite")
    .save("/datascience/misc/kws_NSE_bajo_MX")

    */

  


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
    //val options = nextOption(Map(), Args.toList)
    //val nDays = if (options.contains('nDays)) options('nDays) else 30
    //val since = if (options.contains('since)) options('since) else 1

    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("KeysSegment")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    //MainProcess(spark)

  spark.read.format("csv")
    .option("delimiter","\t")
    .load("/datascience/misc/kws_NSE_MX")
    .toDF("device_type","device_id","segment","url")
    .select("url","segment")
    .dropDuplicates()
    .write
    .format("csv")
    .option("sep", "\t")      
    .mode(SaveMode.Overwrite)
    .save("/datascience/misc/kws_NSE_MX_2")    
  

  }

}