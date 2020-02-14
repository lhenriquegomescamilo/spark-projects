package main.scala
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
object BigRandom {

def getUsersCustom(
      spark: SparkSession,
      nDays: Integer,
      since: Integer
  ) = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_triplets/segments"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val segList=List(154,155,158,177,178,103928,103937,103939,103966,103986)
    val dir = "/datascience/misc/"
    val fileNameFinal = dir + "danone_mx_juizzy"

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .filter("country = 'MX'")
      .withColumnRenamed("feature", "seg_id")
      .select("seg_id","device_id")
      .filter(col("seg_id").isin(segList:_*))
      .dropDuplicates()
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(fileNameFinal)
  }

 def checkNulls(
      spark: SparkSession,
      nDays: Integer,
      since: Integer
  ) = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_triplets/segments"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select("id_partner","day")
      .filter("id_partner is null").select("day").distinct()
      .write.format("csv")
      .option("header",true)
      .option("delimiter","\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/misc/checknulls.csv")

  }


def getDataEventQueue_27(
      spark: SparkSession,
      query_27: String,
      nDays: Integer,
      since: Integer) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val path = "/data/eventqueue"
        // Get the days to be loaded
    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/%s".format(day)) //
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read
        .option("sep", "\t")
        .option("header", "true")
        .format("csv")
        .load(hdfs_files: _*)
        .select("country", "device_id","platforms")
        .na
        .drop()
        .withColumn("platforms", split(col("platforms"), "\u0001"))
        .filter(query_27)
        .select("country", "device_id").distinct()
        .write.format("csv")
        .option("header",true)
        .option("delimiter","\t")
        .mode(SaveMode.Overwrite)
        .save("/datascience/misc/pv_platform27.csv")
  }


def getDataEventQueue(
      spark: SparkSession,
      query: String,
      nDays: Integer,
      since: Integer) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val path = "/data/eventqueue"
        // Get the days to be loaded
    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/%s".format(day)) //
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read
        .option("sep", "\t")
        .option("header", "true")
        .format("csv")
        .load(hdfs_files: _*)
        .filter(query)
        .select("country", "device_id").distinct()
        .na
        .drop()        
        .write.format("csv")
        .option("header","false")
        .option("delimiter","\t")
        .mode(SaveMode.Overwrite)
        .save("/datascience/misc/pv_mx_br.csv")
  }

def getReport(
      spark: SparkSession
  ) = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val nDays = 90
    val since = 1
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_triplets/segments"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path))) //analogue to "os.exists"

    val dir = "/datascience/misc/"
    val fileNameFinal = dir + "doohmain_90d_PE"

    val path_data = "/datascience/geo/crossdeviced/doohmain_90d_PE_2-1-2020-15h_xd_dropped"
    val data =  spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load(path_data)

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .filter("country = 'PE'")
      .withColumnRenamed("feature", "seg_id")
      .select("seg_id","device_id")
      .join(broadcast(data), Seq("device_id"))
      .groupBy("name","device_id")
      .agg(collect_list("seg_id") as "segments")
      .withColumn("segments", concat_ws(",", col("segments")))     
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(fileNameFinal)
  }


 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark = SparkSession.builder
    .appName("BigRandom")
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .getOrCreate()


    //val countries = "AR,BR,CL,CO,EC,MX,PE,US".split(",").toList

    val countries = "AR".split(",").toList

    for (country <- countries) {

     var df_old = spark.read.format("csv")
    .option("delimiter","\t")
    .option("header",false)
    .load("/datascience/keywiser/test/%s_scrapper_test_15Dsince24*".format(country))
    .withColumnRenamed("_c1", "device_id")
    .withColumnRenamed("_c2", "segment_id")
    .groupBy("segment_id")
    .agg(countDistinct("device_id").as("count_old"))

    var df_new = spark.read.format("csv")
    .option("delimiter","\t")
    .option("header",false)
    .load("/datascience/keywiser/test/%s_scrapper_test_15Dsince1*".format(country))
    .withColumnRenamed("_c1", "device_id")
    .withColumnRenamed("_c2", "segment_id")
    .groupBy("segment_id")
    .agg(countDistinct("device_id").as("count_new"))

    var df = df_old.join(df_new,Seq("segment_id"))
    df.withColumn("country", lit(country))
    .write
    .format("csv")
    .option("delimiter","\t")
    .option("header",true)
    .mode("append") 
    .save("/datascience/misc/scrapper_test_results")
    }
    
    /**  
    val audiences = """MX_71172_2020-01-30T14-29-02-220256,MX_71172_2020-01-30T14-29-14-744166,MX_71172_2020-01-30T14-29-33-106219,MX_71172_2020-01-30T14-29-02-220256,MX_71172_2020-01-30T14-29-23-107754,MX_71172_2020-01-30T14-29-35-550514,MX_71172_2020-01-30T14-29-38-074317,MX_71172_2020-01-30T14-28-57-423908,MX_71172_2020-01-30T14-29-40-379240""".split(",").toList
    val ids = """124641,124643,124645,124647,124649,124651,124653,124655,124657""".split(",").toList

    // Total por audiencia
    for ((file, i) <- (audiences zip ids)){
        spark.read.format("csv")
        .option("sep", "\t")
        .load("/datascience/devicer/processed/%s".format(file))
        .filter("_c2 LIKE '%"+i+"%'")
        .write.format("csv")
        .option("sep", "\t")
        .mode("append")
        .save("/datascience/misc/amex_leo_feb6.csv")
    }

  

    def getString =
  udf((array: Seq[Integer]) => array.map(_.toString).mkString(","))
          
    val df = spark.read.format("csv")
        .option("sep", "\t")
        .load("/datascience/misc/amex_leo_feb6.csv")
        .toDF("device_type","device_id","segment")
        .groupBy("device_type","device_id").agg(collect_list("segment").as("segment"))
        .withColumn("segment",getString(col("segment")))
        .write.format("csv")
        .option("sep", "\t")
        .mode(SaveMode.Overwrite)
        .save("/datascience/misc/amex_leo_feb6_total")      

  */


  }
}