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


 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("BigRandom").config("spark.sql.files.ignoreCorruptFiles", "true").getOrCreate()

    val nDays = 30
    val since = 1

    getUsersCustom(spark=spark,
                   nDays=nDays,
                   since=since)

    //val query_27 = "country IN ('MX', 'BR') AND event_type = 'pv' AND array_contains(platforms, '27')"
 
    /**
    val query = "country IN ('MX', 'BR') AND event_type = 'pv'"
    val nDays = 2
    val since = 1
    
     getDataEventQueue(
      spark=spark,
      query=query,
      nDays=nDays,
      since=since)
    
    getDataEventQueue_27(
      spark=spark,
      query_27=query_27,
      nDays=nDays,
      since=since)
    **/
  }
}