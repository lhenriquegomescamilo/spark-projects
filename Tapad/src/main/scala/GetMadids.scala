package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.ml.Pipeline
import org.joda.time.Days
import org.apache.spark._
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.commons.lang3.StringUtils
import scala.collection.mutable.WrappedArray
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.functions.{
  upper,
  count,
  col,
  abs,
  udf,
  regexp_replace,
  split,
  lit,
  explode,
  length,
  to_timestamp,
  from_unixtime,
  date_format,
  sum
}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{
 StructType,
 StructField,
 StringType,
 IntegerType
}
import org.apache.spark.sql.{Column, Row}
import scala.util.Random.shuffle


object GetMadids {
  def madids_startapp( spark: SparkSession,date: String){
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://rely-hdfs")

    val fs = FileSystem.get(conf)
    val os = fs.create(new Path("/datascience/devicer/to_process/madids_startapp_%s.json".format(date)))

    val content = """{"xd": 0, "partnerId": "1139", "query": "device_type = 'android' or device_type = 'and' or device_type = 'ios'", "ndays": 45, "queue": "datascience", "pipeline": 0, "segmentId": 1139, "since": 1, "priority": 14, "common": "country = ''", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}"""
    os.write(content.getBytes)
    fs.close()

  }

  def madids_factual(spark:SparkSession,date:String){

    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://rely-hdfs")

    val fs = FileSystem.get(conf)
    val os = fs.create(new Path("/datascience/devicer/to_process/madids_factual_%s.json".format(date)))

    val content = """{"xd": 0, "partnerId": "1008", "query": "device_type = 'android' or device_type = 'and' or device_type = 'ios'", "ndays": 45, "queue": "datascience", "pipeline": 0, "segmentId": 1008, "since": 1, "priority": 14, "common": "country = ''", "as_view": 119, "push": 0, "xdFilter": "index_type = 'coo'"}"""
    os.write(content.getBytes)
    fs.close()
  }



  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("Selected Keywords")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    val format = "yyyyMMdd"
    val date = DateTime.now.toString(format)
    madids_factual(spark,date)
    madids_startapp(spark,date)

  }
}
