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


object TapadWhitelist {

  def get_monthly_data_homes(spark:SparkSession, country:String): DataFrame = {
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(0)
    val path = "/datascience/data_insights/homes/"

    val days = (0 until 30).map(start.minusDays(_)).map(_.toString(format))

    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val data = spark.read
                    .option("basePath", path)
                    .parquet(hdfs_files: _*)
                    .filter("device_type != 'web'") // get only madids
                    .withColumnRenamed("device_id","madid")
                    .select("madid")
    data

  }

  def whitelist_madids_report(spark: SparkSession,date:String){
    val current_month = DateTime.now().toString("yyyyMM")

    val today = DateTime.now.toString("yyyyMMdd")
    val madids_factual = spark.read.format("csv").option("sep","\t")
                              .load("/datascience/devicer/processed/madids_factual_%s/".format(current_month))
                              .withColumnRenamed("_c1","madids")
                              .select("madids")

    val madids_startapp = spark.read.format("csv").option("sep","\t")
                              .load("/datascience/devicer/processed/madids_startapp_%s/".format(current_month))
                              .withColumnRenamed("_c1","madids")
                              .select("madids")
    // GEO
    val madids_geo_ar = get_monthly_data_homes(spark,"AR")

    val madids_geo_mx = get_monthly_data_homes(spark,"MX")

    val madids_geo_cl = get_monthly_data_homes(spark,"CL")

    val madids_geo_co = get_monthly_data_homes(spark,"CO")

    // Etermax
    val madids_etermax = spark.read.format("csv")
                              .load("/datascience/data_tapad/madids_etermax.csv")
                              .withColumnRenamed("_c0","madids")

    madids_factual.union(madids_startapp)
                  .union(madids_geo_ar)
                  .union(madids_geo_mx)
                  .union(madids_geo_cl)
                  .union(madids_geo_co)
                  .union(madids_etermax)
                  .withColumn("madids",lower(col("madids")))
                  .distinct
                  .write.format("csv")
                  .save("/datascience/data_tapad/whitelist/%s".format(today))

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
    val date = DateTime.now.minusDays(1).toString(format)
    whitelist_madids_report(spark,date)

  }
}
