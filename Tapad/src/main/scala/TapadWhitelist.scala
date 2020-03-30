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
    val madids_geo_ar = spark.read.format("csv").option("delimiter","\t")
                              .load("/datascience/geo/NSEHomes/argentina_365d_home_21-1-2020-12h")
                              .withColumnRenamed("_c0","madids")
                              .select("madids")

    val madids_geo_mx = spark.read.format("csv").option("delimiter","\t")
                          .load("/datascience/geo/NSEHomes/mexico_200d_home_29-1-2020-12h")
                          .withColumnRenamed("_c0","madids")
                          .select("madids")

    val madids_geo_cl = spark.read.format("csv").option("delimiter","\t")
                                  .load("/datascience/geo/NSEHomes/CL_90d_home_29-1-2020-12h")
                                  .withColumnRenamed("_c0","madids")
                                  .select("madids")

    val madids_geo_co = spark.read.format("csv").option("delimiter","\t")
                              .load("/datascience/geo/NSEHomes/CO_90d_home_18-2-2020-12h")
                              .withColumnRenamed("_c0","madids")
                              .select("madids")


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
