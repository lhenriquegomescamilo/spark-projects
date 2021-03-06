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


object TapadFull {
  
  def full_madid_report(spark:SparkSession){
    val date_bridge = DateTime.now.minusDays(30).toString("MM_yyyy") // Use previous month date
    val today = DateTime.now.toString("yyyyMMdd")
    
    val mapping = Map("android" -> "HARDWARE_ANDROID_AD_ID", "ios" -> "HARDWARE_IDFA", "AAID" ->"HARDWARE_ANDROID_AD_ID", "IDFA" -> "HARDWARE_IDFA")
    val udfDeviceType = udf((device_type: String) => mapping(device_type))

    val data_bridge = spark.read
                            .format("csv")
                            .option("header","true")
                            .load("/data/providers/Bridge/files_-_Retargetly_Bridge_Linkage_LATAM_%s.csv".format(date_bridge))
                            .select("Timestamp","IP_Address","Device_ID","Device_Type")
                            .withColumn("Timestamp",unix_timestamp(col("Timestamp")))
                            .filter("Device_Type is not null")
                            .withColumn("Device_Type",udfDeviceType(col("Device_Type")))
                            .withColumn("Platform",lit(""))
                            .select("Timestamp","Device_ID","IP_Address","Device_Type","platform")

    data_bridge.write
              .format("csv")
              .option("sep","\t")
              .mode(SaveMode.Overwrite)
              .save("/datascience/data_tapad/full_report/%s".format(today))
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
    full_madid_report(spark)

  }
}
