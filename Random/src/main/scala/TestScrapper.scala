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
object TestScrapper {

 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark = SparkSession.builder
    .appName("TestScrapper")
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .getOrCreate()


    val df_old = spark.read.format("csv")
    .option("delimiter","\t")
    .option("header",false)
    .load("/datascience/keywiser/test99/AR_old_15Dsince30_*")
    .withColumnRenamed("_c1", "device_id")
    .withColumnRenamed("_c2", "segment_id")
    .groupBy("segment_id")
    .agg(
      approx_count_distinct(col("device_id"), rsd = 0.02) as "count_old"
    )    

    val df_new = spark.read.format("csv")
    .option("delimiter","\t")
    .option("header",false)
    .load("/datascience/keywiser/test99/AR_new_15Dsince10_*")
    .withColumnRenamed("_c1", "device_id")
    .withColumnRenamed("_c2", "segment_id")
    .groupBy("segment_id")
    .agg(
      approx_count_distinct(col("device_id"), rsd = 0.02) as "count_new"
    )    

    val country = "AR"
    val df = df_old.join(df_new,Seq("segment_id"),"outer").na.fill(0)
    df.withColumn("country", lit(country))
    .write
    .format("csv")
    .option("delimiter","\t")
    .option("header",true)
    .mode("append") 
    .save("/datascience/misc/st99")
  
    /**   


    val countries = "AR,BR,CL,CO,EC,MX,PE,US".split(",").toList
    
    for (country <- countries) {

     var df_old = spark.read.format("csv")
    .option("delimiter","\t")
    .option("header",false)
    .load("/datascience/keywiser/test_all/%s_st3_15Dsince29_*".format(country))
    .withColumnRenamed("_c1", "device_id")
    .withColumnRenamed("_c2", "segment_id")
    .groupBy("segment_id")
    .agg(
      approx_count_distinct(col("device_id"), rsd = 0.02) as "count_old"
    )    

    var df_new = spark.read.format("csv")
    .option("delimiter","\t")
    .option("header",false)
    .load("/datascience/keywiser/test_all/%s_st3_15Dsince2_*".format(country))
    .withColumnRenamed("_c1", "device_id")
    .withColumnRenamed("_c2", "segment_id")
    .groupBy("segment_id")
    .agg(
      approx_count_distinct(col("device_id"), rsd = 0.02) as "count_new"
    )    

    var df = df_old.join(df_new,Seq("segment_id"))
    df.withColumn("country", lit(country))
    .write
    .format("csv")
    .option("delimiter","\t")
    .option("header",true)
    .mode("append") 
    .save("/datascience/misc/st3_all_countries")
    }
 
     **/  

  }
}