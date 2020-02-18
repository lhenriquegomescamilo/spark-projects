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
    .load("/datascience/keywiser/test_stem/AR_stem_scrapper_test_15Dsince28*")
    .withColumnRenamed("_c1", "device_id")
    .withColumnRenamed("_c2", "segment_id")
    .groupBy("segment_id")
    .agg(
      approx_count_distinct(col("device_id"), rsd = 0.02) as "count_old"
    )    

    val df_new = spark.read.format("csv")
    .option("delimiter","\t")
    .option("header",false)
    .load("/datascience/keywiser/test_stem/AR_stem_scrapper_test_15Dsince1*")
    .withColumnRenamed("_c1", "device_id")
    .withColumnRenamed("_c2", "segment_id")
    .groupBy("segment_id")
    .agg(
      approx_count_distinct(col("device_id"), rsd = 0.02) as "count_new"
    )    

    val country = "AR"
    val df = df_old.join(df_new,Seq("segment_id"))
    df.withColumn("country", lit(country))
    .write
    .format("csv")
    .option("delimiter","\t")
    .option("header",true)
    .mode("append") 
    .save("/datascience/misc/stem_test_results2")

    //val countries = "AR,BR,CL,CO,EC,MX,PE,US".split(",").toList
    /*
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
    **/
    
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