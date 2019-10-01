package main.scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.joda.time.Days
import org.joda.time.DateTime

object Randomcito {

  def process_data(spark: SparkSession) = {
    val test = spark.read.format("parquet")
    .option("basePath", "/datascience/data_audiences_streaming_5/")
    .load("/datascience/data_audiences_streaming_5/hour=20190924*")
    
    val filters = test.groupBy("id_partner","share_data")
        .agg(count("*").as("Cantidad"))
        .orderBy("id_partner", "share_data")

    filters.repartition(1).write
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .option("sep", ";")
        .save("/datascience/data_audiences_temporal")
  }

  def prueba_eq(spark: SparkSession) = {
    val input = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/data/eventqueue/2019/10/01/1300.tsv.gz")

    //input.orderBy("device_type", "nav_type", "event_type", "data_type", "country", "site_id", "category")
    input.orderBy("device_id")
      .repartition(1)
      .write
      .format("parquet")
      .mode("overwrite")
      //.option("sep", "\t")
      //.option("header", "true")
      //.option("compression", "gzip")
      .save("/datascience/data_audiences_temporal/prueba_pq/")
  }

  def main(args: Array[String]) {

    val spark = SparkSession.builder
        .appName("Run Randomcito")
        //.config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .getOrCreate()
    
    prueba_eq(spark)
  }

}
