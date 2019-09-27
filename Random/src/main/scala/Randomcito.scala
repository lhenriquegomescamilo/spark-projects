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
    
    test.groupBy("id_partner","share_data")
        .agg(count("*").as("Cantidad"))
        .orderBy("id_partner", "share_data")

    test.write
        .mode("overwrite")
        .format("parquet")
        .save("/datascience/data_audiences_temporal")
  }

  def main(args: Array[String]) {

    val spark = SparkSession.builder
        .appName("Run Randomcito")
        //.config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .getOrCreate()
    
    process_data(spark)
  }

}
