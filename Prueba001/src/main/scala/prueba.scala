package main.scala

import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}

object Prueba1 {


  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Prueba q").getOrCreate()

    val df = spark.read.parquet("/datascience/data_audiences_streaming/hour=2019072917/")
    df.columns.foreach(println)
  }
}
