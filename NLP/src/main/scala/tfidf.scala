package main.scala
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.joda.time.{Days, DateTime}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.Pipeline


/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object tfidf {





 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark = SparkSession.builder
    .appName("Ranlp")
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .getOrCreate()

    
    val path = "/datascience/scraper/selected_keywords/2020-02-10.csv"
    val df = spark.read
            .format("csv")
            .option("header", "True")
            .load(path)
            .select("url_raw","kw")
            .na.drop()
            .withColumn("document", split(col("kw"), " "))
            .withColumn("doc_id", monotonically_increasing_id())

    
    val columns = df.columns.map(col) :+
        (explode(col("document")) as "token")
    val unfoldedDocs = df.select(columns: _*)

    val tokensWithTf = unfoldedDocs.groupBy("doc_id", "token")
      .agg(count("document") as "tf")

    val tokensWithDf = unfoldedDocs.groupBy("token")
      .agg(countDistinct("doc_id") as "df")


    /*
    val calcIdfUdf = udf { df: Long => calcIdf(docCount, df) } 

    def calcIdf = IDF(t,D) = log[ (|D| + 1) / (DF(t,D) + 1) ] 

    def getAllPlatforms =
      udf((array: Seq[Integer]) => array.reduce((i1, i2) => i1 | i2).toInt)


    val tokensWithIdf = tokensWithDf.withColumn("idf", calcIdfUdf(col("df")))

    val tfidf_docs = tokensWithTf
      .join(tokensWithIdf, Seq("token"), "left")
      .withColumn("tf_idf", col("tf") * col("idf"))

    //tfidf_docs.merge(df)  

      */      



  





  }
}