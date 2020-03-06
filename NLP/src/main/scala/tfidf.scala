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

import scala.math.log


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
    .appName("TFIDF")
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .getOrCreate()


    // ESTO DEBO hacerlo enrealidad tokenizando el TEXT (DEL DUMP).

    val path = "/datascience/selected_keywords/2020-02-10.csv"
    val df = spark.read
            .format("csv")
            .option("header", "True")
            .load(path)
            .select("url_raw","kw")
            .limit(500)
            .na.drop()
            .withColumn("document", split(col("kw"), " ")) 
            .withColumn("doc_id", monotonically_increasing_id())
            
    val docCount = df.count().toInt               
            
    val columns = df.columns.map(col) :+
        (explode(col("document")) as "token")
    val unfoldedDocs = df.select(columns: _*)

    //TF: times token appears in document
    val tokensWithTf = unfoldedDocs.groupBy("doc_id", "token")
      .agg(count("document") as "TF")

    //DF: number of documents where a token appears
    val tokensWithDf = unfoldedDocs.groupBy("token")
      .agg(countDistinct("doc_id") as "DF")

    //IDF: logarithm of (Total number of documents divided by DF) . How common/rare a word is.
    def calcIdf =
      udf(
        (docCount: Int,DF: Long) =>
          log(docCount/DF)
      )

    val tokensWithIdf = tokensWithDf.withColumn("IDF", calcIdf(lit(docCount),col("DF")))

    val tfidf_docs = tokensWithTf
      .join(tokensWithIdf, Seq("token"), "left")
      .withColumn("tf_idf", col("tf") * col("idf"))
      .join(df,Seq("doc_id"),"left")


  }
}