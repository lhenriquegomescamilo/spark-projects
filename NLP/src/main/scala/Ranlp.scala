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
import com.johnsnowlabs.nlp._
import com.johnsnowlabs.nlp.annotators._
import com.johnsnowlabs.nlp.annotators.pos.perceptron._
import com.johnsnowlabs.nlp.annotators.sbd.pragmatic._
import com.johnsnowlabs.nlp.util.io.ResourceHelper
import com.johnsnowlabs.util.Benchmark

import com.johnsnowlabs.nlp.{DocumentAssembler}
import com.johnsnowlabs.nlp.annotator.{PerceptronModel, PerceptronApproach, SentenceDetector, Tokenizer}

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object Ranlp {





 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark = SparkSession.builder
    .appName("Ranlp")
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .getOrCreate()

    
    val path = "/datascience/scraper/temp_dump2/2020-02-10_daily.csv"
    val df = spark.read
            .format("csv")
            .option("header", "True")
            .option("sep", "\t")
            .load(path)
            .select("url","text")
            .na. drop()

    val documentAssembler = new DocumentAssembler()               
                           .setInputCol("text")     
                           .setOutputCol("document")     
                           .setCleanupMode("shrink")

    //val doc_df = documentAssembler.transform(spark_df)


    //pipeline??
    // POS perceptron model spanish?
    
    val sentenceDetector = new SentenceDetector()
    .setInputCols("document")
    .setOutputCol("sentence")

    val tokenizer = new Tokenizer()
    .setInputCols("sentence")
    .setOutputCol("token")
    //.setContextChars(Array("(", ")", "?", "!"))
    //.setSplitChars(Array('-'))


    /**
    val spanish_pos_path =
    val spanish_pos = PerceptronModel.load(spanish_pos_path)
      .setInputCols(Array("sentence", "token"))
      .setOutputCol("pos")

    */  


    

    //val spanish_pos = PerceptronModel.pretrained("pos_ud_gsd", lang="es")

    val spanish_pos = PerceptronModel.load("/datascience/misc/pos_ud_gsd_es_2.4.0_2.4_1581891015986")

    
    val posTagger = spanish_pos
    .setInputCols(Array("sentence", "token"))
    .setOutputCol("pos")



    /**

    //import com.johnsnowlabs.nlp.training.POS
    //val trainPOS = POS().readDataset(spark, "./src/main/resources/anc-pos-corpus")

    val posTagger = new PerceptronApproach()
    .setInputCols(Array("sentence", "token"))
    .setOutputCol("pos")
    .setNIterations(2)
    .fit(trainPOS)
    */

    

    val pipeline = new Pipeline().setStages(Array(
        documentAssembler,
        sentenceDetector,
        tokenizer,
        posTagger
    ))


    /**


    val pipeline = new Pipeline().setStages(Array(
        documentAssembler,
        sentenceDetector,
        tokenizer
    ))

    */
    val doc = pipeline.fit(df).transform(df)

    //println(doc.withColumn("tmp", explode(col("pos"))).select("tmp.*").show())

    //println(doc.show())

    def getWord =
          udf(
            (mapa: Map[String,String]) =>
              mapa("word")
          )
    
    def getString =
    udf((array: Seq[String]) => array.map(_.toString).mkString(","))

    doc.withColumn("tmp", explode(col("pos"))).select("url","tmp.*")
      .withColumn("keyword", getWord(col("metadata")))
      .select("url","keyword","result")
      .filter("result = 'NOUN' or result = 'PROPN'")
      .groupBy("url")
      .agg(collect_list("keyword").as("kws"))
      .withColumn("kws",getString(col("kws")))
      .select("url","kws")
      .write.format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/misc/testnlp2.csv")
    





  }
}