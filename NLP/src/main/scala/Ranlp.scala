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
import com.johnsnowlabs.nlp.annotator.{PerceptronModel, SentenceDetector, Tokenizer, Normalizer}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.Pipeline

import scala.math.log


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

    
    val path = "/datascience/scraper/parsed/processed/day=20200312"
    val doc = spark.read
            .format("parquet")
            .option("header", "True")
            .option("sep", "\t")
            .load(path)
            .select("url","text")
            .na. drop()

    val documentAssembler = new DocumentAssembler()               
                           .setInputCol("text")     
                           .setOutputCol("document")     
                           .setCleanupMode("shrink")
    
    val sentenceDetector = new SentenceDetector()
    .setInputCols("document")
    .setOutputCol("sentence")

    val tokenizer = new Tokenizer()
    .setInputCols("sentence")
    .setOutputCol("token")
    .setContextChars(Array("(", ")", "?", "!",":","¡","¿"))
    .setTargetPattern("^a-zA-Z0-9")

    // lo que no se bien es el tema de las ñ

    val spanish_pos = PerceptronModel.load("/datascience/misc/pos_ud_gsd_es_2.4.0_2.4_1581891015986")

    val posTagger = spanish_pos
    .setInputCols(Array("sentence", "token"))
    .setOutputCol("pos")

    /**

    // LA FORMA QUE NO PUEDO HACER CAMINAR----------

    val finisher = new Finisher()
    .setInputCols("pos")
    .setIncludeMetadata(true)
    .setOutputAsArray(true)

    val pipeline = new Pipeline().setStages(Array(
        documentAssembler,
        sentenceDetector,
        tokenizer,       
        posTagger,
        finisher
    ))

    var df = pipeline.fit(doc).transform(doc) 

    val udfZip = udf((finished_pos: Seq[String], finished_pos_metadata: Seq[(String,String)]) => finished_pos zip finished_pos_metadata)
    
    val udfGet1 = udf((word: Row, index:String ) => word.getAs[String](index))

    val udfGet2 = udf((word: Row, index:String ) => word.getAs[Array[String]](index))
    
    df = df.withColumn("zipped",udfZip(col("finished_pos"),col("finished_pos_metadata")))
    df.show()
    df = df.withColumn("zipped", explode(col("zipped")))
    df.show()
    df = df.withColumn("tag",udfGet1(col("zipped"),lit("_1")))
    df.show()
    df = df.filter("tag = 'NOUN' or tag = 'PROPN'")
    df.show()

    */

    
    val pipeline = new Pipeline().setStages(Array(
        documentAssembler,
        sentenceDetector,
        tokenizer,
        posTagger
    ))

    var df = pipeline.fit(doc).transform(doc)

    def getWord =
          udf(
            (mapa: Map[String,String]) =>
              mapa("word")
          )
    
    def getString =
    udf((array: Seq[String]) => array.map(_.toString).mkString(","))

     
    df = df.withColumn("tmp", explode(col("pos"))).select("url","tmp.*")
      .withColumn("words", getWord(col("metadata")))
      .select("url","words","result")
      .filter("result = 'NOUN' or result = 'PROPN'")
      .withColumn("words", lower(col("words")))
      .groupBy("url")
      .agg(collect_list("words").as("document"))
      .select("url","document")
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
    
    //TF-IDF: score of a word in a document.
    //The higher the score, the more relevant that word is in that particular document.
    val tfidf_docs = tokensWithTf
      .join(tokensWithIdf, Seq("token"), "left")
      .withColumn("tf_idf", col("tf") * col("idf"))
      .join(df,Seq("doc_id"),"left")

    tfidf_docs.groupBy("url")
    .agg(collect_list("token").as("kws"),collect_list("tf_idf").as("tfidf"))
    .select("url","kws","tfidf")
    .withColumn("kws",getString(col("kws")))
    .withColumn("tfidf",getString(col("tfidf")))
    .write.format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .mode(SaveMode.Overwrite)
    .save("/datascience/misc/testnlp3.csv")
  


  }
}