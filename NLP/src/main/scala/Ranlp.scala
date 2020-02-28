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


/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object Ranlp {

  def getDataKeywords(
      spark: SparkSession,
      country: String,
      nDays: Integer,
      since: Integer,
      stemming: Integer
  ): DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, country)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val to_select =
      if (stemming == 1) List("stemmed_keys", "device_id","domain")
      else List("content_keys", "device_id","domain")

    val columnName = to_select(0).toString

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select(to_select.head, to_select.tail: _*)
      .withColumnRenamed(columnName, "content_keywords")
      .na
      .drop()

    df
  }


  def getJointKeys(
      df_keys: DataFrame,
      df: DataFrame,
      verbose: Boolean
  ): DataFrame = {

    val df_joint = df
      .join(broadcast(df_keys), Seq("content_keywords"))
      .select("content_keywords", "device_id","domain")
      .dropDuplicates()

    /**
    if verbose {
      println(
        "count del join con duplicados: %s"
          .format(df_joint.select("device_id").distinct().count())
      )
    }
    */   
    val df_grouped = df_joint
      .groupBy("device_id")
      .agg(collect_list("content_keywords").as("kws"),collect_list("domain").as("domain"))
      .withColumn("device_type", lit("web"))
      .select("device_type", "device_id", "kws","domain")
    df_grouped
  }


///////////////////////7////////////////////////////////////

import scala.collection.mutable.ListBuffer

val domains = List("ac", "ad", "ae", "af", "ag", "ai", "al", "am", "an", "ao", "aq", "ar", "as", "asia", "at", "au", "aw", "ax", "az", "ba", "bb", "bd", "be", "bf", "bg", "bh", "bi", "biz", "bj", "bm", "bn", "bo", "br", "bs", "bt", "bv", "bw", "by", "bz", "ca", "cat", "cc", "cd", "cf", "cg", "ch", "ci", "ck", "cl", "cm", "cn", "co", "com", "coop", "cr", "cu", "cv", "cx", "cy", "cz", "de", "dj", "dk", "dm", "do", "dz", "ec", "edu", "ee", "eg", "er", "es", "et", "eu", "fi", "fj", "fk", "fm", "fo", "fr", "ga", "gb", "gd", "ge", "gf", "gg", "gh", "gi", "gl", "gm", "gn", "gob", "gov", "gp", "gq", "gr", "gs", "gt", "gu", "gw", "gy", "hk", "hm", "hn", "hr", "ht", "hu", "id", "ie", "il", "im", "in", "info", "int", "io", "iq", "ir", "is", "it", "je", "jm", "jo", "jobs", "jp", "ke", "kg", "kh", "ki", "km", "kn", "kp", "kr", "kw", "ky", "kz", "la", "lb", "lc", "li", "lk", "lr", "ls", "lt", "lu", "lv", "ly", "ma", "mc", "md", "me", "mg", "mh", "mil", "mk", "ml", "mm", "mn", "mo", "mobi", "mp", "mq", "mr", "ms", "mt", "mu", "mv", "mw", "mx", "my", "mz", "na", "nc", "ne", "net", "nf", "ng", "ni", "nl", "no", "np", "nr", "nu", "nz", "om", "org", "pa", "pe", "pf", "pg", "ph", "pk", "pl", "pm", "pn", "pr", "pro", "ps", "pt", "pw", "py", "qa", "re", "ro", "rs", "ru", "rw", "sa", "sb", "sc", "sd", "se", "sg", "sh", "si", "sj", "sk", "sl", "sm", "sn", "so", "sr", "st", "su", "sv", "sy", "sz", "tc", "td", "tel", "tf", "tg", "th", "tj", "tk", "tl", "tm", "tn", "to", "tp", "tr", "tt", "tv", "tw", "tz", "ua", "ug", "uk", "us", "uy", "uz", "va", "vc", "ve", "vg", "vi", "vn", "vu", "wf", "ws", "xxx", "ye", "yt", "za", "zm", "zw")

def parseURL(url: String): String = {

  // First we obtain the query string and the URL divided in 
  val split = url.split("\\?")

  val fields = split(0).split('/')

  // Now we can get the URL path and the section with no path at all
  val path = (if (url.startsWith("http")) fields.slice(3, fields.length)
  else fields.slice(1, fields.length))
  val non_path = (if (url.startsWith("http")) fields(2) else fields(0)).split("\\:")(0)

  // From the non-path, we can get the extension, the domain, and the subdomain
  val parts = non_path.split("\\.").toList
  var extension: ListBuffer[String] = new ListBuffer[String]()
  var count = parts.length
  if (count > 0) {
    var part = parts(count - 1)
    // First we get the extension
    while (domains.contains(part) && count > 1) {
      extension += part
      count = count - 1
      part = parts(count - 1)
    }

  // Now we obtain the domain and subdomain.
  val domain = if (count > 0) parts(count - 1) else ""

  domain

  } else ("")
  }

val udfGetDomain = udf(
    (url: String) =>
    parseURL(url)
  )

 def getDataURLS(
      spark: SparkSession,
      country: String,
      nDays: Integer,
      since: Integer
  ): DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/url_ingester/data"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, country)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .withColumn("domain",udfGetDomain(col("url")))
      .na
      .drop()

    df
  }





 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark = SparkSession.builder
    .appName("Ranlp")
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .getOrCreate()

    
    val path = "/datascience/scraper/temp_dump/2020-02-10_daily.csv"
    val df = spark.read
            .format("csv")
            .option("header", "True")
            .option("sep", "\t")
            .load(path)
            .select("text")


    import com.johnsnowlabs.nlp.{DocumentAssembler}
    import com.johnsnowlabs.nlp.annotator.{PerceptronModel, PerceptronApproach, SentenceDetector, Tokenizer}
    import org.apache.spark.ml.Pipeline

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

    import com.johnsnowlabs.nlp.training.POS
    val trainPOS = POS().readDataset(spark, "./src/main/resources/anc-pos-corpus")

    /**
    val spanish_pos_path =
    val spanish_pos = PerceptronModel.load(spanish_pos_path)
      .setInputCols(Array("sentence", "token"))
      .setOutputCol("pos")

    *//  

    val posTagger = new PerceptronApproach()
    .setInputCols(Array("sentence", "token"))
    .setOutputCol("pos")
    .setNIterations(2)
    .fit(trainPOS)

    val pipeline = new Pipeline().setStages(Array(
        documentAssembler,
        sentenceDetector,
        tokenizer,
        posTagger
    ))

    val doc = pipeline.fit(df).transform(df)


    doc.write.format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/misc/testnlp.csv")






  }
}