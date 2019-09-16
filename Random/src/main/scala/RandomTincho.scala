package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.hadoop.conf.Configuration
import org.apache.spark.ml.classification.{
  RandomForestClassificationModel,
  RandomForestClassifier
}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.classification.{
  GBTClassificationModel,
  GBTClassifier
}

//import org.apache.spark.mllib.feature.Stemmer

import java.security.MessageDigest
import java.util
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import java.time.DateTimeException
import java.sql.Savepoint

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object RandomTincho {

  def get_report_gcba_1134(spark:SparkSession)
      get_segments_pmi(spark)  {
    val myUDF = udf((url: String) => processURL(url))

    /// Configuraciones de spark
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    /// Obtenemos la data de los ultimos ndays
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(1)

    val days =
      (0 until 33).map(start.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_partner_streaming"
    val dfs = days
      .flatMap(
        day =>
          (0 until 24).map(
            hour =>
              path + "/hour=%s%02d/id_partner=1134"
                .format(day, hour)
          )
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      .map(
        x =>
          spark.read
            .option("basePath", "/datascience/data_audiences_streaming/")
            .parquet(x)
            .filter("event_type = 'tk' AND url LIKE '%r_mobile=%'")
            .select("url")
            .withColumn("values", myUDF(col("url")))
            .filter(length(col("values"))>0)
      )

    /// Concatenamos los dataframes
    val dataset = dfs.reduce((df1, df2) => df1.union(df2)).distinct()

    dataset.write.format("parquet")
            .mode(SaveMode.Overwrite)
            .save("/datascience/custom/1134_septiembre")

  }

 def get_segments_pmi(spark:SparkSession){

   val files = List("/datascience/misc/cookies_chesterfield.csv",
                 "/datascience/misc/cookies_marlboro.csv",
                 "/datascience/misc/cookies_phillip_morris.csv",
                 "/datascience/misc/cookies_parliament.csv")

   /// Configuraciones de spark
   val sc = spark.sparkContext
   val conf = sc.hadoopConfiguration
   val fs = org.apache.hadoop.fs.FileSystem.get(conf)

   val format = "yyyyMMdd"
   val start = DateTime.now.minusDays(1)

   val days = (0 until 30).map(start.minusDays(_)).map(_.toString(format))
   val path = "/datascience/data_triplets/segments/"
   val dfs = days.map(day => path + "day=%s/".format(day) + "country=AR")
     .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
     .map(
       x =>
         spark.read
           .option("basePath", "/datascience/data_triplets/segments/")
           .parquet(x)
     )

   var data = dfs.reduce((df1, df2) => df1.union(df2))

   for (filename <- files){
     var cookies = spark.read.format("csv").load(filename)
                                         .withColumnRenamed("_c0","device_id")

     data.join(broadcast(cookies),Seq("device_id"))
         .select("device_id","feature","count")
         .dropDuplicates()
         .write.format("csv")
         .mode(SaveMode.Overwrite)
         .save("/datascience/custom/segments_%s".format(filename.split("/").last.split("_").last))
   }
 }

 

  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("Random Tincho").config("spark.sql.files.ignoreCorruptFiles", "true").getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)
    
    get_report_gcba_1134(spark) 
  }

}