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

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object ByTwoData {

  /**
    *
    *
    *
    *
    *        DATA DE ENCUESTAS DE LAS VOTACIONES
    *
    *
    *
    *
    */
  def getDataVotaciones(spark: SparkSession) = {
    val data_audience =
      getDataAudiences(spark, nDays = 30, since = 1)
        .filter(
          "country = 'AR' and event_type IN ('tk', 'batch', 'data', 'pv')"
        )
        .select("device_id", "url", "timestamp", "all_segments")
    val data_votaciones =
      spark.read
        .format("csv")
        .option("sep", "\t")
        .option("header", "true")
        .load("/datascience/custom/approvable_pgp_employed.csv")
        .withColumnRenamed("_c0", "device_id")
    // .withColumnRenamed("_c1", "cluster")

    val joint = data_audience
      .join(broadcast(data_votaciones), Seq("device_id"))
      .withColumn("all_segments", concat_ws(",", col("all_segments")))

    joint.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/amex_con_data_all")
  }

  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    getDataVotaciones(spark = spark) //, 31 , 1, "pitch_danone_full")

  }
}
