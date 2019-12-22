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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.Row

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object GetURLsForAudience {

  def processURL(dfURL: DataFrame, field: String = "url"): DataFrame = {
    // First of all, we get the domains, and filter out those ones that are very generic
    val generic_domains = List(
      "google",
      "facebook",
      "yahoo",
      "android",
      "bing",
      "instagram",
      "cxpublic",
      "criteo",
      "outbrain",
      "flipboard",
      "googleapis",
      "googlequicksearchbox"
    )
    val query_generic_domains = generic_domains
      .map(dom => "domain NOT LIKE '%" + dom + "%'")
      .mkString(" AND ")
    val filtered_domains = dfURL
      .selectExpr("*", "parse_url(%s, 'HOST') as domain".format(field))
      .filter(query_generic_domains)
    // Now we filter out the domains that are IPs
    val filtered_IPs = filtered_domains
      .withColumn(
        "domain",
        regexp_replace(col("domain"), "^([0-9]+\\.){3}[0-9]+$", "IP")
      )
      .filter("domain != 'IP'")
    // Now if the host belongs to Retargetly, then we will take the r_url field from the QS
    val retargetly_domains = filtered_IPs
      .filter("domain LIKE '%retargetly%'")
      .selectExpr(
        "*",
        "parse_url(%s, 'QUERY', 'r_url') as new_url".format(field)
      )
      .filter("new_url IS NOT NULL")
      .withColumn(field, col("new_url"))
      .drop("new_url")
    // Then we process the domains that come from ampprojects
    val pattern =
      """^([a-zA-Z0-9_\-]+).cdn.ampproject.org/?([a-z]/)*([a-zA-Z0-9_\-\/\.]+)?""".r
    def ampPatternReplace(url: String): String = {
      var result = ""
      if (url != null) {
        val matches = pattern.findAllIn(url).matchData.toList
        if (matches.length > 0) {
          val list = matches
            .map(
              m =>
                if (m.groupCount > 2) m.group(3)
                else if (m.groupCount > 0) m.group(1).replace("-", ".")
                else "a"
            )
            .toList
          result = list(0).toString
        }
      }
      result
    }
    val ampUDF = udf(ampPatternReplace _, StringType)
    val ampproject_domains = filtered_IPs
      .filter("domain LIKE '%ampproject%'")
      .withColumn(field, ampUDF(col(field)))
      .filter("length(%s)>0".format(field))
    // Now we union the filtered dfs with the rest of domains
    val non_filtered_domains = filtered_IPs.filter(
      "domain NOT LIKE '%retargetly%' AND domain NOT LIKE '%ampproject%'"
    )
    val filtered_retargetly = non_filtered_domains
      .unionAll(retargetly_domains)
      .unionAll(ampproject_domains)
    // Finally, we remove the querystring and protocol
    filtered_retargetly
      .withColumn(
        field,
        regexp_replace(col(field), "http.*://(.\\.)*(www\\.){0,1}", "")
      )
      .withColumn(
        field,
        regexp_replace(col(field), "(\\?|#).*", "")
      )
      .drop("domain")
      .withColumn(field, lower(col(field)))
  }

  def get_data_urls(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      country: String
  ): DataFrame = {
    /// Configuraciones de spark
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    /// Obtenemos la data de los ultimos ndays
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(since)

    val days =
      (0 until ndays).map(start.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_demo/data_urls/"
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val urls = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    processURL(dfURL = urls, field = "url")
    urls
  }

  /**
    * GET URL DATA FOR A GIVEN AUDIENCE
    */
  def getURLForAudience(spark: SparkSession, path: String, country: String) = {
    val audience = spark.read
      .option("sep", "\t")
      .format("csv")
      .load(path)
      .withColumnRenamed("_c1", "device_id")
      .withColumnRenamed("_c2", "ids")
      .drop("_c0")

    val urls =
      get_data_urls(spark, 60, 1, country).select(
        "device_id",
        "url",
        "event_type"
      )

    audience
      .join(urls, Seq("device_id"))
      .write
      .format("parquet")
      .mode("overwrite")
      .save("/datascience/custom/%s_urls".format(path.split("/").last))
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

    getURLForAudience(
      spark = spark,
      "/datascience/custom/ages_br_gt_bk.csv",
      "BR"
    )

  }
}
