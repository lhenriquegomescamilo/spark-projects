package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.joda.time._
import org.apache.hadoop.fs.{FileSystem, Path}

object TestJoin {
  def getDataUrls(
      spark: SparkSession,
      nDays: Integer,
      since: Integer
  ): DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_demo/data_urls"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select("url")

    // processURLHTTP(df)
    //   .withColumn(
    //     "url",
    //     regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
    //   )
    //   .withColumn("url", regexp_replace(col("url"), "'", ""))
    df
  }

  def getAudienceData(spark: SparkSession, today: String): DataFrame = {
    val df = spark.read
      .option("basePath", "/datascience/data_audiences_streaming/")
      .parquet("/datascience/data_audiences_streaming/hour=%s*".format(today)) // We read the data
      .select(
        "device_id",
        "device_type",
        "url",
        "country"
      )
    processURLHTTP(df)
      .withColumn(
        "url",
        regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
      )
      .withColumn("url", regexp_replace(col("url"), "'", ""))
  }

  def processURLHTTP(dfURL: DataFrame, field: String = "url"): DataFrame = {
    // First of all, we get the domains, and filter out those ones that are very generic

    val generic_domains = List(
      "google",
      "doubleclick",
      "facebook",
      "messenger",
      "yahoo",
      "android",
      "android-app",
      "bing",
      "instagram",
      "cxpublic",
      "content",
      "cxense",
      "criteo",
      "outbrain",
      "flipboard",
      "googleapis",
      "googlequicksearchbox",
      "0_media",
      "provider",
      "parser",
      "downloads",
      "xlxx",
      "xvideo2",
      "coffetube"
    )
    val query_generic_domains = generic_domains
      .map(dom => "new_domain NOT LIKE '%" + dom + "%'")
      .mkString(" AND ")
    val filtered_domains = dfURL
      .selectExpr("*", "parse_url(%s, 'HOST') as new_domain".format(field))
      .filter(query_generic_domains)
    // Now we filter out the domains that are IPs
    val filtered_IPs = filtered_domains
      .withColumn(
        "new_domain",
        regexp_replace(col("new_domain"), "^([0-9]+\\.){3}[0-9]+$", "IP")
      )
      .filter("new_domain != 'IP'")
    // Now if the host belongs to Retargetly, then we will take the r_url field from the QS
    val retargetly_domains = filtered_IPs
      .filter("new_domain LIKE '%retargetly%'")
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
      .filter("new_domain LIKE '%ampproject%'")
      .withColumn(field, ampUDF(col(field)))
      .filter("length(%s)>0".format(field))
    // Now we union the filtered dfs with the rest of domains
    val non_filtered_domains = filtered_IPs.filter(
      "new_domain NOT LIKE '%retargetly%' AND new_domain NOT LIKE '%ampproject%'"
    )
    val filtered_retargetly = non_filtered_domains
      .unionAll(retargetly_domains)
      .unionAll(ampproject_domains)
    // Finally, we remove the querystring and protocol
    filtered_retargetly
      .withColumn(
        field,
        regexp_replace(col(field), "://(.\\.)*", "://")
      )
      .withColumn(
        field,
        regexp_replace(col(field), "(\\?|#).*", "")
      )
      .drop("new_domain")
      .withColumn(field, lower(col(field)))
      .withColumn(
        field,
        regexp_replace(col(field), "@", "_")
      )
  }

  def main(args: Array[String]) {
    /// Configuracion spark
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder
      .appName("keyword ingestion")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    // getAudienceData(spark, "20200126")
    //   .select("url")
    //   .distinct()
    //   .write
    //   .format("csv")
    //   .mode("overwrite")
    //   .save("/datascience/custom/urls_test_data_keywords")

    getDataUrls(spark, 1, 1)
      .select("url")
      .distinct()
      .write
      .format("csv")
      .mode("overwrite")
      .save("/datascience/custom/urls_test_data_keywords_urls")
  }

}
