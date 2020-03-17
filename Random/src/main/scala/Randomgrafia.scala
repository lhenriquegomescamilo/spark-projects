package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.fs.{FileSystem, Path}


object Randomgrafia {

def processURL(spark: SparkSession, dfURL: DataFrame, field: String = "url"): DataFrame = {
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
      //.withColumn(
      //  field,
      //  regexp_replace(col(field), "http.*://(.\\.)*(www\\.){0,1}", "")
      //)
      .withColumn(
        field,
        regexp_replace(col(field), "(\\?|#).*", "")
      )
      .drop("domain")
      .withColumn(field, lower(col(field)))
}

def getDataTripletsURL(
      spark: SparkSession,
      nDays: Int = 30,
      from: Int = 1,
      path: String = "/datascience/data_demo/data_urls/") = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // read files from dates
    val format = "yyyyMMdd"
    val endDate = DateTime.now.minusDays(from)
    val days = (0 until nDays.toInt).map(endDate.minusDays(_)).map(_.toString(format))
    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    var df = spark.read.option("basePath", path).parquet(hdfs_files: _*)
    
    df.select("url", "device_id", "segments")

  }
  
def getURLSegmentPred(
      spark: SparkSession,
      nDays: Int = 30,
      from: Int = 1,
      path: String = "/datascience/scraper/predictions/segments/lang=sp/") = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // read files from dates
    val format = "yyyyMMdd"
    val endDate = DateTime.now.minusDays(from)
    val days = (0 until nDays.toInt).map(endDate.minusDays(_)).map(_.toString(format))
    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    var df = spark.read.option("basePath", path).parquet(hdfs_files: _*)
    
    df.select("url", "iab_segments_pred", "day").dropDuplicates("url")
  }

  def queryDeviceCountByURLPredicted(spark: SparkSession) {
      var df_triplets = getDataTripletsURL(spark, 30,1)
      df_triplets  = processURL(spark, df_triplets)
      
      var df_url_pred = getURLSegmentPred(spark, 30, 1)

      df_triplets = df_triplets.join(df_url_pred, Seq("url"), "inner")

      df_triplets.groupBy("url")
          .agg(countDistinct(col("device_id")).as("devices"),
              first("segments").as("segments"),
              first("iab_segments_pred").as("iab_segments_pred"),
              first("day").as("day")
              )
          .write
          .mode("overwrite")
          .format("parquet")
          .partitionBy("day")
          .save("/datascience/custom/url_classifier/device_count/")
  }

  def main(args: Array[String]) {

    val spark = SparkSession.builder
        .appName("Run Randomgrafia")
        //.config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .getOrCreate()
    
    queryDeviceCountByURLPredicted(spark)
  }

}
