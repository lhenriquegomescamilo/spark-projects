package main.scala

import org.apache.spark.sql._
import org.joda.time.{Days, DateTime}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object TestRules {

  def getEventqueueData(spark: SparkSession): DataFrame = {
    val data = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/data/eventqueue/2020/01/01/0000.tsv.gz")
      .filter("id_partner = 412")
    val columns =
      """device_id, id_partner, event_type, device_type, segments, first_party, all_segments, url, referer, 
                     search_keyword, tags, track_code, campaign_name, campaign_id, site_id, time,
                     placement_id, advertiser_name, advertiser_id, app_name, app_installed,
                     version, country, activable, share_data"""
        .replace("\n", "")
        .replace(" ", "")
        .split(",")
        .toList

    // List of columns that are arrays of strings
    val array_strings = "tags app_installed".split(" ").toSeq

    // List of columns that are arrays of integers
    val array_ints =
      "segments first_party all_segments"
        .split(" ")

    val ints =
      "id_partner activable"
        .split(" ")
        .toSeq

    // Now we transform the columns that are array of strings
    val withArrayStrings = array_strings.foldLeft(data)(
      (df, c) => df.withColumn(c, split(col(c), "\u0001"))
    )

    // We do the same with the columns that are integers
    val withInts = ints.foldLeft(withArrayStrings)(
      (df, c) => df.withColumn(c, col(c).cast("int"))
    )

    // Finally, we repeat the process with the columns that are array of integers
    val finalDF = array_ints
      .foldLeft(withInts)(
        (df, c) =>
          df.withColumn(c, split(col(c), "\u0001"))
            .withColumn(c, col(c).cast("array<int>"))
      )

    finalDF
  }

  def runTest(spark: SparkSession) = {
    import spark.implicits._

    val rules = spark.read
      .format("csv")
      .option("sep", "\t")
      .load("/data/metadata/dataset_rules.tsv")
      .withColumnRenamed("_c0", "id_partner")
      .withColumnRenamed("_c2", "rule")
      .withColumnRenamed("_c1", "segment")

    val finalDF = getEventqueueData(spark)
    finalDF.cache()

    // Partners that are part of the eventqueue
    val partners =
      finalDF.select("id_partner").collect().map(row => row(0).toString)
    val filtered_rules = rules.filter(col("id_partner").isin(partners: _*))
    filtered_rules.cache()

    val N = filtered_rules.count().toInt
    val batch_size = 100

    val queries = filtered_rules.rdd
      .take(N)
      .filter(row => !row(2).toString.contains("zMRgqo"))
      .map(
        row =>
          "id_partner = %s AND (%s)".format(
            row(0),
            scala.xml.Utility.escape(row(2).toString.replace("o'", "o\\'"))
          )
      )

    for (batch <- (0 to (N / batch_size).toInt)) {
      var queries_batch = ((0 until batch_size) zip queries.slice(
        batch * batch_size,
        (batch + 1) * batch_size
      ))
      queries_batch
        .map(
          t => {
            val df: DataFrame = try {
              finalDF
                .filter(t._2)
                .withColumn("segment", lit(batch * batch_size + t._1))
                .select("device_id", "segment")
            } catch {
              case e: Exception => {
                println("Failed on query: %s".format(t._2))
                Seq.empty[(String, String)].toDF("device_id", "segment")
              }
            }
            df
          }
        )
        .reduce((df1, df2) => df1.unionAll(df2))
        .distinct()
        .groupBy("device_id")
        .agg(collect_list("segment").as("segments"))
        .withColumn("segments", concat_ws(",", col("segments")))
        .write
        .format("csv")
        .mode("append")
        .save("/datascience/custom/test_rules")
    }
  }

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    runTest(spark)
  }
}
