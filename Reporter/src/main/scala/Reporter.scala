package main.scala

import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._

object GetAudience {

  /**
    * This method returns a DataFrame with the data from the partner data pipeline, for the interval
    * of days specified. Basically, this method loads the given path as a base path, then it
    * also loads the every DataFrame for the days specified, and merges them as a single
    * DataFrame that will be returned.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param query: query to filter by id partner, date, and segments.
    *
    * @return a DataFrame with the information coming from the data read.
  **/
  def getDataset(
      spark: SparkSession,
      query: String
  ): DataFrame = {
    println("DEVICER LOG: PIPELINE ID PARTNERS")
    val path =
      "/datascience/data_partner_streaming/"

    val data = spark.read
      .load(path)
      .filter(query)

    // This is the list of columns to be allowed
    val columns =
      """device_id, id_partner, first_party, all_segments"""
        .replace("\n", "")
        .replace(" ", "")
        .split(",")

    val df =
      if (data.columns.length > 0)
        data
          .select(columns.head, columns.tail: _*)
      else
        spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(columns: _*))),
          StructType(columns.map(c => StructField(c, StringType, true)).toArray)
        )

    df
  }

  def getOverlap(
      spark: SparkSession,
      dataset: DataFrame,
      segments: Seq[Int]
  ): DataFrame = {
    // This function is used to add a ficticious segment that will serve
    // as the total per id partner
    val addTotalIdUDF = udf((segments: Seq[Int]) => segments :+ 2)

    // In this part we process the dataset so that we have all the segments per device,
    // the totals and filter to only keep the relevant segments
    val datasetWithSegments = dataset
      .withColumn("all_segments", addTotalIdUDF(col("all_segments")))
      .withColumn("segment", explode(col("all_segments")))
      .filter(col("segment").isin(segments: _*))

    // Now we group by segment and obtain the two relevant metrics: count and device_unique
    val grouped = datasetWithSegments
      .groupBy("segment")
      .agg(
        countDistinct(col("device_id")) as "device_unique",
        count("device_id") as "count"
      )

    grouped
  }

  //TODO agregar documentation
  def getQueryReport(spark: SparkSession, jsonContent: Map[String, String]) = {
    // TODO Agregar try y Catch
    // First of all we read all the parameters that are of interest
    val query = jsonContent("query")
    val segments = jsonContent("datasource").split(",").map(_.toInt).toSeq :+ 0
    val firstParty = jsonContent("segments")
    val split = jsonContent("split")

    // Then we obtain the dataset and the overlap
    val dataset = getDataset(spark, query)
    val overlap = getOverlap(spark, dataset, segments)

    // If there is a split, then we have to add the field firstParty as a column
    val report = if (split == "1") {
      overlap
        .withColumn("first_party", lit(firstParty))
        .select("first_party", "segment", "count", "device_unique")
    } else {
      overlap.select("segment", "count", "device_unique")
    }

    report.createOrReplaceTempView("report")
    val table = spark.table("report")

    table
      .repartition(1)
      .write
      .format("csv")
      .option("sep", ",")
      .save("/datascience/reporter/processed/test")
  }

  def main(args: Array[String]) {
    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("Spark devicer")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .getOrCreate()

    val testMap = Map(
      "query" -> "id_partner = 146 AND first_party IN (1453, 4309) AND (hour >= '2019102300' AND hour <= '2019112023')",
      "datasource" -> "2,3,129,61,59,26,32,250,430",
      "split" -> "0",
      "segments" -> "1453,4309"
    )

    getQueryReport(spark, testMap)
  }
}
