package main.scala

import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._

object Reporter {

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
    val addTotalIdUDF = udf((segments: Seq[Int]) => Option(segments).getOrElse(Seq(-1)) :+ 0)

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
  def getQueryReport(
      spark: SparkSession,
      jsonContent: Map[String, String],
      name: String
  ) = {
    // First of all we read all the parameters that are of interest
    val query = jsonContent("query")
    val segments = jsonContent("datasource").split(",").map(_.toInt).toSeq :+ 0
    val firstParty = jsonContent("segments")
    val split = jsonContent("split")

    // Then we obtain the dataset and the overlap
    val dataset = getDataset(spark, query)
    val overlap = getOverlap(spark, dataset, segments)

    // If there is a split, then we have to add the field firstParty as a column
    val report = if (split == "1" || split == "true") {
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
      .option("header", "true")
      .mode("append")
      .save("/datascience/reporter/processed/%s".format(name))
  }

  // TODO add documentation
  def processFile(spark: SparkSession, fileName: String) = {
    // Moving the file to the folder in_progress
    Utils.moveFile("to_process/", "in_progress/", fileName)

    try {
      // Getting the data out of the file
      val jsonContents = Utils.getQueriesFromFile(
        spark,
        "/datascience/reporter/in_progress/" + fileName
      )

      for (jsonContent <- jsonContents) {
        // Then we export the report
        getQueryReport(spark, jsonContent, fileName.replace(".json", ""))
      }

      // Finally we move the file to done
      Utils.moveFile("in_progress/", "done/", fileName)
    } catch {
      case e: Exception => {
        // In case it fails
        e.printStackTrace()
        Utils.moveFile("in_progress/", "errors/", fileName)
      }
    }

  }

  def main(args: Array[String]) {
    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("Spark devicer")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .getOrCreate()

    // Get the json files to be processed
    val path = "/datascience/reporter/to_process"
    val files = Utils.getQueryFiles(spark, path)

    files.foreach(file => processFile(spark, file))
  }
}
