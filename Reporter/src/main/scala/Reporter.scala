package main.scala

import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.{DateTime, Days}
import org.joda.time.format.DateTimeFormat

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
      query: String,
      interval: Seq[String],
      id_partner: String
  ): DataFrame = {
    println("DEVICER LOG: PIPELINE ID PARTNERS")
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the list of days to be retrieved
    val from =
      DateTime.parse(interval(0), DateTimeFormat.forPattern("yyyyMMddHH"))
    val to =
      DateTime.parse(interval(1), DateTimeFormat.forPattern("yyyyMMddHH"))
    val nDays = Days.daysBetween(from, to).getDays()

    // Get list of valid days
    val hdfs_files = (0 to nDays)
      .flatMap(
        day =>
          (0 to 24).map(
            hour =>
              "/datascience/data_partner_streaming/hour=%s%02d/id_partner=%s"
                .format(from.toString("yyyyMMdd"), hour, id_partner)
          )
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    // Load the data
    val path =
      "/datascience/data_partner_streaming/"
    val data = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .filter(query)

    // This is the list of columns to be allowed
    val columns =
      """device_id, all_segments"""
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

  /**
    * This function calculates the overlap between a set of users and a set of segments.
    * That is, it checks how many of such users have any of the given segments.
    * It returns a DataFrame with these stats (count and device_unique).
    *
    * - spark: SparkSession that will be used to load and store the data.
    * - dataset: dataset with all the users that have matched the query and that
    * will be overlaped with the list of segments.
    * - segments: list of segments to be used for the report.
    *
    * It returns a dataframe with three columns:
    *  - segment that has matched.
    *  - device_unique
    *  - count
    */
  def getOverlap(
      spark: SparkSession,
      dataset: DataFrame,
      segments: Seq[Int]
  ): DataFrame = {
    // This function is used to add a ficticious segment that will serve
    // as the total per id partner
    val addTotalIdUDF = udf(
      (segments: Seq[Int]) =>
        segments :+ 0 //Option(segments).getOrElse(Seq(-1)) :+ 0
    )

    // In this part we process the dataset so that we have all the segments per device,
    // the totals and filter to only keep the relevant segments
    val datasetWithSegments = dataset
      .select("device_id", "all_segments")
      .na
      .drop()
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

  /**
    * Given a query and other parameters in a dictionary, this function downloads the
    * corresponding users (those that match with the given filter). After getting them,
    * it checks how many of them have the set of segments given in the dictionary. Finally,
    * it generates the 'overlap' report where it gives the count and device_unique for
    * each segment.
    *
    * - spark: Spark Session that will be used to read and store data in HDFS.
    * - jsonContent: map with all the parameters necessary to run the query. At least it
    * should contain the following fields: query, datasource, segments, split.
    * - name: file name that will be used to name the report as well.
    *
    * It stores the overlap report in /datascience/reporter/processed/name.
    */
  def getQueryReport(
      spark: SparkSession,
      jsonContent: Map[String, String],
      name: String
  ) = {
    // First of all we read all the parameters that are of interest
    val interval = jsonContent("interval").split(",").toSeq
    val query = jsonContent("query")
    val segments = jsonContent("datasource").split(",").map(_.toInt).toSeq :+ 0
    val firstParty = jsonContent("segments")
    val split = jsonContent("split")

    // Then we obtain the dataset and the overlap
    val dataset = getDataset(spark, query, interval, jsonContent("partnerId"))
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

  /**
    * This function takes as input a json file with one or more queries to be run.
    * It iterates over all of them, and for each one it generates the corresponding
    * report and meta file (if specified).
    *
    * - spark: Spark Session that will be used to load and store the data in HDFS.
    * - fileName: file's name to be processed.
    *
    * As a result it stores the result in /datascience/reporter/processed/fileName and
    * leaves the meta file in /datascience/ingester/ready/
    */
  def processFile(spark: SparkSession, fileName: String) = {
    println("LOG: Processing File..")
    // Moving the file to the folder in_progress
    Utils.moveFile("to_process/", "in_progress/", fileName)

    try {
      println("LOG: Reading File..")
      // Getting the data out of the file
      val jsonContents = Utils.getQueriesFromFile(
        spark,
        "/datascience/reporter/in_progress/" + fileName
      )

      for (jsonContent <- jsonContents) {
        println("LOG: Processing the following query:")
        jsonContent.foreach(t => println("\t%s -> %s".format(t._1, t._2)))
        // Then we export the report
        getQueryReport(spark, jsonContent, fileName.replace(".json", ""))
        println("LOG: Report finished..")
      }

      // Finally we move the file to done and generate the meta file if it is required
      println("Generating .meta File..")
      Utils.moveFile("in_progress/", "done/", fileName)
      if (jsonContents(0)("push") == "true") {
        Utils.generateMetaFile(fileName.replace(".json", ""), jsonContents(0))
      }
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
