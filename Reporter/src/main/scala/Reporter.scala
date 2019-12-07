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
    * @param inverval: interval of days to be loaded.
    * @param id_partner: partner for which the data will be loaded.
    *
    * @return a DataFrame with the information coming from the data read.
  **/
  def getPipelineData(
      spark: SparkSession,
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
    val hdfs_files_reporter = (0 to nDays)
      .flatMap(
        day =>
          "/datascience/data_reporter2/day=%s%id_partner=%s"
            .format(
              from.plusDays(day).toString("yyyyMMdd"),
              id_partner
            )
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    println("Files to be processed")
    hdfs_files_reporter.foreach(file => println(file))

    // Load the data
    val path =
      "/datascience/data_%s/"
    val data = spark.read
      .option("basePath", path.format("reporter2"))
      .parquet(hdfs_files_reporter: _*)

    data
  }

  /**
    * This function takes as input the pipeline data, and returns a DataFrame with
    * all the data to be used by the Reporter.
    *
    * @param pipeline: DataFrame with all the pipeline data for a given partner.
    * @param query: query to be used to filter the rows that are not useful.
    * @param segment_column: column that will be used to overlap. It can be 'third_party' or 'first_party'.
    * @param first_party_segments: list of segments for the first party data. It can be empty.
    * @param overlap_segments: list of segments that will be overlapped with the first party data.
    *
    * Returns a DataFrame with three columns: device_id, first_party and segments (with the list of segments
    * to overlap).
    */
  def getDataset(
      spark: SparkSession,
      pipeline: DataFrame,
      query: String,
      segment_column: String,
      first_party_segments: Set[Int],
      overlap_segments: Set[Int]
  ): DataFrame = {
    // These UDF will be useful to filter the list of segments
    // that will be used in the report
    val filterSegments = udf(
      (segmentsCol: Seq[Int], segmentsToKeep: Set[Int]) =>
        if (segmentsToKeep.size > 0)
          segmentsCol.filter(s => segmentsToKeep.contains(s))
        else segmentsCol
    )
    // Filter the pipeline
    val data = pipeline
      .filter(query)

    val df =
      if (data.columns.length > 0) // If there is data for the partner
        data
          .withColumn("segments", col(segment_column))
          .withColumn(
            "first_party",
            filterSegments(col("first_party"), lit(first_party_segments))
          )
          .withColumn(
            "segments",
            filterSegments(col("segments"), lit(overlap_segments))
          )
          .select("device_id", "first_party", "segments")
      else
        spark.createDataFrame(
          spark.sparkContext.parallelize(
            Seq(Row(List("device_id", "first_party", "segments"): _*))
          ),
          StructType(
            List("device_id", "first_party", "segments")
              .map(c => StructField(c, StringType, true))
              .toArray
          )
        )

    df
  }

  /**
    * This function calculates the overlap between a set of users and a set of segments.
    * That is, it checks how many of such users have any of the given segments.
    * It returns a DataFrame with these stats (count and device_unique).
    *
    * @param dataset: dataset with all the users that have matched the query and that
    * will be overlaped with the list of segments.
    * @param split: boolean that tells us if we should explode the dataframe for each
    * of the first party segments.
    *
    * It returns a dataframe with three columns:
    *  - first party segment.
    *  - segment that has matched.
    *  - device_unique.
    */
  def getOverlap(
      dataset: DataFrame,
      split: Boolean
  ): DataFrame = {
    // This function is used to add a ficticious segment that will serve
    // as the total per id partner
    val addTotalIdUDF = udf(
      (segmentsCol: Seq[Int]) => segmentsCol :+ 0
    )

    // In this part we process the dataset so that we have all the segments per device,
    // the totals and filter to only keep the relevant segments
    val datasetWithSegments = dataset.na
      .drop()
      .withColumn("egments", addTotalIdUDF(col("segments")))
      .withColumn("segment", explode(col("segments")))
      .withColumn(
        "first_party",
        if (split) explode(col("first_party")) else lit("")
      )

    // Now we group by segment and obtain the two relevant metrics: count and device_unique
    val grouped = datasetWithSegments
      .groupBy("first_party", "segment")
      .agg(
        //countDistinct(col("device_id")) as "device_unique"
        approx_count_distinct(col("device_id"), rsd = 0.02) as "device_unique"
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
    val segments = jsonContent("datasource").split(",").map(_.toInt).toSeq :+ 0
    val firstParty = jsonContent("segments")
    val segmentFilter = jsonContent("segmentsFilter").split(",").toList
    val split = jsonContent("split")
    val partnerId = jsonContent("partnerId")
    val segmentsQuery = segmentFilter
      .map(s => "array_contains(first_party, %s)".format(s))
      .mkString(" OR ")
    var query = "id_partner = '%s'".format(partnerId)
    if (segmentsQuery.size > 0) {
      query = query + "(%s)".format(segmentsQuery)
    }
    val segment_column =
      if (jsonContent("report_subtype") == "thirdparty") "third_party"
      else "first_party"

    // Then we obtain the dataset and the overlap
    val pipeline = getPipelineData(
      spark,
      interval,
      partnerId
    )
    val dataset = getDataset(
      spark,
      pipeline,
      query,
      segment_column,
      segmentFilter.map(_.toInt).toSet,
      segments.map(_.toInt).toSet
    )
    val overlap = getOverlap(dataset, (split == "1" || split == "true"))

    // If there is a split, then we have to add the field firstParty as a column
    val report = overlap //if (split == "1" || split == "true") {
    //   overlap
    //     .withColumn("first_party", lit(firstParty))
    //     .select("first_party", "segment", "count", "device_unique")
    // } else {
    //   overlap.select("segment", "count", "device_unique")
    // }

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

      for (jsonContent <- jsonContents.slice(0, 1)) { // WATCH OUT, THIS IS HARDCODED
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

  type OptionMap = Map[Symbol, Int]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--nFiles" :: value :: tail =>
        nextOption(map ++ Map('nFiles -> value.toInt), tail)
    }
  }

  def main(args: Array[String]) {
    // Parse the parameters
    // val options = nextOption(Map(), args.toList)
    // val nFiles = if (options.contains('nFiles)) options('nFiles) else 10
    val nFiles = 5

    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("Spark devicer")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .getOrCreate()

    // Get the json files to be processed
    val path = "/datascience/reporter/to_process"
    val files = Utils.getQueryFiles(spark, path).slice(0, nFiles)

    files.foreach(file => processFile(spark, file))
  }
}
