package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions.{
  explode,
  desc,
  lit,
  size,
  concat,
  col,
  concat_ws,
  collect_list,
  udf,
  broadcast
}
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs._

/**
  * This function generates the dataset requested by Publicis. Basically, it takes the information from the eventqueue and then
  * it gets all the organic and modelled segments for every user. It removes the duplicates, keeping the segments for which the date is
  * the lowest. Finally, it creates a set of files in bz format, with a specific layout.
  * Layout:
            {
              "rtgtly_uid": "0000a2c8-661b-42cd-9773-d70efde26d9f",
              "segids": [
                  {"segid": "735", "segmentstartdate": 20190219},
                  {"segid": "567", "segmentstartdate": 20190219},
                  {"segid": "561", "segmentstartdate": 20190219},
                  {"segid": "570", "segmentstartdate": 20190219},
                  {"segid": "1348", "segmentstartdate": 20190219},
                  {"segid": "m_51", "segmentstartdate": 20190209},
                  {"segid": "m_129", " segmentstartdate ": 20190209}
              ]
            }
  *
  * @param spark: Spark Session that will be used to load all the data.
  * @param ndays: number of days to be considered to generate the dataset.
  * @param runType: it can be either full (for more than 1 day) or incr (for only one day).
  *
  * The resulting dataframe is going to be stored in hdfs://rely-hdfs/datascience/data_publicis/memb/
 **/
object generateOrganic {

  /**
    * This function returns a DataFrame with the segment triplets data.
    *
    * @param spark: Spark Session that will be used to load the data.
    * @param nDays: number of days to be loaded for the pipeline. If -1,
    * then it loads the whole pipeline (120 days).
    * @param from: number of days to be skipped from now into the past.
    * @param path: path from which the data is loaded.
    *
    * Returns a DataFrame with the following columns:
    *   - device_id
    *   - feature: this is the segment id
    *   - count: number of times the device_id visited the segment for a day.
    *   - id_partner: id_partner from which the visit was obtained.
    *   - country
    */
  def getDataTriplets(
      spark: SparkSession,
      nDays: Int = -1,
      from: Int = 1,
      path: String = "/datascience/data_triplets/segments/"
  ): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val df = if (nDays > 0) {
      // read files from dates
      val format = "yyyyMMdd"
      val endDate = DateTime.now.minusDays(from)
      val days =
        (0 until nDays.toInt)
          .map(endDate.minusDays(_))
          .map(_.toString(format))
      // Now we obtain the list of hdfs folders to be read
      val hdfs_files = days
        .map(day => path + "/day=%s/".format(day))
        .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      spark.read.option("basePath", path).parquet(hdfs_files: _*)
    } else {
      // read all date files
      spark.read.load(path)
    }
    df
  }

  def generate_organic(
      spark: SparkSession,
      ndays: Int,
      runType: String = "full",
      from: Int = 1
  ) {
    // Setting all the meta-classes that will be used to work with the data and file systems
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    /// This is the list of days that will be used to get the data from
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(from)

    println(
      "PUBLICIS LOGGER:\n  - From: %s\n  - ndays: %s\n  - Start date: %s"
        .format(from, ndays, start)
    )

    val days =
      (0 until ndays).map(start.plusDays(_)).map(_.toString(format))

    // This function takes all the segments and append the "m_" if the event_type is xp.
    val udfGetSegments = udf(
      (segments: Seq[Int], event_type: String) =>
        segments
          .map(s => "%s%s".format((if (event_type == "xp") "m_" else ""), s))
          .toSeq
    )

    /// Once we have the list of days, we can load it into memory
    val dfs = days.reverse
      .map(
        x =>
          spark.read
            .option("basePath", "/datascience/data_audiences_streaming/")
            .parquet("/datascience/data_audiences_streaming/hour=%s*".format(x))
            .filter("country = 'MX'")
            .withColumn("day", lit(x))
            .select("device_id", "day", "segments", "event_type")
            .na
            .drop()
            .withColumn(
              "segments",
              udfGetSegments(col("segments"), col("event_type"))
            )
            .select("device_id", "day", "segments")
      )
    val df = dfs.reduce((df1, df2) => df1.union(df2))

    /// Now we load and format the taxonomy
    val taxo_general = spark.read
      .format("csv")
      .option("sep", ",")
      .option("header", "True")
      .load("/datascience/data_publicis/taxonomy_publicis.csv")
      .select("Segment ID")
      .rdd
      .map(row => row(0))
      .collect()

    /// Given the fact that this is a very small list, we can broadcast it
    val taxo_general_b = sc.broadcast(taxo_general)

    /// This function filter out all the segments that don't belong to the general taxonomy.
    val udfGralSegments = udf(
      (segments: Seq[String]) =>
        segments.filter(
          segment => taxo_general_b.value.contains(segment.replace("m_", ""))
        )
    )
    /// Given a list of segments and a day, this function generates a list of tuples of the form (segment, day)
    val udfAddDay = udf(
      (segments: Seq[String], day: String) =>
        segments.map(segment => (segment, day))
    )
    // This UDF takes a list of list, where the final element is a Row object. Each Row actually represents
    // the (segment, day) tuple. Basically, this function flattens the list of lists into a single list of tuples.
    val udfFlattenLists = udf(
      (listOfLists: Seq[Seq[Row]]) =>
        listOfLists.flatMap(
          list =>
            list.map(
              row => (row(0).asInstanceOf[String], row(1).asInstanceOf[String])
            )
        )
    )
    // This function removes the duplicated tuples, keeping the tuples that have the lowest day.
    val udfDropDuplicates = udf(
      (segments: Seq[Row]) =>
        //"[%s]".format(
        segments
          .map(
            row => (row(0).asInstanceOf[String], row(1).asInstanceOf[String])
          )
          .groupBy(row => row._1)
          .map(row => row._2.sorted.last)
          .toList
          .map(
            tuple => Map("segid" -> tuple._1, "segmentstartdate" -> tuple._2)
            // """{"segid": "%s", "segmentstartdate": %s}"""
            //  .format(tuple._1, tuple._2)
          )
          .toSeq
      //.mkString(", ")
      //)
    )

    // Here we process the data that will be sent.
    val userSegments = df
      .withColumn("gral_segments", udfGralSegments(col("segments"))) // obtain only gral segment list
      .filter(size(col("gral_segments")) > 0) // remove the users with no gral_segments
      .withColumn("gral_segments", udfAddDay(col("gral_segments"), col("day"))) // add the day to every segment
      .groupBy("device_id")
      .agg(collect_list("gral_segments") as "gral_segments") // obtain the list of list of segments with day
      .withColumn("gral_segments", udfFlattenLists(col("gral_segments"))) // flatten the list of lists into a single list
      .withColumn("segids", udfDropDuplicates(col("gral_segments"))) // remove duplicates from the list
      .withColumnRenamed("device_id", "rtgtly_uid")
      .select("rtgtly_uid", "segids")

    // Last step is to store the data in the format required (.tsv.bz)
    val pathToJson = "hdfs://rely-hdfs/datascience/data_publicis/memb/%s/dt=%s"
      .format(runType, DateTime.now.minusDays(from).toString("yyyyMMdd"))

    userSegments.write
      .format("json")
      .option("compression", "bzip2")
      .option("quote", "")
      .option("escape", "")
      //.option("sep", "\t")
      //.option("header", true)
      .mode(SaveMode.Overwrite)
      .save(pathToJson)

    // Finally we rename all the generated files to stick to the format requested.
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    val files = hdfs.listStatus(new Path(pathToJson))
    val originalPath = files.map(_.getPath())

    originalPath.par
      .filter(!_.toString.contains("_SUCCESS"))
      .foreach(
        e =>
          hdfs.rename(
            e,
            new Path(
              pathToJson + "/retargetly_MX_memb_%s_%s_%s.json.bz2".format(
                runType,
                e.toString.split("/").last.split("-")(1),
                DateTime.now.minusDays(from).toString("yyyyMMdd")
              )
            )
          )
      )
  }
  def main(args: Array[String]) {
    /// Configuracion spark
    val spark =
      SparkSession.builder.appName("Generate Organic Data").getOrCreate()

    /// Parseo de parametros
    val ndays = if (args.length > 0) args(0).toInt else 30
    val from = if (args.length > 1) args(1).toInt else 1

    generate_organic(spark, ndays, "full", from)

  }
}
