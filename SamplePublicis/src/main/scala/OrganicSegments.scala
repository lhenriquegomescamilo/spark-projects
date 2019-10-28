package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs._
import org.apache.spark.sql.expressions.Window

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
object OrganicSegments {

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

  def getSegmentsData(spark: SparkSession, ndays: Int, from: Int): DataFrame = {
    // Setting all the meta-classes that will be used to work with the data and file systems
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    /// This is the list of days that will be used to get the data from
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(from)

    println(
      "PUBLICIS LOGGER - Organic Data:\n  - From: %s\n  - ndays: %s\n  - Start date: %s"
        .format(from, ndays, start)
    )

    val days =
      (0 until ndays).map(start.plusDays(_)).map(_.toString(format))

    /// Once we have the list of days, we can load it into memory
    val dfs = days.reverse
      .map(
        x =>
          spark.read
            .option("basePath", "/datascience/data_triplets/segments/")
            .parquet("/datascience/data_triplets/segments/day=%s*".format(x))
            .filter("country = 'MX'")
            .withColumn("day", lit(x))
            .withColumnRenamed("feature", "segment")
            .select("device_id", "day", "segment")
            .na
            .drop()
      )
    val df = dfs.reduce((df1, df2) => df1.union(df2))

    df
  }

  def getModelledData(spark: SparkSession, ndays: Int, from: Int): DataFrame = {
    // Setting all the meta-classes that will be used to work with the data and file systems
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    /// This is the list of days that will be used to get the data from
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(from)

    println(
      "PUBLICIS LOGGER - Data Modelled:\n  - From: %s\n  - ndays: %s\n  - Start date: %s"
        .format(from, ndays, start)
    )

    val days =
      (0 until ndays).map(start.plusDays(_)).map(_.toString(format))

    println(
      days.filter(
        day =>
          fs.exists(
            new org.apache.hadoop.fs.Path(
              "/datascience/data_lookalike/expansion/day=%s/country=MX/"
                .format(day)
            )
          )
      )
    )

    /// Once we have the list of days, we can load it into memory
    val dfs = days.reverse
      .filter(
        day =>
          fs.exists(
            new org.apache.hadoop.fs.Path(
              "/datascience/data_lookalike/expansion/day=%s/country=MX/"
                .format(day)
            )
          )
      )
      .map(
        day =>
          spark.read
            .format("csv")
            .option("sep", "\t")
            .load(
              "/datascience/data_lookalike/expansion/day=%s/country=MX/".format(
                day
              )
            )
            .withColumn("day", lit(day))
            .withColumnRenamed("_c0", "device_id")
            .withColumnRenamed("_c1", "segment")
            .withColumn("segment", split(col("segment"), ","))
            .withColumn("segment", explode(col("segment")))
            .select("device_id", "day", "segment")
            .na
            .drop()
      )
    val df = dfs.reduce((df1, df2) => df1.union(df2))

    val mapping_xp = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/data_lookalike/mapping_xp_segments.csv")
      .withColumnRenamed("parentId", "segment")

    df.join(broadcast(mapping_xp), Seq("segment"))
      .drop("segment")
      .withColumnRenamed("segmentId", "segment")
      .select("device_id", "day", "segment")
  }

  def generate_organic(
      spark: SparkSession,
      ndays: Int,
      runType: String = "full",
      from: Int = 1
  ) {
    // Load segment & modelled data
    val organicData =
      getSegmentsData(spark, ndays, from).withColumn("prefix", lit(""))
    val modelledData =
      getModelledData(spark, ndays, from).withColumn("prefix", lit("m_"))
    val df = organicData.unionAll(modelledData)

    // Now we load and format the taxonomy
    val taxo_general = spark.read
      .format("csv")
      .option("sep", ",")
      .option("header", "True")
      .load("/datascience/data_publicis/taxonomy_publicis.csv")
      .select("Segment ID")
      .rdd
      .map(row => row(0).toString.toInt)
      .collect()
      .toArray

    // Given the fact that this is a very small list, we can broadcast it
    val sc = spark.sparkContext
    val taxo_general_b = sc.broadcast(taxo_general)

    // This window will be used to keep the largest day per segment, per device_id
    val w =
      Window.partitionBy("device_id", "segment").orderBy(col("day").desc())

    // This function constructs the map that will be then stored as json
    val udfMap = udf(
      (segments: Seq[String], days: Seq[String]) =>
        (segments zip days)
          .map(
            tuple => Map("segid" -> tuple._1, "segmentstartdate" -> tuple._2)
          )
          .toSeq
    )

    val userSegments = df
      // Filter out the segments that are not part of the Publicis taxonomy
      .filter(col("segment").isin(taxo_general_b.value: _*))
      .withColumn("segment", concat(col("prefix"), col("segment")))
      // Keep the largest date per segment, per device_id
      .withColumn("rn", row_number().over(w))
      .filter(col("rn") == 1)
      .select("device_id", "segment", "day")
      // Get list of segments per device
      .groupBy("device_id")
      .agg(collect_list("segment") as "segment", collect_list("day") as "day")
      // construct json-like list of segments per device
      .withColumn("segids", udfMap(col("segment"), col("day")))
      .withColumnRenamed("device_id", "rtgtly_uid")
      .select("rtgtly_uid", "segids")

    // Last step is to store the data in the format required (.tsv.bz)
    val pathToJson = "hdfs://rely-hdfs/datascience/data_publicis/memb2/%s/dt=%s"
      .format(runType, DateTime.now.minusDays(from).toString("yyyyMMdd"))

    userSegments.write
      .format("json")
      .option("compression", "bzip2")
      .option("quote", "")
      .option("escape", "")
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

    generate_organic(spark, ndays)

  }
}
