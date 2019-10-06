package main.scala.updaters

import main.scala.Geodevicer

import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode, Row}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}

import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import com.vividsolutions.jts.geom.{
  Coordinate,
  Geometry,
  Point,
  GeometryFactory
}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.apache.spark.storage.StorageLevel

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.serializer.KryoRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator

/**
  * This process takes the daily data that comes from SafeGraph and generates a new pipeline in
  * parquet, with the same set of columns.
  *
  * The resulting pipeline is stored in /datascience/geo/safegraph/.
  */
object UpdateSafeGraphData {

  /**
    * This method loads the data from GCBA and processes it, so that it has the same
    * schema as the SafeGraph pipeline.
    * - nDays: number of days to be read from GCBA.
    * - since: number of days to be skiped.
    *
    * As a result, this function returns a DataFrame with GCBA data. The DataFrame has the following
    * schema:
    *              "utc_timestamp": "long"
    *              "ad_id": "string"
    *              "id_type": "string"
    *              "geo_hash": "string"
    *              "latitude": "double"
    *              "longitude": "double"
    *              "horizontal_accuracy": "float"
    *              "country": "string"
    */
  def get_gcba_geo_data(
      spark: SparkSession,
      nDays: Int,
      since: Int
  ): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "dd_MM_yyyy"
    val format_day = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays)
      .map(end.minusDays(_))
      .map(day => (day, day.toString(format)))

    // Now we obtain the list of hdfs files to be read
    val path = "/datascience/geo/AR/"
    val hdfs_files = days
      .map(day => (day._1, path + "data_gcba_%s.csv/".format(day._2)))
      .filter(
        dayPath => fs.exists(new org.apache.hadoop.fs.Path(dayPath._2))
      )

    // Time format that will be used to transform the date to Unix time
    val time_format = "yyyy-MM-dd HH:mm:ss"

    if (hdfs_files.length > 0) {
      // Read the data

      val dfs = hdfs_files
        .map(
          hdfs_file =>
            spark.read
              .format("csv")
              .option("header", "true")
              .load(hdfs_file._2)
              .na
              .drop()
              // Now we define the day
              .withColumn("day", lit(hdfs_file._1.toString(format_day)))
        )
        .filter(
          df =>
            df.columns.contains("Device Id") && df.columns
              .contains("Lat/Lon") && df.columns.contains("timestamp")
        )
        .map(df => df.select("Device Id", "Lat/Lon", "timestamp", "day"))
      val gcba_df = dfs.reduce((df1, df2) => df1.union(df2))

      // Finally we process all the columns so that they match the safegraph schema
      gcba_df
        .withColumn("Lat/Lon", split(col("Lat/Lon"), ","))
        .withColumn("latitude", col("Lat/Lon").getItem(0).cast("double"))
        .withColumn("longitude", col("Lat/Lon").getItem(0).cast("double"))
        .withColumn("country", lit("argentina"))
        .withColumn("id_type", lit("maid"))
        .withColumn("geo_hash", lit("gcba"))
        .withColumn("horizontal_accuracy", lit(0f))
        .withColumnRenamed("Device Id", "ad_id")
        // Transform the date from format 2019-10-01T20:30:08.969Z to Unix time (in seconds)
        .withColumn(
          "utc_timestamp",
          when(
            col("timestamp").contains("T"),
            unix_timestamp(
              regexp_replace(
                regexp_replace(col("timestamp"), "T", " "),
                "\\..*Z",
                ""
              ),
              time_format
            )
          ).otherwise(col("timestamp").cast("long"))
        )
        .select(
          "utc_timestamp",
          "ad_id",
          "id_type",
          "geo_hash",
          "latitude",
          "longitude",
          "horizontal_accuracy",
          "country",
          "day"
        )
    } else {
      // If there are no files, then we create an empty dataframe.
      val schema =
        new StructType()
          .add("utc_timestamp", "long")
          .add("ad_id", "string")
          .add("id_type", "string")
          .add("geo_hash", "string")
          .add("latitude", "double")
          .add("longitude", "double")
          .add("horizontal_accuracy", "float")
          .add("country", "string")
          .add("day", "string")
      val empty_df = spark.createDataFrame(
        spark.sparkContext.emptyRDD[Row],
        schema
      )
      empty_df
    }

  }

  /**
    * This function takes as input the number of days to process (nDays) and the number of days
    * to skip (from), and generates a parquet pipeline reading the data from SafeGraph, partitioning by
    * day and by country.
    * - nDays: number of days to be read from SafeGraph.
    * - since: number of days to be skiped.
    *
    * As a result, this function returns a DataFrame with SafeGraph data. The DataFrame has the following
    * schema:
    *              "utc_timestamp": "long"
    *              "ad_id": "string"
    *              "id_type": "string"
    *              "geo_hash": "string"
    *              "latitude": "double"
    *              "longitude": "double"
    *              "horizontal_accuracy": "float"
    *              "country": "string"
    */
  def get_safegraph_data(
      spark: SparkSession,
      nDays: Int,
      since: Int
  ): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays)
      .map(end.minusDays(_))
      .map(_.toString(format))

    // Now we obtain the list of hdfs files to be read
    val path = "/data/geo/safegraph/"
    val hdfs_files = days
      .map(day => path + "%s/".format(day))
      .filter(
        dayPath => fs.exists(new org.apache.hadoop.fs.Path(dayPath))
      )

    // This is the Safegraph data schema
    val schema =
      new StructType()
        .add("utc_timestamp", "long")
        .add("ad_id", "string")
        .add("id_type", "string")
        .add("geo_hash", "string")
        .add("latitude", "double")
        .add("longitude", "double")
        .add("horizontal_accuracy", "float")
        .add("country", "string")

    // Finally we read, filter by country, rename the columns and return the data
    val dfs = hdfs_files.map(
      file =>
        spark.read
          .option("header", "true")
          .schema(schema)
          .csv(file)
          .withColumn(
            "day",
            lit(file.slice(file.length - 11, file.length).replace("/", "")) // Here we obtain the day from the file name.
          )
    )

    val df_safegraph = dfs.reduce((df1, df2) => df1.union(df2))

    // In the last step we write the data partitioned by day and country.
    df_safegraph
  }

  /**
    * This function reads all the sources of Geo data and stores the results in parquet format.
    * - nDays: number of days to be read from SafeGraph.
    * - since: number of days to be skiped.
    *
    * As a result, this function stores the pipeline in /datascience/geo/safegraph/, partitioned
    * by day and by country.
    */
  def storeData(spark: SparkSession, nDays: Int, since: Int) = {
    // Read data from SafeGraph
    val df_safegraph = get_safegraph_data(spark, nDays, since)
    df_safegraph.printSchema
    // Read data from GCBA
    val df_gcba = get_gcba_geo_data(spark, nDays, since)
    df_gcba.printSchema

    // Store the data in parquet format
    df_safegraph
      .unionAll(df_gcba)
      .write
      .format("parquet")
      .partitionBy("day", "country")
      .mode("append")
      .save("/datascience/geo/safegraph/")
  }

  type OptionMap = Map[Symbol, String]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--ndays" :: value :: tail =>
        nextOption(map ++ Map('ndays -> value.toString), tail)
      case "--from" :: value :: tail =>
        nextOption(map ++ Map('from -> value.toString), tail)
    }
  }

  def main(args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val from = if (options.contains('from)) options('from).toInt else 1
    val nDays =
      if (options.contains('ndays)) options('ndays).toInt else 1

    // Start Spark Session
    val spark = SparkSession
      .builder()
      .appName("match_POI_geospark")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    // Finally we perform the GeoJoin
    storeData(spark, nDays, from)
  }
}
