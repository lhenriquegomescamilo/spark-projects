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
object UpdateStartApp {

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
  def get_startapp_data(
      spark: SparkSession,
      nDays: Int,
      since: Int
  ): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays)
      .map(end.minusDays(_))
      .map(_.toString(format))

    // Now we obtain the list of hdfs files to be read
    val path = "/data/providers/Startapp_Geo/"
    val hdfs_files = days
      .flatMap(
        day =>
          (0 to 20).map(
            node =>
              path + "location_-_%s_-_startapp_location_%s16_v_soda_node00%s.tsv.gz"
                .format(
                  day,
                  day,
                  if (node >= 10) node
                  else "0" + node.toString
                )
          )
      )
      .filter(
        dayPath => fs.exists(new org.apache.hadoop.fs.Path(dayPath))
      )

    hdfs_files.foreach(println)

    // This is the Safegraph data schema
    val customSchema = StructType(
      Array(
        StructField("ad_id", StringType, true),
        StructField("id_type", StringType, true),
        StructField("country", StringType, true),
        StructField("latitude", DoubleType, true),
        StructField("longitude", DoubleType, true),
        StructField("horizontal_accuracy", FloatType, true),
        StructField("date", StringType, true)
      )
    )

    // Finally we read, filter by country, rename the columns and return the data
    val df_startapp = spark.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(customSchema)
      .load(hdfs_files: _*)
      .withColumn("geo_hash", lit("startapp"))
      .withColumn("utc_timestamp", unix_timestamp(col("date")))
      .withColumn(
        "day",
        regexp_replace(regexp_replace(col("date"), "-", ""), " .*", "")
      )
      .drop("date")
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

    // In the last step we write the data partitioned by day and country.
    df_startapp
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
    val df_startapp = get_startapp_data(spark, nDays, since)

    // Store the data in parquet format
    df_startapp.write
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
