package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime
import org.apache.spark.sql.functions.{round, broadcast, col, abs, upper}
import org.apache.spark.sql.SaveMode
import scala.collection.Map

import main.scala.crossdevicer.CrossDevicer
import main.scala.matchers._

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.serializer.KryoRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator

/**
  Job Summary:
  * The goal of this job is to create an audiencie based on Points Of Interests (POIs). The method takes as input a time frame (be default, december 2018) and a dataset containing the POIs. This dataset should be already formatted in three columns segment|latitude|longitude (without the index) and with the latitude and longitude with point (".") as delimiter.
  * The method filters the safegraph data by country, and creates a geocode for both the POIs and the safegraph data. This geocode is used to match both datasets by performing a SQL join. The resulting rows will contain a user id, device type type, user latitude and longitude and POI id, latitude and longitude. Then the vincenty formula is used to calculate distance between the pairs of latitude and longitude.
   The method then proceeds to filter the users by a desired minimum distance returning a final dataset with user id and device type.
   The current method will provide the basis for future more customizable geolocation jobs.
  */
object Main {

  /**
    * This method returns a Map with all the parameters obtained from the JSON file.
    *
    * @param path_geo_json: JSON file name. This is the json where all the parameters are going to be extracted from.
    */
  def get_variables(
      spark: SparkSession,
      path_geo_json: String
  ): Map[String, String] = {
    // First we read the json file and store everything in a Map.
    val file =
      "hdfs://rely-hdfs/datascience/geo/geo_json/%s.json".format(path_geo_json)
    println("LOGGER JSON FILE: " + file)
    val df = spark.sqlContext.read.json(file)
    val columns = df.columns
    val query = df
      .collect()
      .map(fields => fields.getValuesMap[Any](fields.schema.fieldNames))
      .toList(0)

    // Now we parse the Map assigning default values.
    val max_radius =
      if (query.contains("max_radius") && Option(query("max_radius"))
            .getOrElse("")
            .toString
            .length > 0) query("max_radius").toString
      else "200"
    val country =
      if (query.contains("country") && Option(query("country"))
            .getOrElse("")
            .toString
            .length > 0) query("country").toString
      else "argentina"
    val poi_output_file =
      if (query.contains("output_file") && Option(query("output_file"))
            .getOrElse("")
            .toString
            .length > 0) query("output_file").toString
      else "custom"
    val path_to_pois =
      if (query.contains("path_to_pois") && Option(query("path_to_pois"))
            .getOrElse("")
            .toString
            .length > 0) query("path_to_pois").toString
      else ""
    val crossdevice =
      if (query.contains("crossdevice") && Option(query("crossdevice"))
            .getOrElse("")
            .toString
            .length > 0) query("crossdevice").toString
      else "false"
    val nDays =
      if (query.contains("nDays") && Option(query("nDays"))
            .getOrElse("")
            .toString
            .length > 0) query("nDays").toString
      else "30"
    val since =
      if (query.contains("since") && Option(query("since"))
            .getOrElse("")
            .toString
            .length > 0) query("since").toString
      else ""

    // Finally we construct the Map that is going to be returned
    val value_dictionary: Map[String, String] = Map(
      "max_radius" -> max_radius,
      "country" -> country,
      "poi_output_file" -> poi_output_file,
      "path_to_pois" -> path_to_pois,
      "crossdevice" -> crossdevice,
      "nDays" -> nDays,
      "since" -> since
    )

    println("LOGGER PARAMETERS:")
    println(s"""
    "max_radius" -> $max_radius,
    "country" -> $country,
    "poi_output_file" -> $poi_output_file,
    "path_to_pois" -> $path_to_pois,
    "crossdevice" -> $crossdevice,
    "nDays" -> $nDays,
    "since" -> $since""")
    value_dictionary
  }

  type OptionMap = Map[Symbol, Any]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--path_geo_json" :: value :: tail =>
        nextOption(map ++ Map('path_geo_json -> value.toString), tail)
      case "--geospark" :: value :: tail =>
        nextOption(map ++ Map('geospark -> value.toString), tail)
    }
  }

  def main(args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val path_geo_json =
      if (options.contains('path_geo_json)) options('path_geo_json).toString
      else ""
    val geospark = if (options.contains('geospark)) true else false

    // Start Spark Session based on the type of matcher that will be used.
    val spark =
      if (geospark)
        SparkSession
          .builder()
          .config("spark.serializer", classOf[KryoSerializer].getName)
          .config(
            "spark.kryo.registrator",
            classOf[GeoSparkKryoRegistrator].getName
          )
          // .config("geospark.global.index", "true")
          // .config("geospark.global.indextype", "rtree")
          .config("geospark.join.gridtype", "kdbtree")
          // .config("geospark.join.numpartition", 200)
          .appName("GeoSpark Matcher")
          .getOrCreate()
      else
        SparkSession.builder
          .appName("GeoCode Matcher")
          .getOrCreate()

    // Parsing parameters from json file.
    if (geospark) GeoSparkSQLRegistrator.registerAll(spark)
    val value_dictionary = get_variables(spark, path_geo_json)

    // Here we perform the join
    if (geospark) {
      GeoSparkMatcher.join(spark, value_dictionary)
    } else {
      POICrossDevicerJson.match_POI(spark, value_dictionary)
    }

    // Finally, we perform the cross-device if requested.
    if (value_dictionary("crossdevice") != "false")
      cross_device(spark, value_dictionary)

  }
}
