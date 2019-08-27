package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime
import org.apache.spark.sql.functions.{round, broadcast, col, abs, upper}
import org.apache.spark.sql.SaveMode

import main.scala.crossdevicer.CrossDevicer
import main.scala.aggregators.Aggregations
import main.scala.matchers._

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.serializer.KryoRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.apache.log4j.{Level, Logger}

/**
  Job Summary:
  * The goal of this job is to create an audiencie based on Points Of Interests (POIs). The method takes as input a time frame (be default, december 2018) and a dataset containing the POIs. This dataset should be already formatted in three columns segment|latitude|longitude (without the index) and with the latitude and longitude with point (".") as delimiter.
  * The method filters the safegraph data by country, and creates a geocode for both the POIs and the safegraph data. This geocode is used to match both datasets by performing a SQL join. The resulting rows will contain a user id, device type type, user latitude and longitude and POI id, latitude and longitude. Then the vincenty formula is used to calculate distance between the pairs of latitude and longitude.
   The method then proceeds to filter the users by a desired minimum distance returning a final dataset with user id and device type.
   The current method will provide the basis for future more customizable geolocation jobs.
  */
object Geodevicer {

  Logger.getRootLogger.setLevel(Level.WARN)

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
      else "0"
    // 1 o 0, determines if we want to calculate the analytcis DataFrame
    val analytics_df =
      if (query.contains("analytics_df") && Option(query("analytics_df"))
            .getOrElse("")
            .toString
            .length > 0) query("analytics_df").toString
      else "0"

    // 1 o 0, determines if we want to calculate the DataFrame where we have all the stats for every POI.
    val map_df =
      if (query.contains("map_df") && Option(query("map_df"))
            .getOrElse("")
            .toString
            .length > 0) query("map_df").toString
      else "0"

    // Time thresholds to consider a user to be valid.
    val umbralmin =
      if (query.contains("umbralmin") && Option(query("umbralmin"))
            .getOrElse("")
            .toString
            .length > 0) query("umbralmin").toString
      else "0"
    val umbralmax =
      if (query.contains("umbralmax") && Option(query("umbralmax"))
            .getOrElse("")
            .toString
            .length > 0) query("umbralmax").toString
      else "150"

    // Distance threshold to consider a user to be valid.
    val umbraldist =
      if (query.contains("umbraldist") && Option(query("umbraldist"))
            .getOrElse("")
            .toString
            .length > 0) query("umbraldist").toString
      else "0"

    // Column that identifies every POI.
    val poi_column_name =
      if (query.contains("poi_column_name") && Option(query("poi_column_name"))
            .getOrElse("")
            .toString
            .length > 0) query("poi_column_name").toString
      else ""

    // Column that identifies the audience, if present.
    val audience_column_name =
      if (query.contains("audience_column_name") && Option(
            query("audience_column_name")
          ).getOrElse("")
            .toString
            .length > 0) query("audience_column_name").toString
      else "no_push"

    // Number of days to look up in data_keywords
    val web_days =
      if (query.contains("web_days") && Option(query("web_days"))
            .getOrElse("")
            .toString
            .length > 0) query("web_days").toString
      else "0"

// Column to aggregate the segment count for web behaviour from data_keywords
    val web_column =
      if (query.contains("web_column") && Option(query("web_column"))
            .getOrElse("")
            .toString
            .length > 0) query("web_column").toString
      else "0"

    // Column to activate polygon matching
    val polygon_input =
      if (query.contains("polygon_input") && Option(query("polygon_input"))
            .getOrElse("")
            .toString
            .length > 0) query("polygon_input").toString
      else "0"

    // Column to select old pipeline if desired
    val old_pipeline =
      if (query.contains("old_pipeline") && Option(query("old_pipeline"))
            .getOrElse("")
            .toString
            .length > 0) query("old_pipeline").toString
      else "0"

    //To process an atribution data
    val atribution_date =
      if (query.contains("atribution_date") && Option(query("atribution_date"))
            .getOrElse("")
            .toString
            .length > 0) query("atribution_date").toString
      else "0"

    //To process transports 
      val column_w_stop_list_id =
      if (query.contains("column_w_stop_list_id") && Option(query("column_w_stop_list_id"))
            .getOrElse("")
            .toString
            .length > 0) query("column_w_stop_list_id").toString
      else "0"

    val transport_min_ocurrence =
      if (query.contains("transport_min_ocurrence") && Option(query("transport_min_ocurrence"))
            .getOrElse("")
            .toString
            .length > 0) query("transport_min_ocurrence").toString
      else "0"

    val transport_min_distance =
      if (query.contains("transport_min_distance") && Option(query("transport_min_distance"))
            .getOrElse("")
            .toString
            .length > 0) query("transport_min_distance").toString
      else "0"
    
//this will filter the final dataset to be generated as an audience
  val min_frequency_of_detection =
      if (query.contains("min_frequency_of_detection") && Option(query("min_frequency_of_detection"))
            .getOrElse("")
            .toString
            .length > 0) query("min_frequency_of_detection").toString
      else "0"

    //this will filter the final dataset to be generated as an audience
  val filter_true_user =
      if (query.contains("filter_true_user") && Option(query("filter_true_user"))
            .getOrElse("")
            .toString
            .length > 0) query("filter_true_user").toString
      else "0"

     //this selects the repartitions to be used when broadcasting a file
  val repartition =
      if (query.contains("repartition") && Option(query("repartition"))
            .getOrElse("")
            .toString
            .length > 0) query("repartition").toString
      else "1"
    

    // Finally we construct the Map that is going to be returned
    val value_dictionary: Map[String, String] = Map(
      "max_radius" -> max_radius,
      "country" -> country,
      "poi_output_file" -> poi_output_file,
      "path_to_pois" -> path_to_pois,
      "crossdevice" -> crossdevice,
      "nDays" -> nDays,
      "since" -> since,
      "analytics_df" -> analytics_df,
      "map_df" -> map_df,
      "umbralmin" -> umbralmin,
      "umbralmax" -> umbralmax,
      "umbraldist" -> umbraldist,
      "poi_column_name" -> poi_column_name,
      "audience_column_name" -> audience_column_name,
      "web_days" -> web_days,
      "polygon_input" -> polygon_input,
      "old_pipeline" -> old_pipeline,
      "atribution_date" -> atribution_date,
    "column_w_stop_list_id" -> column_w_stop_list_id,
      "transport_min_ocurrence" -> transport_min_ocurrence,
      "transport_min_distance" -> transport_min_distance,
      "min_frequency_of_detection" -> min_frequency_of_detection,
    "filter_true_user" -> filter_true_user,
    "repartition" -> repartition)

    println("LOGGER PARAMETERS:")
    println(s"""
    "max_radius" -> $max_radius,
    "country" -> $country,
    "poi_output_file" -> $poi_output_file,
    "path_to_pois" -> $path_to_pois,
    "crossdevice" -> $crossdevice,
    "nDays" -> $nDays,
    "since" -> $since,
    "analytics_df" -> $analytics_df,
    "map_df" -> $map_df,
    "umbralmin" -> $umbralmin,
    "umbralmax" -> $umbralmax,
    "umbraldist" -> $umbraldist,
    "poi_column_name" -> $poi_column_name,
    "audience_column_name" -> $audience_column_name,
    "web_days" -> $web_days,
    "polygon_input"->$polygon_input,
    "old_pipeline"-> $old_pipeline,
    "atribution_date" -> $atribution_date,
    "column_w_stop_list_id" -> $column_w_stop_list_id,
    "transport_min_ocurrence" -> $transport_min_ocurrence,
    "transport_min_distance" -> $transport_min_distance,
    "min_frequency_of_detection" -> $min_frequency_of_detection,
    "filter_true_user" -> $filter_true_user,
    "repartition" -> $repartition""")
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
      case "--geospark" :: tail =>
        nextOption(map ++ Map('geospark -> true), tail)
    }
  }

  def main(args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val path_geo_json =
      if (options.contains('path_geo_json)) options('path_geo_json).toString
      else ""
    //val geospark = if (options.contains('geospark)) true else false

    // Start Spark Session based on the type of matcher that will be used.
    var spark =
      /*
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
       */
      SparkSession.builder
        .appName("GeoCode Matcher")
        .getOrCreate()

    // Parsing parameters from json file.
    //if (geospark) GeoSparkSQLRegistrator.registerAll(spark)
    val value_dictionary = get_variables(spark, path_geo_json)

    // Here we perform the join
    // if (geospark) {
    //  GeoSparkMatcher.join(spark, value_dictionary)
    //} else {

    if (value_dictionary("polygon_input") == "1") {

      //acá deberíamos activar geospark
      spark = SparkSession
        .builder()
        .config("spark.serializer", classOf[KryoSerializer].getName)
        .config(
          "spark.kryo.registrator",
          classOf[GeoSparkKryoRegistrator].getName
        )
        .config("geospark.join.gridtype", "kdbtree")
        // This is because the polygons (left side of join) are usually smaller in size than the Safegraph dataframes.
        //.config("geospark.join.indexbuildside", "right") 
        .appName("GeoSpark Matcher")
        .getOrCreate()

      GeoSparkSQLRegistrator.registerAll(spark)

      PolygonMatcher.match_Polygon(spark, value_dictionary)

      // If we need to calculate the aggregations, we do so as well.
      if (value_dictionary("analytics_df") == "1")
        Aggregations.userAggregateFromPolygon(spark, value_dictionary)

      if (value_dictionary("map_df") == "1")
        Aggregations.PolygonAggregate(spark, value_dictionary)

    } else {

      POICrossDevicerJson.match_POI(spark, value_dictionary)
      // If we need to calculate the aggregations, we do so as well.
      if (value_dictionary("analytics_df") == "1")
        Aggregations.userAggregate(spark, value_dictionary)
      if (value_dictionary("map_df") == "1")
        Aggregations.POIAggregate(spark, value_dictionary)

    }

    // Finally, we perform the cross-device if requested.
    if (value_dictionary("crossdevice") != "false" && value_dictionary(
          "crossdevice"
        ) != "0")
      CrossDevicer.cross_device(
        spark,
        value_dictionary,
        column_name = "device_id",
        header = "true"
      )

    if (value_dictionary("crossdevice") != "false" && value_dictionary(
          "crossdevice"
        ) != "0" && value_dictionary("map_df") == "1")
      Aggregations.POIAggregate_w_xd(spark, value_dictionary)

// Here we perform the attribution by date
  if (value_dictionary("atribution_date") != "0")
      Aggregations.create_audiences_from_attribution_date(
        spark,
        value_dictionary)
  
  if (value_dictionary("crossdevice") != "false" && 
    value_dictionary("crossdevice") != "0" && 
  value_dictionary("atribution_date") != "0") 

      CrossDevicer.cross_device(
        spark,
        value_dictionary,
        path = "/datascience/geo/audience/%s_att_date-%s".format(value_dictionary("poi_output_file"),atribute_day_name),
        column_name = "device_id",
        header = "true"
      )

    
    // Here we join with web behaviour
    if (value_dictionary("web_days").toInt > 0)
      Aggregations.get_segments_from_triplets(spark, value_dictionary)

  }
}
