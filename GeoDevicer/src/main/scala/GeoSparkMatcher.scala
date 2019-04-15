package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.log4j.{Level, Logger}

import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.apache.spark.storage.StorageLevel

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.serializer.KryoRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator

object GeoSparkMatcher {

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

  /**
    * This method reads the safegraph data, selects the columns "ad_id" (device id), "id_type" (user id), "latitude", "longitude", creates a
    * geocode for each row and future spatial operations and finally removes duplicates users that were detected in the same
    * location (i.e. the same user in different lat long coordinates will be conserved, but the same user in same lat long coordinates will be dropped).
    *
    * @param spark: Spark session that will be used to load the data.
    * @param value_dictionary: Map with all the parameters. In particular there are three parameters that has to be contained in the Map: country, since and nDays.
    *
    * @return df_safegraph: dataframe created with the safegraph data, filtered by the desired country, extracting the columns user id, device id, latitude and
    * longitude removing duplicate users that have repeated locations and with added geocode.
    */
  def get_safegraph_data(
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(value_dictionary("since").toInt)
    val days = (0 until value_dictionary("nDays").toInt)
      .map(end.minusDays(_))
      .map(_.toString(format))

    // Now we obtain the list of hdfs files to be read
    val path = "/data/geo/safegraph/"
    val hdfs_files = days
      .map(day => path + "%s/".format(day))
      .filter(
        path => fs.exists(new org.apache.hadoop.fs.Path("/data/geo/safegraph/"))
      )
      .map(day => day + "*.gz")

    // Finally we read, filter by country, rename the columns and return the data
    val df_safegraph = spark.read
      .option("header", "true")
      .csv(hdfs_files: _*)
      .filter("country = '%s'".format(value_dictionary("country")))
      .select("ad_id", "id_type", "latitude", "longitude", "utc_timestamp")

    df_safegraph.createOrReplaceTempView("data")
    var safegraphDf = spark
      .sql("""
          SELECT ST_Transform(ST_Point(CAST(data.longitude AS Decimal(24,20)), 
                                       CAST(data.latitude AS Decimal(24,20)), 
                                       data.ad_id,
                                       data.id_type,
                                       data.utc_timestamp),
                              "epsg:4326", 
                              "epsg:3857") AS pointshape
          FROM data
      """)

    safegraphDf
  }

  /**
    * This method reads the user provided POI dataset, and renames the columns. The file provided must be correctly formatted as described below.
    *
    * @param spark: Spark session that will be used to load the data.
    * @param file_name: path of the dataset containing the POIs. Must be correctly formated as described (name|latitude|longitude (without the index),
    * with the latitude and longitude with point (".") as decimal delimiter.)
    *
    * @param return df_pois_final: dataframe created from the one provided by the user containing the POIS: contains the geocode and renamed columns.
    */
  def get_POI_coordinates(
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {

    // Loading POIs. The format of the file is Name, Latitude, Longitude
    val df_pois = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv(value_dictionary("path_to_pois"))

    val other_columns = df_pois.columns
      .filter(c => c != "latitude" && c != "longitude")
      .map(c => "POIs." + c)
      .mkString(",")
    df_pois.createOrReplaceTempView("POIs")

    val query =
      """
    SELECT ST_Transform(ST_Point(CAST(POIs.longitude AS Decimal(24,20)), 
                                 CAST(POIs.latitude  AS Decimal(24,20)),
                                 %s),
                        "epsg:4326", 
                        "epsg:3857") AS pointshape
    FROM POIs""".format(other_columns)

    println("LOGGER POI query:\n" + query)

    var poisDf = spark.sql(query)
    poisDf
  }

  def join(spark: SparkSession, value_dictionary: Map[String, String]) = {
    val safegraphDf = get_safegraph_data(spark, value_dictionary)
    val poisDf = get_POI_coordinates(spark, value_dictionary)

    // TODO: pasar por parametro las reparticiones
    safegraphDf.createOrReplaceTempView("safegraph")
    poisDf.repartition(10)
    // TODO: pasar por parametro si se quiere o no persistir
    poisDf.persist(StorageLevel.MEMORY_ONLY)
    poisDf.createOrReplaceTempView("poisPoints")

    var distanceJoinDf = spark.sql(
      """select *, ST_Distance(safegraph.pointshape, poisPoints.pointshape) AS distance
      from safegraph, poisPoints
      where ST_Distance(safegraph.pointshape, poisPoints.pointshape) < %s"""
        .format(value_dictionary("max_radius"))
    )

    // TODO: Overwrite output
    distanceJoinDf.write.format("csv").save("/datascience/geo/%s".format(value_dictionary("poi_output_file")))
      // .rdd
      // .map(
      //   arr =>
      //     arr(0)
      //       .asInstanceOf[com.vividsolutions.jts.geom.Geometry]
      //       .getUserData()
      //       .toString
      //       .replaceAll("\\s{1,}", ",") + "," +
      //       arr(1)
      //         .asInstanceOf[com.vividsolutions.jts.geom.Geometry]
      //         .getUserData()
      //         .toString
      //         .replaceAll("\\s{1,}", ",")
      // )
      // .saveAsTextFile(
      //   "/datascience/geo/%s".format(value_dictionary("poi_output_file"))
      // )
    println("LOGGER: Results already stored.")
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
    }
  }

  def main(args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val path_geo_json =
      if (options.contains('path_geo_json)) options('path_geo_json).toString
      else ""

    // Start Spark Session
    val spark = SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config(
        "spark.kryo.registrator",
        classOf[GeoSparkKryoRegistrator].getName
      )
      // .config("geospark.global.index", "true")
      // .config("geospark.global.indextype", "rtree")
      // .config("geospark.join.gridtype", "kdbtree")
      // .config("geospark.join.numpartition", 200)
      .appName("match_POI_geospark")
      .getOrCreate()

    // Initialize the variables
    GeoSparkSQLRegistrator.registerAll(spark)
    val value_dictionary = get_variables(spark, path_geo_json)
    Logger.getRootLogger.setLevel(Level.WARN)

    // Now we remove the file if it exists already
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val outPutPath =
      "/datascience/geo/%s".format(value_dictionary("poi_output_file"))
    if (fs.exists(new Path(outPutPath)))
      fs.delete(new Path(outPutPath), true)

    // Finally we perform the GeoJoin
    join(spark, value_dictionary)
  }
}
