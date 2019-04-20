package main.scala.matchers

import main.scala.Main

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.log4j.{Level, Logger}

import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import com.vividsolutions.jts.geom.{Coordinate, Geometry, Point, GeometryFactory}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.apache.spark.storage.StorageLevel

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.serializer.KryoRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator

object GeoSparkMatcher {

  /**
    * This method reads the safegraph data, selects the columns "ad_id" (device id), "id_type" (user id), "latitude", "longitude", creates a
    * geocode for each row and future spatial operations and finally removes duplicates users that were detected in the same
    * location (i.e. the same user in different lat long coordinates will be conserved, but the same user in same lat long coordinates will be dropped).
    *
    * @param spark: Spark session that will be used to load the data.
    * @param value_dictionary: Map that contains all the necessary information to run the match. The following fields are required:
    *        - nDays: number of days to be loaded from Safegraph data.
    *        - since: number of days to be skipped in Safegraph data.
    *        - country: country for which the Safegraph data is going to be extracted from.

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
    * @param value_dictionary: Map that contains all the necessary information to run the match. The following fields are required:
    *        - max_radius: maximum distance allowed in the distance join.
    *        - path_to_pois: path where the POIs are stored.
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
      .filter(c => c != "latitude" && c != "longitude" && c!="radius")
    val query_other_columns = other_columns
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
    FROM POIs""".format(query_other_columns)

    println("LOGGER POI query:\n" + query)

    var poisDf = spark.sql(query)
    (poisDf, other_columns)
  }

  /**
    * This method performs the vincenty distance calculation between the POIs and the Safegraph data for the selected country and days.
    * To do this, we perform a join between both dataframes using GeoSpark. This is useful when both dataframes are large, since GeoSpark
    * creates and index that will basically map the points into buckets based on the Geo position.
    *
    * @param spark: Spark session that will be used to load the data.
    * @param value_dictionary: Map that contains all the necessary information to run the match. The following fields are required:
    *        - poi_output_file: path where the result is going to be stored.
    *        - max_radius: maximum distance allowed in the distance join.
    *        - nDays: number of days to be loaded from Safegraph data.
    *        - since: number of days to be skipped in Safegraph data.
    *        - country: country for which the Safegraph data is going to be extracted from.
    *        - path_to_pois: path where the POIs are stored.
    *
    * @param return df_pois_final: dataframe created from the one provided by the user containing the POIS: contains the geocode and renamed columns.
    */
  def join(spark: SparkSession, value_dictionary: Map[String, String]) = {
    // First we load the data.
    val safegraphDf = get_safegraph_data(spark, value_dictionary)
    val poisResult = get_POI_coordinates(spark, value_dictionary)
    val poisDf = poisResult._1
    val other_columns = poisResult._2
    println("LOGGER: Other columns: "+other_columns.mkString(", "))

    // This is a tweak for performance.
    // TODO: pasar por parametro las reparticiones
    safegraphDf.repartition(1000).createOrReplaceTempView("safegraph")
    poisDf.repartition(10)
    // TODO: pasar por parametro si se quiere o no persistir
    poisDf.persist(StorageLevel.MEMORY_ONLY)
    poisDf.createOrReplaceTempView("poisPoints")

    // Useful function that will be used to extract the info out of the Geometry objects.
    val getUserData = (point: Geometry) =>
      Seq(Seq(point.asInstanceOf[Point].getX().toString), 
          Seq(point.asInstanceOf[Point].getY().toString), 
          point.getUserData().toString.replaceAll("\\s{1,}", ",").split(",").toSeq)
    spark.udf.register("getUserData", getUserData)

    // Here we perform the actual join.
    val poiQuery = (0 until other_columns.length).map(i => "POI[2][%s] as %s".format(i, other_columns(i))).mkString(", ")
    var distanceJoinDf = spark.sql(
      """SELECT safegraph[0][0] as longitude_user,
                safegraph[1][0] as latitude_user,
                safegraph[2][0] as device_id,
                safegraph[2][1] as device_type,
                safegraph[2][2] as timestamp,
                POI[0][0] as longitude_poi,
                POI[1][0] as latitude_poi,
                distance,
                %s
         FROM (SELECT getUserData(safegraph.pointshape) as safegraph, 
                      getUserData(poisPoints.pointshape) as POI, 
                      ST_Distance(safegraph.pointshape, poisPoints.pointshape) AS distance
               FROM safegraph, poisPoints
               WHERE ST_Distance(safegraph.pointshape, poisPoints.pointshape) < %s)"""
        .format(poiQuery, value_dictionary("max_radius"))
    )

    // Finally we store the results.
    distanceJoinDf.write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/%s".format(value_dictionary("poi_output_file")))
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
      .config("geospark.join.gridtype", "kdbtree")
      // .config("geospark.join.numpartition", 200)
      .appName("match_POI_geospark")
      .getOrCreate()

    // Initialize the variables
    GeoSparkSQLRegistrator.registerAll(spark)
    val value_dictionary = Main.get_variables(spark, path_geo_json)
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
