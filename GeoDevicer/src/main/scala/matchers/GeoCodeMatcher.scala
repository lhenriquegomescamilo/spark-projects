package main.scala.matchers

import main.scala.Main

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import scala.collection.Map

/**
  Job Summary:
  * The goal of this job is to create an audiencie based on Points Of Interests (POIs). The method takes as input a time frame (be default, december 2018) and a dataset containing the POIs. This dataset should be already formatted in three columns segment|latitude|longitude (without the index) and with the latitude and longitude with point (".") as delimiter.
  * The method filters the safegraph data by country, and creates a geocode for both the POIs and the safegraph data. This geocode is used to match both datasets by performing a SQL join. The resulting rows will contain a user id, device type type, user latitude and longitude and POI id, latitude and longitude. Then the vincenty formula is used to calculate distance between the pairs of latitude and longitude.
   The method then proceeds to filter the users by a desired minimum distance returning a final dataset with user id and device type.
   The current method will provide the basis for future more customizable geolocation jobs.
  */
object POICrossDevicerJson {

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
      .dropDuplicates("ad_id", "latitude", "longitude")
      .filter("country = '%s'".format(value_dictionary("country")))
      .select("ad_id", "id_type", "latitude", "longitude", "utc_timestamp")
      .withColumnRenamed("latitude", "latitude_user")
      .withColumnRenamed("longitude", "longitude_user")
      .withColumn(
        "geocode",
        ((abs(col("latitude_user").cast("float")) * 10)
          .cast("int") * 10000) + (abs(
          col("longitude_user").cast("float") * 100
        ).cast("int"))
      )

    df_safegraph
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

    //creating geocodes,assigning radius and renaming columns
    val df_pois_parsed = df_pois
      .withColumn(
        "geocode",
        ((abs(col("latitude").cast("float")) * 10).cast("int") * 10000) + (abs(
          col("longitude").cast("float") * 100
        ).cast("int"))
      )
      .withColumnRenamed("latitude", "latitude_poi")
      .withColumnRenamed("longitude", "longitude_poi")

    if (df_pois_parsed.columns.contains("radius")) {
      val df_pois_final = df_pois_parsed

      df_pois_final
    } else {
      val df_pois_final = df_pois_parsed.withColumn(
        "radius",
        lit(value_dictionary("max_radius").toInt)
      )
      df_pois_final
    }

  }

  /**
    * This method performs the vincenty distance calculation between the POIs and the Safegraph data for the selected country and days.
    * To do this, we perform a join between both dataframes using the geocode. The resulting dataframe has a row where both the latitude and
    * longitude of the user and the poi are present. Then using the vincenty formula, a distance is obtained. Finaly, we filter it by a minium distance.
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
  def match_POI(spark: SparkSession, value_dictionary: Map[String, String]) = {

    val df_users = get_safegraph_data(spark, value_dictionary)
    val df_pois_final = get_POI_coordinates(spark, value_dictionary)
    val columns = df_pois_final.columns.filter(!List("latitude_poi", "longitude_poi", "radius", "geocode"))

    //joining datasets by geocode (added broadcast to force..broadcasting)
    val joint = df_users
      .join(broadcast(df_pois_final), Seq("geocode"))
      .withColumn("longitude_poi", round(col("longitude_poi").cast("float"), 4))
      .withColumn("latitude_poi", round(col("latitude_poi").cast("float"), 4))
      .withColumn(
        "longitude_user",
        round(col("longitude_user").cast("float"), 4)
      )
      .withColumn("latitude_user", round(col("latitude_user").cast("float"), 4))

    // Using vincenty formula to calculate distance between user/device location and the POI.
    joint.createOrReplaceTempView("joint")
    val query =
      """SELECT ad_id as device_id, 
                id_type as device_type, 
                utc_timestamp as timestamp, 
                latitude_user,
                longitude_user,
                longitude_poi,
                longitude_poi,
                %s,
                distance
            FROM (
              SELECT *,((1000*111.045)*DEGREES(ACOS(COS(RADIANS(latitude_user)) * COS(RADIANS(latitude_poi)) *
              COS(RADIANS(longitude_user) - RADIANS(longitude_poi)) +
              SIN(RADIANS(latitude_user)) * SIN(RADIANS(latitude_poi))))) as distance
              FROM joint 
            )
            WHERE distance < radius""".format(columns.mkString(","))

    // Storing result
    val sqlDF = spark.sql(query)

    // We want information about the process
    println(sqlDF.explain(extended = true))

    val filtered =
      sqlDF.write
        .format("csv")
        .option("sep", "\t")
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .save("/datascience/geo/%s".format(value_dictionary("poi_output_file")))
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
    val spark = SparkSession.builder
      .appName("audience generator by keywords")
      .getOrCreate()

    val value_dictionary = Main.get_variables(spark, path_geo_json)

    match_POI(spark, value_dictionary)
  }
}
