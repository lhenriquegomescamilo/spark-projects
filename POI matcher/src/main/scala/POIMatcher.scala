package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.joda.time.DateTime
import org.apache.spark.sql.functions.{round, broadcast, col, abs}
import org.apache.spark.sql.SaveMode

/**
  Job Summary: 
 * The goal of this job is to create an audiencie based on Points Of Interests (POIs). The method takes as input a time frame (be default, december 2018) and a dataset containing the POIs. This dataset should be already formatted in three columns segment|latitude|longitude (without the index) and with the latitude and longitude with point (".") as delimiter. 
   * The method filters the safegraph data by country, and creates a geocode for both the POIs and the safegraph data. This geocode is used to match both datasets by performing a SQL join. The resulting rows will contain a user id, device type type, user latitude and longitude and POI id, latitude and longitude. Then the vincenty formula is used to calculate distance between the pairs of latitude and longitude.
   The method then proceeds to filter the users by a desired minimum distance returning a final dataset with user id and device type.

   The current method will provide the basis for future more customizable geolocation jobs. 
     */
object POIMatcher {
   
 
/**
This method reads the safegraph data, selects the columns "ad_id" (device id), "id_type" (user id), "latitude", "longitude", creates a geocode for each row and future spatial operations and finally removes duplicates users that were detected in the same location (i.e. the same user in different lat long coordinates will be conserved, but the same user in same lat long coordinates will be dropped).

   @param spark: Spark session that will be used to load the data.
   @param nDays: parameter to define the number of days. Currently not used, hardcoded to the whole month of december 2018. 
   @param country: country from with to filter the data, it is currently hardcoded to Mexico
   @return df_safegraph: dataframe created with the safegraph data, filtered by the desired country, extracting the columns user id, device id, latitude and longitude removing duplicate users that have repeated locations and with added geocode.

 */

  def get_safegraph_data(spark: SparkSession, nDays: Integer, country: String, since: Integer = 1) = {
    //loading user files with geolocation, added drop duplicates to remove users who are detected in the same location
    // Here we load the data, eliminate the duplicates so that the following computations are faster, and select a subset of the columns
    // Also we generate a new column call 'geocode' that will be used for the join
    val format = "yyyy/MM/dd"
    val end   = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    
    // Now we obtain the list of hdfs folders to be read
    val path = "/data/geo/safegraph/"
    val hdfs_files = days.map(day => path+"%s/*.gz".format(day))
    val df_safegraph = spark.read.option("header", "true").csv(hdfs_files:_*)
                                  .dropDuplicates("ad_id","latitude","longitude")
                                  .filter("country = '%s'".format(country))
                                  .select("ad_id", "id_type", "latitude", "longitude","utc_timestamp")
                                  .withColumnRenamed("latitude", "latitude_user")
                                  .withColumnRenamed("longitude", "longitude_user")
                                  .withColumn("geocode", ((abs(col("latitude_user").cast("float"))*10).cast("int")*10000)+(abs(col("longitude_user").cast("float")*100).cast("int")))

    df_safegraph
  }

/**
  This method reads the user provided POI dataset, adds a geocode for each POI and renames the columns. The file provided must be correctly formatted as described below.

   @param spark: Spark session that will be used to load the data.
   @param file_name: path of the dataset containing the POIs. Must be correctly formated as described (name|latitude|longitude (without the index), with the latitude and longitude with point (".") as decimal delimiter.)
   @param return df_pois_final: dataframe created from the one provided by the user containing the POIS: contains the geocode and renamed columns.   
     */

  def get_POI_coordinates(spark: SparkSession, file_name: String) = {
    // Loading POIs. The format of the file is Name, Latitude, Longitude
    val df_pois = spark.read.option("header", "true").option("delimiter", ",").csv(file_name)

    //creating geocodes the POIs
    val df_pois_parsed = df_pois.withColumn("geocode", ((abs(col("latitude").cast("float"))*10).cast("int")*10000)+(abs(col("longitude").cast("float")*100).cast("int")))
                                .withColumn("radius", (col("radius").cast("float")))

    // Here we rename the columns
    val columnsRenamed_poi = Seq("name", "latitude_poi", "longitude_poi", "radius", "geocode")

    //renaming columns based on list
    val df_pois_final = df_pois_parsed.toDF(columnsRenamed_poi: _*)

    df_pois_final
  }

/**
  This method performs the vincenty distance calculation between the POIs and the Safegraph data for the selected country and month.
  To do this, we perform a join between both dataframes using the geocode. The resulting dataframe has a row where both the latitude and longitude of the user and the poi are present. Then using the vincenty formula, a distance is obtained. Finaly, we filter it by a minium distance.
 
   @param spark: Spark session that will be used to load the data.
   @param file_name: path of the dataset containing the POIs. Must be correctly formated as described (name|latitude|longitude (without the index), with the latitude and longitude with point (".") as decimal delimiter.)
   @param return df_pois_final: dataframe created from the one provided by the user containing the POIS: contains the geocode and renamed columns.   
     */

  def match_POI(spark: SparkSession, safegraph_days: Integer, POI_file_name: String, country: String, output_file: String) = {
    val df_users = get_safegraph_data(spark, safegraph_days, country)
    val df_pois_final = get_POI_coordinates(spark, POI_file_name)

    //joining datasets by geocode (added broadcast to force..broadcasting)
    val joint = df_users.join(broadcast(df_pois_final),Seq("geocode")).
        withColumn("longitude_poi", round(col("longitude_poi").cast("float"),4)).
        withColumn("latitude_poi", round(col("latitude_poi").cast("float"),4)).
        withColumn("longitude_user", round(col("longitude_user").cast("float"),4)).
        withColumn("latitude_user", round(col("latitude_user").cast("float"),4))


    //using vincenty formula to calculate distance between user/device location and the POI
    //currently the distance is hardcoded to 50 m. 
    joint.createOrReplaceTempView("joint")
    val query = """SELECT ad_id,name,id_type,utc_timestamp,distance
                FROM (
                  SELECT ad_id, name, id_type, utc_timestamp, radius,((1000*111.045)*DEGREES(ACOS(COS(RADIANS(latitude_user)) * COS(RADIANS(latitude_poi)) *
                  COS(RADIANS(longitude_user) - RADIANS(longitude_poi)) +
                  SIN(RADIANS(latitude_user)) * SIN(RADIANS(latitude_poi))))) as distance
                  FROM joint 
                )
                WHERE distance < radius"""

    //storing result
    val sqlDF = spark.sql(query)
    val filtered = 
    sqlDF.write.format("csv").option("sep", "\t").mode(SaveMode.Overwrite).save(output_file)
  }

  type OptionMap = Map[Symbol, Any]

  /**
   * This method parses the parameters sent.
   */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--nDays" :: value :: tail =>
        nextOption(map ++ Map('nDays -> value.toInt), tail)
      case "--country" :: value :: tail =>
        nextOption(map ++ Map('country -> value.toString), tail)
      case "--poi_file" :: value :: tail =>
        nextOption(map ++ Map('poi_file -> value.toString), tail)
      case "--output" :: value :: tail =>
        nextOption(map ++ Map('output -> value.toString), tail)
    }
  }

  def main(args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val safegraph_days = if (options.contains('nDays)) options('nDays).toString.toInt else 30
    val country = if (options.contains('country)) options('country).toString else "mexico"
    val POI_file_name = if (options.contains('poi_file)) options('poi_file).toString else ""
    val output_file = if (options.contains('output)) options('output).toString else ""

    // Start Spark Session
    val spark = SparkSession.builder.appName("audience generator by keywords").getOrCreate()

    // chequear que el POI_file_name este especificado y el output_file tambien

    //val POI_file_name = "hdfs://rely-hdfs/datascience/geo/poi_test_2.csv"
    //val output_file = "/datascience/geo/MX/specific_POIs"

    match_POI(spark, safegraph_days, POI_file_name, country, output_file)
  }
}
