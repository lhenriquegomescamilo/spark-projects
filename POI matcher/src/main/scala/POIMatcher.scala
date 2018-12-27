package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.joda.time.DateTime
import org.apache.spark.sql.functions.{round, broadcast, col, abs}
import org.apache.spark.sql.SaveMode


object POIMatcher {
  def get_safegraph_data(spark: SparkSession, days: Integer) = {
    //loading user files with geolocation, added drop duplicates to remove users who are detected in the same location
    // This is the country we are going to filter on
    val country = "mexico"
    // Here we load the data, eliminate the duplicates so that the following computations are faster, and select a subset of the columns
    // Also we generate a new column call 'geocode' that will be used for the join
    val df_safegraph = spark.read.option("header", "true").csv("/data/geo/safegraph/2018/12/*/*.gz")
                                  .dropDuplicates("ad_id","latitude","longitude")
                                  .filter("country = '%s'".format(country))
                                  .select("ad_id", "id_type", "latitude", "longitude")
                                  .withColumnRenamed("latitude", "latitude_user")
                                  .withColumnRenamed("longitude", "longitude_user")
                                  .withColumn("geocode", ((abs(col("latitude_user").cast("float"))*10).cast("int")*10000)+(abs(col("longitude_user").cast("float")*100).cast("int")))

    df_safegraph
  }

  def get_POI_coordinates(spark: SparkSession, file_name: String) = {
    // Loading POIs. The format of the file is Name, Latitude, Longitude
    val df_pois = spark.read.option("header", "true").option("delimiter", ",").csv(file_name)

    //creating geocodes for bus stops
    val df_pois_parsed = df_pois.withColumn("geocode", ((abs(col("latitude").cast("float"))*10).cast("int")*10000)+(abs(col("longitude").cast("float")*100).cast("int")))

    // Here we rename the columns
    val columnsRenamed_poi = Seq("name", "latitude_poi", "longitude_poi", "geocode")

    //renaming columns based on list
    val df_pois_final = df_pois_parsed.toDF(columnsRenamed_poi: _*)

    df_pois_final
  }

  def match_POI(spark: SparkSession, safegraph_days: Integer, POI_file_name: String, output_file: String) = {
    val df_users = get_safegraph_data(spark, safegraph_days)
    val df_pois_final = get_POI_coordinates(spark, POI_file_name)

    //joining datasets by geocode (added broadcast to force..broadcasting)
    val joint = df_users.join(broadcast(df_pois_final),Seq("geocode")).
        withColumn("longitude_poi", round(col("longitude_poi").cast("float"),4)).
        withColumn("latitude_poi", round(col("latitude_poi").cast("float"),4)).
        withColumn("longitude_user", round(col("longitude_user").cast("float"),4)).
        withColumn("latitude_user", round(col("latitude_user").cast("float"),4))



    //using vincenty formula to calculate distance between user/device location and bus stop
    joint.createOrReplaceTempView("joint")
    val query = """SELECT ad_id,name,id_type
                FROM joint 
                WHERE ((1000*111.045)*DEGREES(ACOS(COS(RADIANS(latitude_user)) * COS(RADIANS(latitude_poi)) *
                COS(RADIANS(longitude_user) - RADIANS(longitude_poi)) +
                SIN(RADIANS(latitude_user)) * SIN(RADIANS(latitude_poi))))) < 50.0"""

    //storing result
    val sqlDF = spark.sql(query)
    sqlDF.write.format("csv").option("sep", "\t").mode(SaveMode.Overwrite).save(output_file)
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("audience generator by keywords").getOrCreate()
    val sc = spark.sparkContext

    val safegraph_days = 30
    val POI_file_name = "hdfs://rely-hdfs/datascience/geo/poi_test.csv"
    val output_file = "/datascience/geo/MX/specific_POIs"

    match_POI(spark, safegraph_days, POI_file_name, output_file)
  }
}