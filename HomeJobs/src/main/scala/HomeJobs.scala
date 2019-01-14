package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.joda.time.DateTime
import org.apache.spark.sql.functions.{round, broadcast, col, abs, to_date, to_timestamp, hour}
import org.apache.spark.sql.SaveMode

object HomeJobs {

  def get_safegraph_data(spark: SparkSession, nDays: Integer, country: String, since: Integer = 1, HourTo : Integer = 6, HourFrom : Integer = 19 ) = {

    val format = "yyyy/MM/dd"
    val end   = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    
    //dictionary for timezones
    val timezone = Map("argentina" -> "GMT-3", "mexico" -> "GMT-5")
    
    //setting timezone depending on country
    spark.conf.set("spark.sql.session.timeZone", timezone(country))

    // Now we obtain the list of hdfs folders to be read
    val path = "/data/geo/safegraph/"
    val hdfs_files = days.map(day => path+"%s/*.gz".format(day))
    val df_safegraph = spark.read.option("header", "true").csv(hdfs_files:_*)
                                  .dropDuplicates("ad_id","latitude","longitude")
                                  .filter("country = '%s'".format(country))
                                  .select("ad_id", "id_type", "latitude", "longitude","utc_timestamp")
                                                         .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp")))).withColumn("Hour", date_format(col("Time"), "HH")).withColumn("Weekday", date_format(col("Time"), "EEE")).filter(col("Hour") > HourFrom || col("Hour") < HourTo)
                                                       
                                  

    df_safegraph
  }



  def main(args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val safegraph_days = if (options.contains('nDays)) options('nDays).toString.toInt else 30
    val country = if (options.contains('country)) options('country).toString else "mexico"
    val output_file = if (options.contains('output)) options('output).toString else ""

    // Start Spark Session
    val spark = SparkSession.builder.appName("HomeJobs creator").getOrCreate()

    //val POI_file_name = "hdfs://rely-hdfs/datascience/geo/poi_test_2.csv"
    //val output_file = "/datascience/geo/MX/specific_POIs"

    match_POI(spark, safegraph_days, country, output_file)
  }
}

