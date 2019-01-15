package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.joda.time.DateTime
import org.apache.spark.sql.functions.{round, broadcast, col, abs, to_date, to_timestamp, hour, date_format, from_unixtime}
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
                                  .filter("country = '%s'".format(country))
                                  .select("ad_id", "id_type", "latitude", "longitude","utc_timestamp")
                                                         .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
                                                         .withColumn("Hour", date_format(col("Time"), "HH"))
                                                         .withColumn("Weekday", date_format(col("Time"), "EEE"))
                                                         .filter(col("Hour") > HourFrom || col("Hour") < HourTo)
                                                       
                                  

    df_safegraph
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
      case "--HourTo" :: value :: tail =>
        nextOption(map ++ Map('HourTo -> value.toInt), tail)
      case "--HourFrom" :: value :: tail =>
        nextOption(map ++ Map('HourFrom -> value.toInt), tail)
      case "--country" :: value :: tail =>
        nextOption(map ++ Map('country -> value.toString), tail)
      case "--output" :: value :: tail =>
        nextOption(map ++ Map('output -> value.toString), tail)
    }
  }


  def get_homejobs(spark: SparkSession,safegraph_days: Integer,  country: String, HourFrom: Integer, HourTo: Integer, output_file: String) = {
    val df_users = get_safegraph_data(spark, safegraph_days, country,HourFrom,HourTo)
    df_users.write.format("csv").option("sep", "\t").mode(SaveMode.Overwrite).save(output_file)
  }

  def main(args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val safegraph_days = if (options.contains('nDays)) options('nDays).toString.toInt else 30
    val HourFrom = if (options.contains('HourFrom)) options('HourFrom).toString.toInt else 19
    val HourTo = if (options.contains('HourTo)) options('HourTo).toString.toInt else 6
    val country = if (options.contains('country)) options('country).toString else "mexico"
    val output_file = if (options.contains('output)) options('output).toString else ""

    // Start Spark Session
    val spark = SparkSession.builder.appName("HomeJobs creator").getOrCreate()

    
    //val output_file = "/datascience/geo/MX/specific_POIs"


    get_homejobs(spark, safegraph_days, country, HourFrom, HourTo, output_file)
  }
}

