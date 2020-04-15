package main.scala.homejobs

import main.scala.HomeJobsJr

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.joda.time.DateTime
import org.apache.spark.sql.functions.{round, broadcast, col, abs, to_date, to_timestamp, hour, date_format, from_unixtime,count, avg,lit,input_file_name}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.{Days, DateTime}
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}



case class Record(ad_id: String, id_type: String, freq: BigInt, geocode: BigInt ,avg_latitude: Double, avg_longitude:Double)

object HomeJobs {

  def get_safegraph_data(
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(value_dictionary("since").toInt)
    val days = (0 until value_dictionary("nDays").toInt)
      .map(end.minusDays(_))
      .map(_.toString(format))

    // Now we obtain the list of hdfs files to be read
    val path = "/datascience/geo/safegraph/"
    val hdfs_files = days
      .map(day => path +  "day=%s/country=%s/".format(day,value_dictionary("country")))
      .filter(
        path => fs.exists(new org.apache.hadoop.fs.Path(path))
      )
      .map(day => day + "*.snappy.parquet")

    // Finally we read, filter by country, rename the columns and return the data
    val df_safegraph = spark.read
      .option("header", "true")
      .parquet(hdfs_files: _*)
      .dropDuplicates("ad_id", "latitude", "longitude")
      .select("ad_id", "id_type", "latitude", "longitude", "utc_timestamp")
      .na.fill("android", Seq("id_type")) //esto es una cabeceada probabilística
      .withColumnRenamed("latitude", "latitude_user")
      .withColumnRenamed("longitude", "longitude_user")
      .withColumn(
        "geocode",
        ((abs(col("latitude_user").cast("float")) * 10)
          .cast("int") * 10000) + (abs(
          col("longitude_user").cast("float") * 100
        ).cast("int"))
      )
      .na.drop()

      
//
    df_safegraph //
  }


  type OptionMap = Map[Symbol, Any]

  /**
   * This method parses the parameters sent.
   
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
      case "--UseType" :: value :: tail =>
        nextOption(map ++ Map('UseType -> value.toString), tail)
      case "--output" :: value :: tail =>
        nextOption(map ++ Map('output -> value.toString), tail)
    }
  }
*/
/*
   safegraph_days: Integer, 
   country: String, 
   HourFrom: Integer, 
   HourTo: Integer, 
   UseType:String,
   output_file: String)
*/

  def get_homejobs(spark: SparkSession,value_dictionary: Map[String, String]) = {
    import spark.implicits._
    val df_users = get_safegraph_data(spark, value_dictionary)

    //dictionary for timezones
       val timezone = Map("argentina" -> "GMT-3",
                        "AR" -> "GMT-3",
                       "CL"->"GMT-3",
                       "CO"-> "GMT-5",
                       "mexico" -> "GMT-5",
                       "MX" -> "GMT-5",
                       "PE"-> "GMT-5",
                       "PY"->"GMT-4",
                       "UY"->"GMT-3")

    
    //setting timezone depending on country
    spark.conf.set("spark.sql.session.timeZone", timezone(value_dictionary("country")))


    
    val geo_hour = df_users.select("ad_id","id_type", "latitude_user", "longitude_user","utc_timestamp","geocode")
                                            .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
                                            .withColumn("Hour", date_format(col("Time"), "HH")).cast("Integer")
                                                .filter(
                                                    if (value_dictionary("UseType")=="home") { 
                                                                col("Hour") >= value_dictionary("HourFrom").toInt || col("Hour") <= value_dictionary("HourTo").toInt 
                                                                            } 
                                                    else {
                                                          (col("Hour") <= value_dictionary("HourFrom").toInt && col("Hour") >= value_dictionary("HourTo").toInt) && 
                                                                !date_format(col("Time"), "EEEE").isin(List("Saturday", "Sunday"):_*) })



    geo_hour
    .withColumn("HourFrom",lit(value_dictionary("HourFrom")))
    .withColumn("HourTo",lit(value_dictionary("HourTo")))
    .withColumn("UseType",lit(value_dictionary("UseType")))
    .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/%s_exploded".format(value_dictionary("output_file")))

    val final_users  = geo_hour.groupBy(col("ad_id"),col("id_type"),col("geocode"))
                        .agg(count(col("latitude_user")).as("freq"),
                            round(avg(col("latitude_user")),4).as("avg_latitude"),
                            (round(avg(col("longitude_user")),4)).as("avg_longitude"))
                    .select("ad_id","id_type","freq","geocode","avg_latitude","avg_longitude")
                    

     
    
       
    //case class Record(ad_id: String, freq: BigInt, geocode: BigInt ,avg_latitude: Double, avg_longitude:Double)
    /*
    val dataset_users = df_count.as[Record].groupByKey(_.ad_id).reduceGroups((x, y) => if (x.freq > y.freq) x else y)

    val final_users = dataset_users.map(
                            row =>  (row._2.ad_id,
                                    row._2.id_type,
                                    row._2.freq,
                                    row._2.geocode,
                                    row._2.avg_latitude,
                                    row._2.avg_longitude )).toDF("ad_id","id_type","freq","geocode","avg_latitude","avg_longitude")

    */


    final_users
    .write.format("csv")
      .option("header", true)
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/%s".format(value_dictionary("output_file")))
  }

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
    import org.apache.log4j.{Level, Logger}
    Logger.getRootLogger.setLevel(Level.WARN)
    
    val options = nextOption(Map(), args.toList)
    val path_geo_json =
      if (options.contains('path_geo_json)) options('path_geo_json).toString
      else ""

    // Start Spark Session
    val spark = SparkSession.builder
      .appName("home assgination by geocode")
      .getOrCreate()

    val value_dictionary = HomeJobsJr.get_variables(spark, path_geo_json)

    get_homejobs(spark, value_dictionary)
  }
}

