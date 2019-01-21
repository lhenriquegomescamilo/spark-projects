package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.joda.time.DateTime
import org.apache.spark.sql.functions.{round, broadcast, col, abs, to_date, to_timestamp, hour, date_format, from_unixtime,count, avg}
import org.apache.spark.sql.SaveMode


object HomeJobs {

   def get_safegraph_data(spark: SparkSession, nDays: Integer, country: String, since: Integer = 1) = {
    import spark.implicits._
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
      case "--UseType" :: value :: tail =>
        nextOption(map ++ Map('UseType -> value.toString), tail)
      case "--output" :: value :: tail =>
        nextOption(map ++ Map('output -> value.toString), tail)
    }
  }


  def get_homejobs(spark: SparkSession,safegraph_days: Integer,  country: String, HourFrom: Integer, HourTo: Integer, UseType:String, output_file: String) = {
    import spark.implicits._
    val df_users = get_safegraph_data(spark, safegraph_days, country)

    val geo_hour = df_users.select("ad_id", "id_type", "latitude_user", "longitude_user","utc_timestamp","geocode")
                                            .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
                                            .withColumn("Hour", date_format(col("Time"), "HH"))
                                                .filter(
                                                    if (UseType=="home") { 
                                                                col("Hour") >= HourFrom || col("Hour") <= HourTo 
                                                                            } 
                                                    else {
                                                          (col("Hour") <= HourFrom && col("Hour") >= HourTo ) && 
                                                                !date_format(col("Time"), "EEEE").isin(List("Saturday", "Sunday"):_*) })

    val df_count  = geo_hour.groupBy(col("ad_id"),col("geocode"))
                        .agg(count(col("latitude_user")).as("freq"),
                            round(avg(col("latitude_user")),4).as("avg_latitude"),
                            (round(avg(col("longitude_user")),4)).as("avg_longitude"))
                    .select("ad_id","freq","geocode","avg_latitude","avg_longitude")

     
    
       
    case class Record(ad_id: String, freq: BigInt, geocode: BigInt ,avg_latitude: Double, avg_longitude:Double)

    val dataset_users = df_count.as[Record].groupByKey(_.ad_id).reduceGroups((x, y) => if (x.freq > y.freq) x else y)

    val final_users = dataset_users.map(
                            row =>  (row._2.ad_id,
                                    row._2.freq,
                                    row._2.geocode,
                                    row._2.avg_latitude,
                                    row._2.avg_longitude )).toDF("ad_id","freq","geocode","avg_latitude","avg_longitude")





    final_users.write.format("csv").option("sep", "\t").mode(SaveMode.Overwrite).save(output_file)
  }

  def main(args: Array[String]) {
    
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val safegraph_days = if (options.contains('nDays)) options('nDays).toString.toInt else 30
    val HourFrom = if (options.contains('HourFrom)) options('HourFrom).toString.toInt else 19
    val HourTo = if (options.contains('HourTo)) options('HourTo).toString.toInt else 6
    val country = if (options.contains('country)) options('country).toString else "mexico"
    val UseType = if (options.contains('UseType)) options('UseType).toString else "home"
    val output_file = if (options.contains('output)) options('output).toString else ""

    // Start Spark Session
    val spark = SparkSession.builder.appName("HomeJobs creator").getOrCreate()
    import spark.implicits._
    

    
    //val output_file = "/datascience/geo/MX/specific_POIs"


    get_homejobs(spark, safegraph_days, country, HourFrom, HourTo, output_file)
  }
}

