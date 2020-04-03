package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{round, broadcast, col, abs, to_date, to_timestamp, hour, date_format, from_unixtime,count, avg}
import org.apache.spark.sql.SaveMode
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.input_file_name




/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object distanceTraveled_AR_CABA_Radios {
  


def getDataPipeline(
      spark: SparkSession,
      path: String,
      nDays: String,
      since: String,
      country: String) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    //specifying country
    //val country_iso = "MX"
      
        // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day,country)) //
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    df
  }

def get_ua_segments(spark:SparkSession) = {

//
//val ua = spark.read.format("parquet")
//        .load("/datascience/data_useragents/day=*/country=AR")
 //       .filter("model != ''") //con esto filtramos los desktop
  //      .withColumn("device_id",upper(col("device_id")))
   //     .drop("user_agent","event_type","url")
    //    .dropDuplicates("device_id")



val ua = getDataPipeline(spark,"/datascience/data_useragents/","30","1","MX")
        .filter("model != ''") //con esto filtramos los desktop
        .withColumn("device_id",upper(col("device_id")))
        .drop("user_agent","event_type","url")
        .dropDuplicates("device_id")        
        //.filter("(country== 'AR') OR (country== 'CL') OR (country== 'MX')")

val segments = getDataPipeline(spark,"/datascience/data_triplets/segments/","15","1","MX")
              .withColumn("device_id",upper(col("device_id")))
              .groupBy("device_id").agg(concat_ws(",",collect_set("feature")) as "segments")

val joined = ua.join(segments,Seq("device_id"))
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/misc/ua_w_segments_30d_MX_II")

                                          }



def get_safegraph_data(
      spark: SparkSession,
      nDays: String,
      since: String,
      country: String
     
  ) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

   
    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt)
      .map(end.minusDays(_))
      .map(_.toString(format))
      

    // Now we obtain the list of hdfs files to be read
    val path = "/datascience/geo/safegraph/"
    val hdfs_files = days
      .map(day => path +  "day=%s/country=%s/".format(day,country))
      .filter(
        path => fs.exists(new org.apache.hadoop.fs.Path(path))
      )
      .map(day => day + "*.snappy.parquet")


    // Finally we read, filter by country, rename the columns and return the data
    val df_safegraph = spark.read
      .option("header", "true")
      .parquet(hdfs_files: _*)
      .dropDuplicates("ad_id", "latitude", "longitude")
      .withColumnRenamed("ad_id","device_id")
      .withColumnRenamed("id_type","device_type")
      .withColumn("device_id",upper(col("device_id")))

     df_safegraph                    
    
  }



  def get_safegraph_all_country(
      spark: SparkSession,
      nDays: String,
      since: String
     
  ) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

   
    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt)
      .map(end.minusDays(_))
      .map(_.toString(format))
      

    // Now we obtain the list of hdfs files to be read
    val path = "/datascience/geo/safegraph/"
    val hdfs_files = days
      .map(day => path +  "day=%s/".format(day))
      .filter(
        path => fs.exists(new org.apache.hadoop.fs.Path(path))
      )
      .map(day => day + "*/*.snappy.parquet")


    // Finally we read, filter by country, rename the columns and return the data
    val df_safegraph = spark.read
      .option("header", "true")
      .parquet(hdfs_files: _*)
      .withColumn("input",input_file_name)
      .withColumn("day",split(col("input"),"/").getItem(6))
      .withColumn("country",split(col("input"),"/").getItem(7))
      
     df_safegraph                    
    
  }



 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("Spark devicer").config("spark.sql.files.ignoreCorruptFiles", "true").getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)


//Esta función obtiene los geohashes los últimos 30 días y mira una desagregacióon por barrio para Argentina. 

val country = "argentina"

val timezone = Map("argentina" -> "GMT-3",
                       "mexico" -> "GMT-5",
                       "CL"->"GMT-3",
                       "CO"-> "GMT-5",
                       "PE"-> "GMT-5")
    
    //setting timezone depending on country
spark.conf.set("spark.sql.session.timeZone", timezone(country))

val today = (java.time.LocalDate.now).toString

val raw = get_safegraph_data(spark,"40","1",country)
.withColumnRenamed("ad_id","device_id")
.withColumn("device_id",lower(col("device_id")))
.withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
.withColumn("Day", date_format(col("Time"), "dd-MM-YY"))
.withColumn("Hour", date_format(col("Time"), "HH"))
.withColumn("geo_hash_7",substring(col("geo_hash"), 0, 7))


val geo_hash_visits = raw
.groupBy("device_id","Day","Hour","geo_hash_7").agg(count("utc_timestamp") as "detections")
.withColumn("country",lit(country))

val output_file = "/datascience/geo/Reports/GCBA/Coronavirus/%s/geohashes_by_user_hourly_%s".format(today,country)

geo_hash_visits
 .write
    .mode(SaveMode.Overwrite)
    .format("parquet")
    .option("header",true)
    .save(output_file)


///////////////barrios y radios censales

val geo_hash_table = spark.read.format("csv")
.option("header",true)
.option("delimiter",",")
.load("/datascience/geo/geohashes_tables/AR_CABA_GeoHash_to_Entity.csv")

//Levantamos la data y la pegamos a los barrios
val geo_labeled_users = spark.read.format("parquet")
.load(output_file)
.join(geo_hash_table,Seq("geo_hash_7"))

geo_labeled_users.persist()

//Agregamos por día
val output_file_tipo_2a = "/datascience/geo/Reports/GCBA/Coronavirus/%s/geohash_travel_barrio_radio_CLASE2_%s".format(today,country)

geo_labeled_users
.groupBy("BARRIO","RADIO","Day","device_id").agg(countDistinct("geo_hash_7") as "geo_hash_7")
.groupBy("BARRIO","RADIO","Day").agg(count("device_id") as "devices",avg("geo_hash_7") as "geo_hash_7_avg",stddev_pop("geo_hash_7") as "geo_hash_7_std")
.repartition(1)
.write
.mode(SaveMode.Overwrite)
.format("csv")
.option("header",true)
.save(output_file_tipo_2a)


//Agregamos por día y horario
val output_file_tipo_2b = "/datascience/geo/Reports/GCBA/Coronavirus/%s/geohash_travel_barrio_radio_CLASE2_hourly_%s".format(today,country)

geo_labeled_users
.groupBy("BARRIO","RADIO","Day","Hour","device_id").agg(countDistinct("geo_hash_7") as "geo_hash_7")
.groupBy("BARRIO","RADIO","Day","Hour").agg(count("device_id") as "devices",avg("geo_hash_7") as "geo_hash_7_avg",stddev_pop("geo_hash_7") as "geo_hash_7_std")
.repartition(1)
.write
.mode(SaveMode.Overwrite)
.format("csv")
.option("header",true)
.save(output_file_tipo_2b)



}


  
}
