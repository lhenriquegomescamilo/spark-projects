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
object distanceTraveledHome {
  


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

/*


/////////////////Distancia al hogar
//Tenemos esta data que tenemos geohashadita y por hora, la agrupamos por geoh y por hora    
//Esto es safegraph pelado los uĺtimos X dáis
//Esto nos da todos los geocode donde estuvo un usuario, y además nos quedamos con un lat long representativo de cada uno
val geo_hash_visits = raw.dropDuplicates("device_id","Day","geo_hash")


//Path home ARG
val path_homes = "/datascience/geo/NSEHomes/argentina_180d_home_27-2-2020--3h"
val df_homes = spark.read.format("csv")
    .option("delimiter","\t")
    .option("header",false)
    .load(path_homes)
    .toDF("device_id","pii_type","freq","else","lat_home","lon_home")
    .filter("lat_home != lon_home")
    .withColumn( "lat_home",((col("lat_home").cast("float"))))
    .withColumn( "lon_home",((col("lon_home").cast("float"))))
    .select("device_id","lat_home","lon_home")
    
//Armamos el vs dataset: acá tenemos el hogar vs cada uno de los geohashes de un usuario
    val device_vs_device = df_homes.join(geo_hash_visits,Seq("device_id"))

// Using vincenty formula to calculate distance between user/device location and ITSELF.
    device_vs_device.createOrReplaceTempView("joint")
    
val columns = device_vs_device.columns

    val query =
    """SELECT latitude,
                longitude,
                Day,
                lat_home,
                lon_home,
                device_id,
                geo_hash,
                distance
            FROM (
            SELECT *,((1000*111.045)*DEGREES(ACOS(COS(RADIANS(latitude)) * COS(RADIANS(lat_home)) *
            COS(RADIANS(longitude) - RADIANS(lon_home)) +
            SIN(RADIANS(latitude)) * SIN(RADIANS(lat_home))))) as distance
            FROM joint 
            )
            WHERE distance >= 0"""

val sqlDF = spark.sql(query)


//Acá tenemos la distancia de cada usuario cada vez que cambio de geohash    
val distance_from_home = spark.sql(query)
//Guardamos este raw sobre el que luego haremos operaciones para obtener métricas de distancia al hgoar
distance_fom_home
  .write
  .mode(SaveMode.Overwrite)
  .format("parquet")
  .save("/datascience/geo/Reports/GCBA/Coronavirus/distance_from_home_%s".format(today))//


////////////////////////////////////////////////Distancia recorrida por día

//Y acá empiezo a calcular la distancia recorrida por usuario. 
val tipito = raw
.withColumn("latituderad",toRadians(col("latitude")))
.withColumn("longituderad",toRadians(col("longitude")))


val windowSpec = Window.partitionBy("device_id").orderBy("utc_timestamp")

val spacelapse = tipito
.withColumn("deltaLat", col("latituderad") - lag("latituderad", 1).over(windowSpec))
.withColumn("deltaLong", col("longituderad") - lag("longituderad", 1).over(windowSpec))
.withColumn("a1", pow(sin(col("deltaLat")/2),2))
.withColumn("a2", cos(col("latituderad")) * cos(lag("latituderad", 1).over(windowSpec)) * col("deltaLong")/2)
.withColumn("a", pow(col("a1")+col("a2"),2))
.withColumn("greatCircleDistance1",(sqrt(col("a"))*2))
.withColumn("greatCircleDistance2",(sqrt(lit(1)-col("a"))))
.withColumn("distance",atan2(col("greatCircleDistance1"),col("greatCircleDistance2"))*6371*1000)
.withColumn("timeDelta", (col("utc_timestamp") - lag("utc_timestamp", 1).over(windowSpec)))
//.withColumn("speed(km/h)",col("distance(m)") *3.6/ col("timeDelta(s)") )
.withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
.withColumn("Day", date_format(col("Time"), "YY-MM-dd"))
.select("device_id","utc_timestamp","latitude","longitude","distance","timeDelta","Day","geo_hash")

//spacelapse

//.groupBy("Day","device_id").agg(sum(col("distance(m)")) as "distance(m)",sum(col("timeDelta(s)")) as "timeDelta(s)")

//Esto nos da por usuario por día, la distancia recorrida. //Esto lo guardaría.
//también quiero un promedio de esto

//val space_lapse_agg = spacelapse.groupBy("Day").agg(count("device_id") as "devices",avg(col("distance(m)")) as "distance_avg",avg(col("timeDelta(s)")) as "timeDelta_avg")


spacelapse
    .write
    .mode(SaveMode.Overwrite)
    .format("parquet")
    .option("header",true)
    .save("/datascience/geo/Reports/GCBA/Coronavirus/distance_traveled_%s".format(today))



*/


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

spark.conf.set("spark.sql.session.timeZone", "GMT-3")

val today = (java.time.LocalDate.now).toString

val raw = get_safegraph_data(spark,"5","1",country)
.withColumnRenamed("ad_id","device_id")
.withColumn("device_id",lower(col("device_id")))
.withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
.withColumn("Day", date_format(col("Time"), "YY-MM-dd"))
.withColumn("geo_hash_7",substring(col("geo_hash"), 0, 7))

//Vamos a usarlo para calcular velocidad y distancia al hogar
raw.persist()

val geo_hash_visits = raw
.groupBy("device_id","Day","geo_hash_7").agg(count("utc_timestamp") as "detections")
.withColumn("country",lit(country))

val output_file = "/datascience/geo/Reports/GCBA/Coronavirus/%s/geohashes_by_user_%s".format(today,country)

geo_hash_visits
 .write
    .mode(SaveMode.Overwrite)
    .format("parquet")
    .option("header",true)
    .save(output_file)



//Con esto de abajo calculamos para barrios, por ahroa sólo funciona para Argentina

val barrios = spark.read.format("csv").option("header",true).option("delimiter",",")
.load("/datascience/geo/Reports/GCBA/Coronavirus/")
.withColumnRenamed("geo_hashote","geo_hash_7")


//Alternativa 1
//Path home ARG

//Nos quedamos con los usuarios de los homes que viven en caba
val homes = spark.read.format("parquet").load("/datascience/data_insights/homes/day=2020-03/country=AR")
val geocode_barrios = spark.read.format("csv").option("header",true).load("/datascience/geo/Reports/GCBA/Coronavirus/Geocode_Barrios_CABA.csv")

val homes_barrio = homes.select("device_id","GEOID").join(geocode_barrios,Seq("GEOID")).drop("GEOID")

val output_file_tipo_1 = "/datascience/geo/Reports/GCBA/Coronavirus/%s/geohashes_by_barrio_tipo1_%s".format(today,country)

spark.read.format("parquet")
.load(output_file)
.withColumn("device_id",lower(col("device_id")))
.groupBy("device_id","Day").agg(countDistinct("geo_hash_7") as "geo_hash_7")
.join(homes_barrio,Seq("device_id"))
.groupBy("BARRIO","Day").agg(avg("geo_hash_7") as "geo_hash_7_avg",stddev_pop("geo_hash_7") as "geo_hash_7_std")
.repartition(1)
.write
.mode(SaveMode.Overwrite)
.format("csv")
.option("header",true)
.save(output_file_tipo_1)



//Alternativa 2
val output_file_tipo_2 = "/datascience/geo/Reports/GCBA/Coronavirus/%s/geohashes_by_barrio_tipo2_%s".format(today,country)
val tipo2 = spark.read.format("parquet")
.load(output_file)
.join(barrios,Seq("geo_hash_7"))
.groupBy("COMUNA","BARRIO","Day","device_id").agg(countDistinct("geo_hash_7") as "geo_hash_7")
.groupBy("COMUNA","BARRIO","Day").agg(avg("geo_hash_7") as "geo_hash_7_avg",stddev_pop("geo_hash_7") as "geo_hash_7_std")
.repartition(1)
.write
.mode(SaveMode.Overwrite)
.format("csv")
.option("header",true)
.save(output_file_tipo_2)


//Alternativa 3
val output_file_tipo_3 = "/datascience/geo/Reports/GCBA/Coronavirus/%s/geohashes_by_barrio_tipo3_%s".format(today,country)

val hash_user = spark.read.format("parquet").load(output_file).withColumn("device_id",lower(col("device_id")))

val barrio_user = spark.read.format("parquet").load("/datascience/geo/Reports/GCBA/Coronavirus/geohashes_list_by_user_2020-03-24")
.withColumn("device_id",lower(col("device_id")))
.join(barrios,Seq("geo_hash_7")).select("COMUNA","BARRIO","device_id").distinct()

barrio_user.join(hash_user,Seq("device_id"))
.groupBy("COMUNA","BARRIO","Day").agg(avg("geo_hash_7") as "geo_hash_7_avg",stddev_pop("geo_hash_7") as "geo_hash_7_std")
.repartition(1)
.write
.mode(SaveMode.Overwrite)
.format("csv")
.option("header",true)
.save(output_file_tipo_3)



}


  
}
