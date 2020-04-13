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
object distanceTraveled_CL {
  


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

val country = "CL"

val timezone = Map("argentina" -> "GMT-3",
                       "mexico" -> "GMT-5",
                       "UY"->"GMT-3",
                       "CL"->"GMT-3",
                       "CO"-> "GMT-5",
                       "PE"-> "GMT-5")
    
    //setting timezone depending on country
spark.conf.set("spark.sql.session.timeZone", timezone(country))

val today = (java.time.LocalDate.now).toString

val raw = get_safegraph_data(spark,"60","1",country)
.withColumnRenamed("ad_id","device_id")
.withColumn("device_id",lower(col("device_id")))
.withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
.withColumn("Day", date_format(col("Time"), "dd-MM-YY"))
.withColumn(
"geo_hash_7",
((abs(col("latitude").cast("float")) * 1000)
.cast("long") * 100000) + (abs(
col("longitude").cast("float") * 1000
).cast("long"))
)

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


///////////Agregación Nivel 0
//Queremos un cálculo general por país
val hash_user = spark.read.format("parquet").load(output_file).withColumn("device_id",lower(col("device_id")))

hash_user
.groupBy("Day","device_id").agg(countDistinct("geo_hash_7") as "geo_hash_7")
.groupBy("Day").agg(avg("geo_hash_7") as "geo_hash_7_avg",stddev_pop("geo_hash_7") as "geo_hash_7_std",count("device_id") as "devices")
.repartition(1)
.write
.mode(SaveMode.Overwrite)
.format("csv")
.option("header",true)
.save("/datascience/geo/Reports/GCBA/Coronavirus/%s/geohashes_by_country_%s".format(today,country))

// Agregaciones geogŕaficas

//Levantamos la tabla de equivalencias
val geo_hash_table = spark.read.format("csv").option("header",true)
.load("/datascience/geo/geohashes_tables/%s_GeoHash_to_Entity.csv".format(country))

//Levantamos la data
val geo_labeled_users = spark.read.format("parquet")
.load(output_file)
.join(geo_hash_table,Seq("geo_hash_7"))

geo_labeled_users.persist()

///////////Agregación Nivel 1
//definimos el output
val output_file_level_1 = "/datascience/geo/Reports/GCBA/Coronavirus/%s/geohashes_by_level_1_%s".format(today,country)

geo_labeled_users
.groupBy("Level1_Code","Level1_Name","Day","device_id").agg(countDistinct("geo_hash_7") as "geo_hash_7")
.groupBy("Level1_Code","Level1_Name","Day").agg(
  count("device_id") as "devices",
  avg("geo_hash_7") as "geo_hash_7_avg",
  stddev_pop("geo_hash_7") as "geo_hash_7_std")
.repartition(1)
.write
.mode(SaveMode.Overwrite)
.format("csv")
.option("header",true)
.save(output_file_level_1)


///////////Agregación Nivel 2

//definimos el output
val output_file_level_2 = "/datascience/geo/Reports/GCBA/Coronavirus/%s/geohashes_by_level_2_%s".format(today,country)

geo_labeled_users
.groupBy("Level1_Code","Level1_Name","Level2_Code","Level2_Name","Day","device_id").agg(countDistinct("geo_hash_7") as "geo_hash_7")
.groupBy("Level1_Code","Level1_Name","Level2_Code","Level2_Name","Day").agg(
  count("device_id") as "devices",
  avg("geo_hash_7") as "geo_hash_7_avg",
  stddev_pop("geo_hash_7") as "geo_hash_7_std")
.repartition(1)
.write
.mode(SaveMode.Overwrite)
.format("csv")
.option("header",true)
.save(output_file_level_2)

//definimos el output
val output_file_level_3 = "/datascience/geo/Reports/GCBA/Coronavirus/%s/geohashes_by_level_3_%s".format(today,country)

geo_labeled_users
.groupBy("Level1_Code","Level1_Name","Level2_Code","Level2_Name","Level3_Code","Level3_Name","Day","device_id").agg(countDistinct("geo_hash_7") as "geo_hash_7")
.groupBy("Level1_Code","Level1_Name","Level2_Code","Level2_Name","Level3_Code","Level3_Name","Day").agg(
  count("device_id") as "devices",
  avg("geo_hash_7") as "geo_hash_7_avg",
  stddev_pop("geo_hash_7") as "geo_hash_7_std")
.repartition(1)
.write
.mode(SaveMode.Overwrite)
.format("csv")
.option("header",true)
.save(output_file_level_3)



}


  
}
