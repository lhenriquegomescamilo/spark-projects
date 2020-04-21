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
import org.apache.spark.sql.functions.{stddev_samp, stddev_pop}



/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object GeoHashesEnrichment {
  

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
      .withColumnRenamed("ad_id","device_id")
      .withColumnRenamed("id_type","device_type")
      .withColumn("device_id",lower(col("device_id")))

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
      .withColumn("country",split(col("input"),"/").getItem(7))
      .withColumn("country",split(col("country"),"=")(1))
      
     df_safegraph                    
    
  }



def get_unique_geo_hashes( spark: SparkSession,
      nDays: String,
      since: String) {


val today = (java.time.LocalDate.now).toString
val output_file = "/datascience/geo/GeoHash/"

val getGeoHash = udf(
      (latitude: Double, longitude: Double) =>
        com.github.davidallsopp.geohash.GeoHash.encode(latitude, longitude, 8)
    )

val geo_data = get_safegraph_all_country(spark,nDays,since)
  .withColumn("geo_hash", getGeoHash(col("latitude"), col("longitude")))
  .withColumn("geo_hash_7", substring(col("geo_hash"), 0, 7))
  .select("geo_hash","geo_hash_7","latitude","longitude","country")

geo_data.persist()

  val precision_8 = geo_data.drop("geo_hash_7").dropDuplicates("geo_hash")
  val precision_7 = geo_data.dropDuplicates("geo_hash_7")
 
precision_8
.write.format("parquet")
.partitionBy("country")
      .mode(SaveMode.Overwrite)
      .save(output_file+"/precision_8")
  
precision_7
.write.format("parquet")
.partitionBy("country")
      .mode(SaveMode.Overwrite)
      .save(output_file+"/precision_7")


                }



 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("Spark devicer").config("spark.sql.files.ignoreCorruptFiles", "true").getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)


get_unique_geo_hashes(spark,"2","1")


}

}
