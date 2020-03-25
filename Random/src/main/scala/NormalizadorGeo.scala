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
  Este proceso la diea es que cuente detecciones y usuarios únicos por día en toda la data geo y las deje guardad en algún lugar*/
object normalizadorGeo {

  

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
      .withColumn("country",substring(col("country"), 9, 10))
      .withColumn("day",substring(col("day"), 5, 8))
      .withColumnRenamed("ad_id","device_id")
      .withColumnRenamed("id_type","device_type")
      .withColumn("device_id",lower(col("device_id")))
      
     df_safegraph                    
    
  }


 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("Spark devicer").config("spark.sql.files.ignoreCorruptFiles", "true").getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)


val today = (java.time.LocalDate.now).toString

get_safegraph_all_country(spark,"30","1")
.groupBy("day","country").agg(countDistinct("device_id") as "devices",count("utc_timestamp") as "detections")
.repartition(1)
.write
    .mode(SaveMode.Overwrite)
    .format("csv")
    .option("header",true)
    .save("/datascience/geo/Reports/Normalizador/%s".format(today))




}


  
}
