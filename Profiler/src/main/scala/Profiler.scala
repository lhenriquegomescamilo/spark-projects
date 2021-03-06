package main.scala


import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.apache.spark.sql.functions._
import scala.collection.Map
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime
import org.apache.spark.sql.functions.{round, broadcast, col, abs, upper}
import org.apache.spark.sql.SaveMode

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object Profiler {
   
//acá vamos a obtener la data de data audiences que la vamos a usar como variable para distintas mini muestras


/////////////////////////////
   def getDataAudiences(
      spark: SparkSession) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not

    val nDays: Int = 2
    val since: Int = 1
    val country : String = "AR"

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_audiences"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day,country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select("device_id","device_type","third_party","event_type","url","timestamp","app_installed")

      df

  }

val spark = SparkSession.builder.appName("Test").getOrCreate()
val daud = getDataAudiences(spark)


/////////////////////////////
def get_ua (
      spark: SparkSession) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    
    val nDays = 2
    val since = 1
    val country = "AR"

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_useragents/"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day,country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .withColumn("category", lit(""))
      .withColumn("title", lit(""))
      
      df
  }


/////
 def get_safegraph_data(
      spark: SparkSession,
      nDays: Int = 2,
      since: Int = 10,
      country : String = "argentina" //,value_dictionary: Map[String, String]
  ) : DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs files to be read
    val path = "/datascience/geo/safegraph_pipeline/"
    val hdfs_files = days
      .map(day => path +  "day=0%s/country=%s/".format(day,country))
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
      

    df_safegraph
  }


//Acá abajo procesamos los datos
/////////////////////////////
def get_activiy (
spark: SparkSession) = {

val activity_min = 10

val activity = daud.groupBy("device_id")          
              .agg(collect_list(col("timestamp")) as "timestamp",
                    collect_list(col("event_type")) as "event_type",
                    collect_list(col("url")) as "url",
                    count("timestamp") as "activity")
              .filter(col("activity")>activity_min)
              .withColumn("url", concat_ws(",", col("url")))
              .withColumn("timestamp", concat_ws(",", col("timestamp")))          
              .withColumn("event_type", concat_ws(",", col("event_type")))

/**
//Proceso viejo:
val activity = daud.select("device_id","timestamp").groupBy("device_id")
          .agg(collect_set(col("timestamp")) as "detections")
          .withColumn("activity",size(col("detections"))).filter(col("activity")>= activity_min )

val high_activity = daud.join(activity,Seq("device_id"),"inner")
//high_activity.select(col("device_id")).distinct().count
//3534
//de esto vamos a querer el event type y la url/domain

val user_activity = high_activity.select("device_id","event_type","url","timestamp","activity")
.groupBy("device_id","activity")
.agg(collect_list(col("url"))  as "site_visits",
      collect_list(col("timestamp")) as "time_visit",
      collect_list(col("event_type")) as "event_types")
.withColumn("site_visits", concat_ws(",", col("site_visits")))
.withColumn("time_visit", concat_ws(",", col("time_visit")))
.withColumn("event_types", concat_ws(",", col("event_types")))

user_activity.write.format("csv")
.option("header",true)
.option("sep", "\t").mode(SaveMode.Overwrite)
.save("/datascience/geo/MiniMuestra/%s".format("activity"))
**/
activity.write.format("csv")
.option("header",true)
.option("sep", "\t").mode(SaveMode.Overwrite)
.save("/datascience/geo/MiniMuestra/%s".format("activity"))



}

/////////////////////////////
def get_apps (
spark: SparkSession) = {

val app_min = 1
//val daud = getDataAudiences(spark)

val apps = daud.select("device_id","app_installed")
.withColumn("app_installed",explode(col("app_installed")))
.groupBy("device_id").agg(collect_set(col("app_installed")) as "apps")
.withColumn("appstotal",size(col("apps"))).filter(col("appstotal") > app_min)
.withColumn("appstotal",col("appstotal")-1)
.withColumn("apps", concat_ws(",", col("apps")))

apps.write
.format("csv")
.option("header",true)
.option("sep", "\t").mode(SaveMode.Overwrite)
.save("/datascience/geo/MiniMuestra/%s".format("apps"))


}

/////////////////////////////


def get_3rd_party(
spark: SparkSession) = {

val third_party_min = 20

//val daud = getDataAudiences(spark)
val segments = daud
    .select("device_id","device_type","third_party")
    .withColumn("third_party",explode(col("third_party")))
    .groupBy("device_id","device_type")
    .agg(collect_set(col("third_party")) as "third_party")
    .withColumn("segment_total",size(col("third_party")))
    .filter(col("segment_total") > third_party_min)
    .withColumn("third_party", concat_ws(",", col("third_party")))

segments.write
.format("csv")
.option("header",true)
.option("sep", "\t").mode(SaveMode.Overwrite)
.save("/datascience/geo/MiniMuestra/%s".format("third_party"))

}


def  geo_high (
spark: SparkSession) = {

val location_min = 50

val dev = get_safegraph_data(spark)

val my_users = dev.groupBy("ad_id").agg(count("utc_timestamp") as "detections") 
                .filter(col("detections") >= location_min)

val filtered = dev.join(my_users,Seq("ad_id"),"inner")

//Esto lo luego haríamos un crossdevice y guardamos la base 1.

//acá generamos los datos de loation de los usuarios con sus timestamps
val with_array = filtered.withColumn("location",concat(lit("("),col("latitude"),lit(","),col("longitude"),lit(")")))
.groupBy("ad_id","id_type","detections").
agg(concat_ws(";",collect_list(col("utc_timestamp"))).as("times_array"), 
  concat_ws(";",collect_list("location")).as("location_array"))

with_array.write
.format("csv")
.option("header",true)
.option("sep", "\t").mode(SaveMode.Overwrite)
.save("/datascience/geo/MiniMuestra/%s".format("geo"))

 }
/*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) = {
    //val spark = SparkSession.builder.appName("Test").getOrCreate()

    //val daud = getDataAudiences(spark)
    //daud.cache()

    val useragent = get_ua(spark)

    
    //get_apps(spark)
    //get_3rd_party(spark)
    //geo_high(spark)
    get_activiy(spark)
  }
}

