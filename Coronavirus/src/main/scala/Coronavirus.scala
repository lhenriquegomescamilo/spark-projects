package main.scala
import main.scala.geodevicer.Geodevicer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import scala.util.parsing.json._
import org.apache.spark.sql.functions.{
  upper,
  count,
  col,
  abs,
  udf,
  regexp_replace,
  split,
  lit,
  explode,
  length,
  to_timestamp,
  from_unixtime,
  date_format,
  sum
}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{
  StructType,
  StructField,
  StringType,
  IntegerType
}
import org.apache.spark.sql.{Column, Row}
import scala.util.Random.shuffle
import org.apache.spark.sql.expressions.Window

object Coronavirus {

  def get_data_pois(spark:SparkSession){

    // Path fijos con los pois de los distintos paises
    val files = List(("tablero_25-03-20_5d_argentina_26-3-2020-16h","AR"),
                      ("Critical_Places_MX_30d_mexico_27-3-2020-11h","MX"))

    val path_geo_jsons = "/datascience/geo/geo_json/"
    
    val timezone = Map("AR" -> "GMT-3",
                       "MX" -> "GMT-5",
                       "CL"->"GMT-3",
                       "CO"-> "GMT-5",
                       "PE"-> "GMT-5")
    
    // iterate over pois and generate files
    for ((filename,country) <- files) {
      //setting timezone depending on country
      spark.conf.set("spark.sql.session.timeZone", timezone(country))

      // Run geo
      Geodevicer.run_geodevicer(spark,path_geo_jsons+filename+".json")
      // Process results and save
      spark.read.format("csv")
                .option("header",true)
                .option("delimiter","\t")
                .load("/datascience/geo/raw_output/%s".format(filename))
                .withColumn("Time", to_timestamp(from_unixtime(col("timestamp"))))
                .withColumn("day", date_format(col("Time"), "dd-MM-YY"))
                .groupBy("day","audience").agg(countDistinct("device_id") as "devices",
                                                count("timestamp") as "detections")
                .orderBy(asc("Day"))
                .withColumn("country", lit(country))
                .write
                .format("parquet")
                .partitionBy("day","country")
                .mode("append")
                .save("/datascience/coronavirus/data_pois/")
    }

  }

  def get_coronavirus(spark: SparkSession,country:String,day:String) {

    val raw = spark.read
      .format("parquet")
      .option("basePath", "/datascience/geo/safegraph/")
      .load("/datascience/geo/safegraph/day=%s/country=%s/".format(day,country))
      .withColumnRenamed("ad_id", "device_id")
      .withColumn("device_id", lower(col("device_id")))
      .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
      .withColumn("Hour", date_format(col("Time"), "YYYYMMddHH"))
      .withColumn("window", date_format(col("Time"), "mm"))
      .withColumn(
        "window",
        when(col("window") > 40, 3)
          .otherwise(when(col("window") > 20, 2).otherwise(1))
      )
      .withColumn("window", concat(col("Hour"), col("window")))
      .drop("Time")

    // Select sample of 1000 users
    val initial_seed = spark.read
      .load("/datascience/coronavirus/coronavirus_seed/day=%s/country=%s".format(day,country))
      .select("device_id", "geo_hash", "window")

    // Get the distinct moments to filter the raw data
    val moments = initial_seed.select("geo_hash", "window").distinct()


    // Join raw data with the moments and store
    raw
      .join(moments, Seq("geo_hash", "window"))
      .withColumn("day",lit(day))
      .withColumn("country",lit(country))
      .write
      .format("parquet")
      .partitionBy("day","country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/coronavirus/coronavirus_contacts/")

    val joint = spark.read.load("/datascience/coronavirus/coronavirus_contacts/day=%s/country=%s".format(day,country))

    // Calculate it by day
    val udfDay = udf((d: String) => d.substring(0, 8))

    joint
      .join(
        initial_seed.withColumnRenamed("device_id", "original_id"),
        Seq("geo_hash", "window")
      )
      .withColumn("day", udfDay(col("window")))
      .groupBy("original_id", "day")
      .agg(collect_set(col("device_id")).as("devices"))
      .withColumn("contacts", size(col("devices"))-1) // Calculate number of contacts for each device
      .groupBy("day")
      .agg(mean("contacts") as "contacts")
      .withColumn("country",lit(country))
      .write
      .format("parquet")
      .partitionBy("day","country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/coronavirus/coronavirus_contacts_per_day/")

  }
  def generate_seed(spark:SparkSession,country:String,day:String){
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(0)

    // Get sample of 10K seed users per day
    val days = (0 until 10).map(start.minusDays(_)).map(_.toString(format))
    //val days = List("20200323","20200322","20200321","20200320","20200319","20200318","20200317","20200316","20200315","20200314","20200313","20200312","20200311","20200310",
                    //"20200309","20200308","20200307","20200306","20200305","20200304","20200303","20200302","20200301")
    val path = "/datascience/geo/safegraph/"
    val dfs = days
      .map(day => path + "day=%s/".format(day) + "country=%s".format(country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      .map(
        x =>
          spark.read
            .option("basePath", path)
            .parquet(x)
            .withColumnRenamed("ad_id", "device_id")
            .select("device_id")
            .distinct
            .limit(20000)
      )

    val users = dfs.reduce((df1, df2) => df1.union(df2)).select("device_id").distinct

    // Now calculate all the moments for the seed users
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    spark.read
          .option("basePath", path)
          .parquet(hdfs_files: _*)
          .withColumnRenamed("ad_id", "device_id")
          .withColumn("device_id", lower(col("device_id")))
          .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
          .withColumn("Hour", date_format(col("Time"), "YYYYMMddHH"))
          .withColumn("window", date_format(col("Time"), "mm"))
          .withColumn("window",
            when(col("window") > 40, 3)
              .otherwise(when(col("window") > 20, 2).otherwise(1))
          )
          .withColumn("window", concat(col("Hour"), col("window")))
          .drop("Time")
          .join(users,Seq("device_id"),"inner")
          .select("device_id","geo_hash", "window")
          .withColumn("day",lit(day))
          .withColumn("country",lit(country))
          .distinct
          .write
          .format("parquet")
          .partitionBy("day","country")
          .mode(SaveMode.Overwrite)
          .save("/datascience/coronavirus/coronavirus_seed/")

  }

  def coronavirus_barrios(spark:SparkSession,country:String, barrios:DataFrame, name:String, day:String){
    val udfGeo = udf((d: String) => d.substring(0, 7))
    
    val initial_seed = spark.read
      .load("/datascience/coronavirus/coronavirus_seed/day=%s/country=%s".format(day,country))
      .select("device_id", "geo_hash", "window")
    
    val contacts = spark.read
                        .load("/datascience/coronavirus/coronavirus_contacts/day=%s/country=%s".format(day,country))
                        .withColumn("geo_hash_join",udfGeo(col("geo_hash")))

    val joint = contacts.join(broadcast(barrios),Seq("geo_hash_join"),"inner")

    // Calculate it by day
    val udfDay = udf((d: String) => d.substring(0, 8))

    joint
      .join(
        initial_seed.withColumnRenamed("device_id", "original_id"),
        Seq("geo_hash", "window")
      )
      .withColumn("day", udfDay(col("window")))
      .groupBy("original_id", "day",name)
      .agg(collect_set(col("device_id")).as("devices"))
      .withColumn("contacts", size(col("devices"))-1) // Calculate number of contacts for each device
      .groupBy(name)
      .agg(mean("contacts") as "contacts")
      .withColumn("day",lit(day))
      .withColumn("country",lit(country))
      .write
      .format("parquet")
      .partitionBy("day","country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/coronavirus/coronavirus_contacts_barrios")

  }

  def main(args: Array[String]) {

    Logger.getRootLogger.setLevel(Level.WARN)
    
    var spark = SparkSession.builder
      .appName("Coronavirus Daily Data")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()
    
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    val since = 1
    val ndays = 10
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(since + ndays)
    val end = DateTime.now.minusDays(since)

    val daysCount = Days.daysBetween(start, end).getDays()
    val days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))
    
    val barrios_ar =  spark.read.format("csv")
                        .option("header",true)
                        .option("delimiter",",")
                        .load("/datascience/geo/Reports/GCBA/Coronavirus/")
                        .withColumnRenamed("geo_hashote","geo_hash_join")

    val barrios_mx = spark.read
                          .format("csv")
                          .option("sep","\t")
                          .option("header","true")
                          .load("/datascience/geo/geo_processed/MX_municipal_mexico_sjoin_polygon")


    for (day <- days){
      println(day)
      println("\tAR")
      // AR
      generate_seed(spark,"argentina",day)
      get_coronavirus(spark,"argentina",day)  
      coronavirus_barrios(spark,"argentina",barrios_ar,"BARRIO",day)
      println("\tMX")

      // MX
      generate_seed(spark,"mexico",day)
      get_coronavirus(spark,"mexico",day)  
      coronavirus_barrios(spark,"mexico",barrios_mx,"NOM_MUN",day)

    }

    

  }
}
