package main.scala

//esto para hacer funcionar geopsark y sus geofunciones
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.log4j.{Level, Logger}

import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import com.vividsolutions.jts.geom.{
  Coordinate,
  Geometry,
  Point,
  GeometryFactory
}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.apache.spark.storage.StorageLevel

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.log4j.{Level, Logger}

import com.vividsolutions.jts.geom.{Coordinate, Geometry, Point, GeometryFactory}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.apache.spark.storage.StorageLevel

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.serializer.KryoRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.apache.spark.sql.types.{DataType, StructType}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader


import org.datasyslab.geospark.utils.GeoSparkConf


import org.datasyslab.geospark.formatMapper.GeoJsonReader





/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object geoDebugging {
  def get_tapad_home_cluster(spark:SparkSession){

val code = spark.read.format("csv").option("header",true).option("delimiter","\t")
.load("/datascience/geo/argentina_365d_home_21-8-2019-0h").withColumn("device_id",upper(col("ad_id")))


val poly = spark.read.format("csv").option("header",true).option("delimiter","\t")
.load("/datascience/geo/radios_argentina_2010_geodevicer_5d_argentina_14-8-2019-17h").withColumn("device_id",upper(col("device_id")))

val index = spark.read.format("csv").option("delimiter","\t").load("/data/crossdevice/2019-07-14/Retargetly_ids_full_20190714_193703_part_00.gz")
val home_index = index.withColumn("tmp",split(col("_c2"),"=")).select(col("_c0"),col("tmp").getItem(1).as("_c2")).drop("tmp").filter(col("_c2").isNotNull).toDF("house_cluster","device_id").withColumn("device_id",upper(col("device_id")))


poly.join(home_index,Seq("device_id"))
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/AR/tapad_w_polygon")

code.join(home_index,Seq("device_id"))
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/AR/tapad_w_geocode")

  }

def getDataPipeline(
      spark: SparkSession,
      path: String,
      nDays: String,
      since: String) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    //specifying country
    val country_iso = "AR"
      
        // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day,country_iso))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    df
  }

def get_ua_segments(spark:SparkSession) = {

val ua = spark.read.format("parquet")
        .load("/datascience/data_useragents/day=*/country=AR")
        .filter("model != ''") //con esto filtramos los desktop
        .withColumn("device_id",upper(col("device_id")))
        .drop("user_agent","event_type","url")
        .dropDuplicates("device_id")

val segments = getDataPipeline(spark,"/datascience/data_triplets/segments/","5","10")
              .withColumn("device_id",upper(col("device_id")))
              .groupBy("device_id").agg(concat_ws(",",collect_set("feature")) as "segments")

val joined = ua.join(segments,Seq("device_id"))
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/misc/ua_w_segments_5d")

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
    val format = "yyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt)
      .map(end.minusDays(_))
      .map(_.toString(format))
      

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
      .select("ad_id", "id_type", "latitude", "longitude", "utc_timestamp")
      .withColumnRenamed("ad_id","device_id")
      .withColumnRenamed("id_type","device_type")
      .withColumn("device_id",upper(col("device_id")))
      .withColumn("latitude",col("latitude").cast("Double"))
      .withColumn("longitude",col("longitude").cast("Double"))

      //.dropDuplicates("ad_id", "latitude", "longitude")

     df_safegraph                    
    
  }


  def get_safegraph_SAMPLE_data(
      spark: SparkSession,
      nDays: String,
      since: String,
      country: String
     
  ) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

   
    // Get the days to be loaded
    val format = "yyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt)
      .map(end.minusDays(_))
      .map(_.toString(format))
      

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
      .select("ad_id", "id_type", "latitude", "longitude", "utc_timestamp")
      .withColumnRenamed("ad_id","device_id")
      .withColumnRenamed("id_type","device_type")
      .withColumn("device_id",upper(col("device_id")))
      .withColumn("latitude",col("latitude").cast("Double"))
      .withColumn("longitude",col("longitude").cast("Double"))

      //.dropDuplicates("ad_id", "latitude", "longitude")

     df_safegraph                    
    
  }


  


 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
   

val spark = SparkSession.builder()
.config("spark.sql.files.ignoreCorruptFiles", "true")
 .config("spark.serializer", classOf[KryoSerializer].getName)
 .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("geospark.global.index","true")
       .appName("myGeoSparkSQLdemo").getOrCreate()
// .config("spark.kryo.registrator",classOf[GeoSparkKryoRegistrator].getName)
     //.config("geospark.join.gridtype", "kdbtree")
      // .config("geospark.join.spatitionside","left")
GeoSparkSQLRegistrator.registerAll(spark)

   // Initialize the variables
val geosparkConf = new GeoSparkConf(spark.sparkContext.getConf)

//Brasil test
//Levantamos los users
val day = "06"
val users = spark.read.format("csv").option("header",true)
.load("/datascience/geo/sample/Safegraph/2019/10/%s".format(day))
users.createOrReplaceTempView("data")

var safegraphDf = spark      .sql(""" SELECT ad_id,ST_Point(CAST(data.longitude AS Decimal(24,20)),
                                                             CAST(data.latitude AS Decimal(24,20))) 
                                                             as pointshape
              FROM data
          """)

safegraphDf.createOrReplaceTempView("data")

//LEvantamos los poligoninios
val inputLocation = "/datascience/geo/POIs/cidade_geodevicer.json"
val allowTopologyInvalidGeometris = true // Optional
val skipSyntaxInvalidGeometries = true // Optional
var spatialRDDpolygon = GeoJsonReader.readToGeometryRDD(spark.sparkContext, inputLocation, allowTopologyInvalidGeometris, skipSyntaxInvalidGeometries)

//Es un rdd pero lo pasamos a SQL dataframe
var rawSpatialDf = Adapter.toDf(spatialRDDpolygon,spark).filter("_c1 != 'janeiro'")
broadcast(rawSpatialDf)
rawSpatialDf.createOrReplaceTempView("rawSpatialDf")

var spatialDf = spark.sql("""       select ST_GeomFromWKT(geometry) as myshape,_c1 as name  FROM rawSpatialDf""".stripMargin)
spatialDf.createOrReplaceTempView("poligonomagico")

//Hacemos la interseccion 
val intersection = spark.sql(
      """SELECT  *   FROM poligonomagico,data   WHERE ST_Contains(poligonomagico.myshape, data.pointshape)""")
            
intersection.select("name","ad_id").groupBy("name").agg(countDistinct("ad_id") as "unique_users")
.write.format("csv").option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/sample/brasi_users_in_city_%s".format(day))

/*
val total = users.select("ad_id").distinct().count()

users.groupBy("ad_id").agg(count("utc_timestamp") as "detections").withColumn("total",lit(total))
.write.format("csv").option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/sample/brasi_users_signals_total_%s".format(day))
*/


  }
}