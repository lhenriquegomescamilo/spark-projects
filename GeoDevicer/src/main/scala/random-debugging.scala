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
      .select("ad_id", "id_type", "latitude", "longitude", "utc_timestamp")
      .withColumn("latitude",col("latitude").cast("Double"))
      .withColumn("longitude",col("longitude").cast("Double"))
      
    df_safegraph                                
    
  }


  /*

Funciones  para telecentro

    val uas = spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/misc/ua_w_segments_5d")  
    
    uas.withColumn("segments",explode(split(col("segments"),","))).groupBy("brand","model","segments").agg(countDistinct("device_id"))
    .write.format("csv")
    .option("header",true)
    .option("delimiter","\t")
    .mode(SaveMode.Overwrite)
    .save("/datascience/misc/ua_agg_segments_5d")



//Hay que matchear con los PII para obtener los hashes

val telecentro_isp =  spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/audiences/crossdeviced/Telecentro_Test_ISP_to_push").withColumn("device_id",upper(col("device_id")))
val pii = spark.read.format("parquet").load("/datascience/pii_matching/pii_tuples/").withColumn("device_id",upper(col("device_id"))).filter("country = 'AR'").drop("device_type")

//telecentro_isp.show(2)
//pii.show(2)


val telecentro_hash = telecentro_isp.join(pii,Seq("device_id"))

     val ispMap = Map(
      "120885"->"Arnet",
       "120884"->"Speedy",
        "120883" ->"Fibertel",
          "120882"->"Telecentro")
          
          
          val audienceUDF = udf((dev_type: String) => ispMap(dev_type))
          
telecentro_hash.withColumn("ISP_Name",audienceUDF(col("ISP"))).write.format("csv").option("header",true).option("delimiter","\t").mode(SaveMode.Overwrite).save("/datascience/audiences/crossdeviced/Telecentro_Hash") 


    val hash_loaded = spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/audiences/crossdeviced/Telecentro_Hash")

hash_loaded.select("ml_sh2","mb_sh2","nid_sh2","ISP_Name").distinct().write.format("csv").option("header",true).option("delimiter","\t").mode(SaveMode.Overwrite).save("/datascience/audiences/crossdeviced/Telecentro_Hash_Unique") 



spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/audiences/crossdeviced/Telecentro_Hash_Unique").filter("ISP_Name == 'Arnet'").write.format("csv").option("header",true).option("delimiter","\t").mode(SaveMode.Overwrite).save("/datascience/audiences/crossdeviced/Telecentro_Hash_Arnet") 

spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/audiences/crossdeviced/Telecentro_Hash_Unique").filter("ISP_Name == 'Speedy'").write.format("csv").option("header",true).option("delimiter","\t").mode(SaveMode.Overwrite).save("/datascience/audiences/crossdeviced/Telecentro_Hash_Speedy") 

spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/audiences/crossdeviced/Telecentro_Hash_Unique").filter("ISP_Name == 'Fibertel'").write.format("csv").option("header",true).option("delimiter","\t").mode(SaveMode.Overwrite).save("/datascience/audiences/crossdeviced/Telecentro_Hash_Fibertel") 

spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/audiences/crossdeviced/Telecentro_Hash_Unique").filter("ISP_Name == 'Telecentro'").write.format("csv").option("header",true).option("delimiter","\t").mode(SaveMode.Overwrite).save("/datascience/audiences/crossdeviced/Telecentro_Hash_Telecentro") 
  }


val HourFrom = 19
val HourTo = 7

val raw = spark.read.format("csv").option("delimiter","\t").option("header",true).load("/datascience/geo/radios_argentina_2010_geodevicer_30d_argentina_30-8-2019-14h")

 val geo_hour = raw.select("device_id","device_type", "latitude", "longitude","utc_timestamp","name").withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp")))).withColumn("Hour", date_format(col("Time"), "HH")).filter(col("Hour") >= HourFrom || col("Hour") <= HourTo)
                                                                 
                                                    
val geo_counts = geo_hour.groupBy("device_id","device_type").agg(collect_list("name") as "radios_censales").withColumn("radios_censales", concat_ws(",", col("radios_censales")))

  geo_counts.write.format("csv").option("header",true).option("delimiter","\t").mode(SaveMode.Overwrite).save("/datascience/geo/geo_processed/radios_argentina_2010_geodevicer_30d_argentina_30-8-2019-14h_agg") 


// Esto es para el proceso de jcdaux
val safegraph_data = get_safegraph_data(spark,"60","1","mexico")
val all_audience_xd = spark.read.format("csv")
    .load("/datascience/audiences/crossdeviced/all_audience_a_k_s_h_a_xd")
    .select("_c1")
    .withColumnRenamed("_c1","device_id")
    .withColumn("device_id",upper(col("device_id")))

val joined = all_audience_xd.join(safegraph_data,Seq("device_id"))

joined.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/MX/JCDecaux/all_audience_xd_safegraph_60")


//tomo la audiencia de usuarios rankeados, madid y web
val audience_ranked = spark.read.format("csv").option("header",true).option("delimiter",",")
  .load("/datascience/geo/MX/JCDecaux/all_audience_ranked.csv")
  .withColumn("device_id",upper(col("device_id")))
  .select("device_id","audience","confidence")

//levanto tabla de equivlencia
val equivalence_table = spark.read.format("csv").load("/datascience/audiences/crossdeviced/all_audience_a_k_s_h_a_xd")
.select("_c0","_c1").toDF("device_id","device_id_xd")

//uso la tabla de equivalencia del XD para quedarme con los XD, ojo, acá me quedo sólo con el XD
val madid_w_category = equivalence_table.join(audience_ranked,Seq("device_id"))
.orderBy(asc("confidence"))
.drop("device_id")
.withColumnRenamed("device_id_xd","device_id")
.withColumn("device_id",upper(col("device_id")))
.dropDuplicates("device_id")

//levanto lo geo que había generado para esta audiencia los últimos 10 días. esto es todo méxico
val the_people_100 = spark.read.format("csv").option("header",true)
.option("delimiter","\t")
.load("/datascience/geo/MX/JCDecaux/all_audience_xd_safegraph_100")
.distinct()

//me quedo con los homes que estan en distrito federal
val homes_in_df = spark.read.format("csv")
.option("header",true)
.option("delimiter","\t")
.load("/datascience/geo/mexico_300d_home_6-9-2019-12h_w_NSE")
.withColumn("ENT", substring(col("CVEGEO"), 1, 2)).filter("ENT == '09'")
.withColumn("device_id",upper(col("ad_id"))).select("device_id")

val the_people_in_CITY = homes_in_df.join(the_people_100,Seq("device_id"))

val category_locations = the_people_in_CITY.join(madid_w_category,Seq("device_id"))


category_locations.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/MX/JCDecaux/category_locations_100")


*/


/*
println(geosparkConf)

import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}


// Ahora tenemos que unir homes con la clasificacion, y pasar esto por los homes de Argentina.
//GEOJSON
// Ahora tenemos que unir homes con la clasificacion, y pasar esto por los homes de Argentina.
//GEOJSON
val inputLocation = "/datascience/geo/polygons/AR/radio_censal/geo_json/radio_deshape.json"
val allowTopologyInvalidGeometris = true // Optional
val skipSyntaxInvalidGeometries = true // Optional
val spatialRDD = GeoJsonReader.readToGeometryRDD(spark.sparkContext, inputLocation, allowTopologyInvalidGeometris, skipSyntaxInvalidGeometries)
var rawSpatialDf = Adapter.toDf(spatialRDD,spark)
rawSpatialDf.createOrReplaceTempView("poligonomagico")

var spatialDf = spark.sql("""       select ST_GeomFromWKT(geometry) as myshape,_c1 as RADIO,_c2 as provincia FROM poligonomagico""".stripMargin).drop("rddshape")



val NSE_radius = spark.read.format("csv").option("header",true)
.option("delimiter","\t")
.load("/datascience/geo/polygons/AR/radio_censal/argentina_NSE_radius_TABLE")

val spatial_NSE = spatialDf.join(NSE_radius.select("RADIO","audience"),Seq("RADIO"))

//Este es el df completo
spatial_NSE.createOrReplaceTempView("poligonomagico")


//Ahora levantamos los usuarios:
val homes_raw = spark.read.format("csv").option("delimiter","\t").option("header",true)
.load("/datascience/geo/argentina_365d_home_1-10-2019-16h")
.toDF("device_id","device_type","freq","geocode","latitude","longitude")
.drop("geocode")


//Aplicando geometría a los puntos

homes_raw.createOrReplaceTempView("data")

var safegraphDf = spark      .sql(""" SELECT device_id,device_type,ST_Point(CAST(data.longitude AS Decimal(24,20)),
                                                             CAST(data.latitude AS Decimal(24,20))) 
                                                             as pointshape
              FROM data
          """)

safegraphDf.createOrReplaceTempView("data")
*/

/*
Esto de había Usado para lo de Brasil y ciudades de Brasil, SampleDataSafegraph. Rompe

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
var rawSpatialDf = Adapter.toDf(spatialRDDpolygon,spark)
rawSpatialDf.repartition(10)
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
.save("/datascience/geo/sample/brasi_users_in_city%s".format(day))

val total = users.select("ad_id").distinct().count()

/*
users.groupBy("ad_id").agg(count("utc_timestamp") as "detections").withColumn("total",lit(total))
.write.format("csv").option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/sample/brasi_users_signals_total_%s".format(day))
*/

*/

def match_users_to_polygons (spark: SparkSession,
      polygon_inputLocation: String,
      nDays: String,
      since: String,
      country: String) {



//Load the polygon
val inputLocation = polygon_inputLocation

val allowTopologyInvalidGeometris = true // Optional
val skipSyntaxInvalidGeometries = true // Optional
val spatialRDD = GeoJsonReader.readToGeometryRDD(spark.sparkContext, inputLocation,allowTopologyInvalidGeometris, skipSyntaxInvalidGeometries)

//Transform the polygon to DF
var rawSpatialDf = Adapter.toDf(spatialRDD,spark).repartition(30)
rawSpatialDf.createOrReplaceTempView("rawSpatialDf")

// Assign name and geometry columns to DataFrame
var spatialDf = spark.sql("""       select ST_GeomFromWKT(geometry) as myshape,_c1 as name FROM rawSpatialDf""".stripMargin).drop("rddshape")

spatialDf.createOrReplaceTempView("poligonomagico")

spatialDf.show(5)

val df_safegraph = get_safegraph_data(spark,nDays,since,country)

df_safegraph.createOrReplaceTempView("data")
//df_safegraph.show(2)

var safegraphDf = spark      .sql(""" SELECT ad_id,ST_Point(CAST(data.longitude AS Decimal(24,20)),
                                                             CAST(data.latitude AS Decimal(24,20))) 
                                                             as pointshape
              FROM data
          """)


safegraphDf.createOrReplaceTempView("data")

//safegraphDf.show(2)



val intersection = spark.sql(
      """SELECT  *   FROM poligonomagico,data   WHERE ST_Contains(poligonomagico.myshape, data.pointshape)""").select("ad_id","name")

println ("miracaloco")
//intersection.show(5)

val output_name = (polygon_inputLocation.split("/").last).split(".json") (0).toString

intersection.dropDuplicates().write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/geo_processed/%s_%s_%s_sjoin_polygon".format(
  output_name,
  nDays,country))



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
       .config("geospark.join.gridtype", "kdbtree")
       .config("geospark.join.spatitionside","left")
       .appName("myGeoSparkSQLdemo").getOrCreate()
     
GeoSparkSQLRegistrator.registerAll(spark)

// Initialize the variables
val geosparkConf = new GeoSparkConf(spark.sparkContext.getConf)

//Logger.getRootLogger.setLevel(Level.WARN)

match_users_to_polygons(spark,
  "/datascience/geo/polygons/AR/radio_censal/radios_argentina_2010_geodevicer.json",
  "10",
  "2",
  "argentina")

  }
}