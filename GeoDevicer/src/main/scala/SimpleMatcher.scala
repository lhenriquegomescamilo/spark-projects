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




object SimpleMatcher {

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
      .filter("geo_hash != 'gcba'")
      
    df_safegraph                                
    
  }

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


//if we want to use safegraph uncomment this
/*
//here we load from safegraph
val df_safegraph = get_safegraph_data(spark,nDays,since,country)
df_safegraph.createOrReplaceTempView("data")

//here we parse it
var safegraphDf = spark .sql("""SELECT ad_id,id_type,ST_Point(CAST(data.longitude AS Decimal(24,20)), CAST(data.latitude AS Decimal(24,20))) as geometry
              FROM data  """)

safegraphDf.createOrReplaceTempView("data")

*/
//if we want to use a specific dataframe with geodata, use this:
val df_safegraph = spark.read.format("csv").option("header",false).option("delimiter","\t")
                        .load("/datascience/geo/mexico_200d_home_29-10-2019-10h/")
                        .toDF("ad_id","id_type","freq","geocode","latitude","longitude")
                        .filter("freq>2")
df_safegraph.createOrReplaceTempView("data")

//here we parse it
var safegraphDf = spark .sql("""SELECT ad_id,id_type,ST_Point(CAST(data.longitude AS Decimal(24,20)), CAST(data.latitude AS Decimal(24,20))) as pointshape
              FROM data  """)

safegraphDf.createOrReplaceTempView("data")


//performing the join

val intersection = spark.sql(
      """SELECT  *   FROM poligonomagico,data   WHERE ST_Contains(poligonomagico.myshape, data.pointshape)""")
.drop("pointshape","myshape")

intersection.explain(extended=true)


val output_name = (polygon_inputLocation.split("/").last).split(".json") (0).toString

intersection.groupBy("ad_id","id_type","name").agg(count("name") as "frequency")
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/geo_processed/%s_%s_%s_sjoin_polygon".format(
  output_name,
  nDays,
  country))

}


def match_sample_to_polygons (spark: SparkSession,
      data_path: String,
      polygon_inputLocation: String,
      country: String) 
      //nDays: String,
      //since: String,
      {



//Load the polygon
val inputLocation = polygon_inputLocation
val allowTopologyInvalidGeometris = true // Optional
val skipSyntaxInvalidGeometries = true // Optional
val spatialRDD = GeoJsonReader.readToGeometryRDD(spark.sparkContext, inputLocation,allowTopologyInvalidGeometris, skipSyntaxInvalidGeometries)

//Transform the polygon to DF
var rawSpatialDf = Adapter.toDf(spatialRDD,spark)
.withColumnRenamed("_c1","CVEGEO")
.withColumnRenamed("_c2","NOM_ENT")
.withColumnRenamed("_c3","NOM_MUN")


/*

.withColumnRenamed("_c1","CO_FRAC_RA")
.withColumnRenamed("_c2","BARRIO")


.withColumnRenamed("_c1","IN1")
.withColumnRenamed("_c2","NAM")
.withColumnRenamed("_c3","FNA")
.withColumnRenamed("_c4","PROVCODE")
.withColumnRenamed("_c5","PROVINCIA")
.repartition(50)
*/
//.withColumnRenamed("_c1","CVEGEO")
//.withColumnRenamed("_c2","NOM_ENT")
//.withColumnRenamed("_c3","NOM_MUN")


rawSpatialDf.createOrReplaceTempView("rawSpatialDf")



// Assign name and geometry columns to DataFrame
var spatialDf = spark.sql("""       select ST_GeomFromWKT(geometry) as myshape,* FROM rawSpatialDf""".stripMargin).drop("rddshape","geometry")

spatialDf.createOrReplaceTempView("poligonomagico")

spatialDf.show(2)

//Esto para levantar csv
/*
val df_safegraph = spark.read.format("csv")
                  .option("header",true)
                  //.option("delimiter","\t")
                  .load(data_path) 
                  //.toDF("ad_id","id_type","freq","geocode","latitude","longitude")
                  .withColumn("latitude",col("latitude").cast("Double"))
                  .withColumn("longitude",col("longitude").cast("Double"))
                                    .na.drop()
                                    .repartition(50)//"/datascience/geo/startapp/2019*"
                //.toDF("ad_id","timestamp","country","longitude","latitude","some")
*/

//Esto para levantar parquet

val df_safegraph = spark.read.format("parquet")
                  .load(data_path) 
                  .withColumn("latitude",col("latitude").cast("Double"))
                  .withColumn("longitude",col("longitude").cast("Double"))
                  .na.drop()
                  .select("geo_hash_7","latitude","longitude")
                  .dropDuplicates("geo_hash_7")
                  //"/datascience/geo/startapp/2019*"
                //.toDF("ad_id","timestamp","country","longitude","latitude","some")
  
               

df_safegraph.createOrReplaceTempView("data")

var safegraphDf = spark      .sql(""" SELECT *,ST_Point(CAST(data.longitude AS Decimal(24,20)),
                                                             CAST(data.latitude AS Decimal(24,20))) 
                                                             as pointshape
              FROM data
          """)

safegraphDf.createOrReplaceTempView("data")
//safegraphDf.show(2)

val intersection = spark.sql(
      """SELECT  *   FROM poligonomagico,data   WHERE ST_Contains(poligonomagico.myshape, data.pointshape)""")
.drop("myshape","geometry","pointshape")
//.select("ad_id","id_type","name") //"utc_timestamp",

            
//intersection.show(5)

//intersection.show(2)

val output_name = (polygon_inputLocation.split("/").last).split(".json") (0).toString


//.groupBy("polygon_name", "ad_id","id_type","utc_timestamp")
//.agg(count("polygon_name") as "frequency")
intersection
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/geo_processed/%s_%s_sjoin_polygon".format(
  output_name,
  country))

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
       //.config("spark.sql.shuffle.partitions", 2000)
       .appName("myGeoSparkSQLdemo").getOrCreate()
     
GeoSparkSQLRegistrator.registerAll(spark)

// Initialize the variables
val geosparkConf = new GeoSparkConf(spark.sparkContext.getConf)

//Logger.getRootLogger.setLevel(Level.WARN)

//"/datascience/geo/polygons/AR/radio_censal/radios_argentina_2010_geodevicer.json",
//
/*
match_users_to_polygons(spark,
  "/datascience/geo/POIs/barrios.geojson",
  "30",
  "3",
  "argentina")


match_sample_to_polygons(spark,
  "/datascience/geo/startapp/2019*",
  "/datascience/geo/POIs/aud_havas_nov_19.json",
    "CO")


*/
/*spark: SparkSession,
      nDays: String,
      since: String,
      country: String
      */


      match_sample_to_polygons(spark,
        "/datascience/geo/Reports/GCBA/Coronavirus/2020-03-31/geohashes_by_user_mexico",
        "/home/data/retargetly/dataset/GeoData/Shapefiles/shapefiles_mexico/municipal/MX_municipal_Updated.json",
        "mexico")

  }
}