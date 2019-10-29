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

import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}


import org.datasyslab.geospark.utils.GeoSparkConf


import org.datasyslab.geospark.formatMapper.GeoJsonReader




object SimpleMatcherRDD {

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



//Load the polygon as RDD
val inputLocation = polygon_inputLocation
val allowTopologyInvalidGeometris = true // Optional
val skipSyntaxInvalidGeometries = true // Optional
val spatialRDDpolygon = GeoJsonReader.readToGeometryRDD(spark.sparkContext, inputLocation,allowTopologyInvalidGeometris, skipSyntaxInvalidGeometries)



//Load the users
val df_safegraph = get_safegraph_data(spark,nDays,since,country)
df_safegraph.createOrReplaceTempView("data")

var safegraphDf = spark .sql("""SELECT ST_Point(CAST(data.longitude AS Decimal(24,20)), CAST(data.latitude AS Decimal(24,20))) as geometry,ad_id
              FROM data  """)
safegraphDf.createOrReplaceTempView("data")   



var spatialRDDusers = Adapter.toSpatialRdd(safegraphDf, "data")



//We validate the geometries
spatialRDDpolygon.analyze()
spatialRDDusers.analyze()

//Acá va a empezar lo que usaba. Voy a cambiar por una manera B
//Manera A
/*
//We perform the sptial join
  //setting variables
val joinQueryPartitioningType = GridType.KDBTREE
val numPartitions = 20

spatialRDDpolygon.spatialPartitioning(joinQueryPartitioningType,numPartitions)
spatialRDDusers.spatialPartitioning(spatialRDDpolygon.getPartitioner)

val considerBoundaryIntersection = true // Only return gemeotries fully covered by each query window in queryWindowRDD esto es muy importante, si no no joinea bien
val usingIndex = true
val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query

val result = JoinQuery.SpatialJoinQueryFlat(spatialRDDpolygon, spatialRDDusers, usingIndex, considerBoundaryIntersection)
// Fin the Manera A
*/

//Manera B
//Acá persistimos en memoria el poligono 
spatialRDDusers.spatialPartitioning(GridType.RTREE,200);
spatialRDDusers.buildIndex(IndexType.RTREE, true);
spatialRDDusers.indexedRDD.persist(StorageLevel.MEMORY_ONLY);
spatialRDDusers.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
spatialRDDpolygon.spatialPartitioning(spatialRDDusers.getPartitioner)
val result = JoinQuery.SpatialJoinQueryFlat(spatialRDDpolygon, spatialRDDusers, true, true);

var intersection = Adapter.toDf(result,spark).select("_c1","_c3").toDF("ad_id","name")


val output_name = (polygon_inputLocation.split("/").last).split(".json") (0).toString

intersection.dropDuplicates()//.groupBy("name", "ad_id").agg(count("name") as "frequency")
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/geo_processed/%s_%s_%s_sjoin_polygon".format(
  output_name,
  nDays,
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
       .appName("myGeoSparkSQLdemo").getOrCreate()
     
GeoSparkSQLRegistrator.registerAll(spark)

    Logger.getRootLogger.setLevel(Level.WARN)

// Initialize the variables
val geosparkConf = new GeoSparkConf(spark.sparkContext.getConf)

//Logger.getRootLogger.setLevel(Level.WARN)
//"/datascience/geo/POIs/natural_geodevicer.json",
//"/datascience/geo/polygons/AR/radio_censal/radios_argentina_2010_geodevicer.json",
//
match_users_to_polygons(spark,
  "/datascience/geo/polygons/AR/radio_censal/radios_argentina_2010_geodevicer.json",
  "63",
  "1",
  "argentina")
/*spark: SparkSession,
      nDays: String,
      since: String,
      country: String
      */

  }
}