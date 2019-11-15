package  main.scala.nseassignation

import main.scala.NSEFromHomes


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



//////////////////////////////////////////////////////////////////////////////////

object NSEAssignation {

  def get_processed_homes(spark: SparkSession, value_dictionary: Map[String, String]) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    
     // First we get the audience. Also, we transform the device id to be upper case.
 		//cargamos los homes
		val homes = spark.read.format("csv")
					.option("delimiter","\t")
					.load(("/datascience/geo/%s".format(value_dictionary("output_file"))))
					.toDF("ad_id","id_type","freq","geocode","latitude","longitude")

		//Aplicando geometría a los puntos
    homes

  }
///////////////////////////////////

def nse_join (spark: SparkSession,
     value_dictionary: Map[String, String]) {

//Load the polygon
val inputLocation = value_dictionary("path_to_polygons")
val allowTopologyInvalidGeometris = true // Optional
val skipSyntaxInvalidGeometries = true // Optional
val spatialRDD = GeoJsonReader.readToGeometryRDD(spark.sparkContext, inputLocation,allowTopologyInvalidGeometris, skipSyntaxInvalidGeometries)

//Transform the polygon to DF
var rawSpatialDf = Adapter.toDf(spatialRDD,spark).repartition(30)
rawSpatialDf.createOrReplaceTempView("rawSpatialDf")

// Assign name and geometry columns to DataFrame
var spatialDf = spark.sql("""       select *,ST_GeomFromWKT(geometry) as myshape FROM rawSpatialDf""".stripMargin)

spatialDf.createOrReplaceTempView("poligonomagico")
spatialDf.show(5)

//Here we get the modeled homes
val df_safegraph = get_processed_homes(spark,value_dictionary)

df_safegraph.createOrReplaceTempView("data")

var safegraphDf = spark      .sql("""             
      SELECT ad_id,id_type,freq,ST_Point(CAST(data.longitude AS Decimal(24,20)), 
                                                CAST(data.latitude AS Decimal(24,20)), 
                                                data.ad_id,
                                                data.id_type,
                                                data.freq) AS pointshape
                  FROM data
              """)
              
    //safegraphDf.createOrReplaceTempView("user_homes")


df_safegraph.createOrReplaceTempView("data")
df_safegraph.show(5)


//performing the join

val intersection = spark.sql(
      """SELECT  *   FROM poligonomagico,data   WHERE ST_Contains(poligonomagico.myshape, data.pointshape)""")
.drop("pointshape","myshape")

intersection.show(5)

 intersection.write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/%s_w_NSE".format(value_dictionary("output_file")))

}


 type OptionMap = Map[Symbol, Any]
 /**
    * This method parses the parameters sent.
    */

  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--path_geo_json" :: value :: tail =>
        nextOption(map ++ Map('path_geo_json -> value.toString), tail)
    }
  }

  def main(args: Array[String]) {


    val options = nextOption(Map(), args.toList)
    val path_geo_json =
      if (options.contains('path_geo_json)) options('path_geo_json).toString
      else ""


    // Start Spark Session
    val spark = SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config(
        "spark.kryo.registrator",
        classOf[GeoSparkKryoRegistrator].getName
      )
      .config("geospark.join.gridtype", "kdbtree")
      .appName("match_POI_geospark")
      .getOrCreate()

    val value_dictionary = NSEFromHomes.get_variables(spark, path_geo_json)

    // Initialize the variables
    GeoSparkSQLRegistrator.registerAll(spark)
    Logger.getRootLogger.setLevel(Level.WARN)
    
    

    // Finally we perform the GeoJoin
    nse_join(spark,value_dictionary)

    }
//
}