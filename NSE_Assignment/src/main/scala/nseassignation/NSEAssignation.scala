package  main.scala.nseassignation


import main.scala.Main

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
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

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.serializer.KryoRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator


//////////////////////////////////////////////////////////////////////////////////

object NSE_assignation {

  def get_processed_homes(spark: SparkSession, value_dictionary: Map[String, String]) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    
     // First we get the audience. Also, we transform the device id to be upper case.
 		//cargamos los homes
		val homes = spark.read.format("csv")
					.option("delimiter","\t")
					.load("/datascience/geo/%s".format(value_dictionary("output_file")))
					.toDF("ad_id","id_type","freq","geocode","latitude","longitude")

		//Aplicando geometría a los puntos

		homes.createOrReplaceTempView("data")

		var safegraphDf = spark      .sql("""             
			SELECT ad_id,id_type,freq,ST_Point(CAST(data.longitude AS Decimal(24,20)), 
		                                            CAST(data.latitude AS Decimal(24,20)), 
		                                            data.ad_id,
		                                            data.id_type,
		                                            data.freq) AS pointshape
		              FROM data
		          """)
		          
		//safegraphDf.createOrReplaceTempView("user_homes")


		safegraphDf
  }

  def getPolygons(spark: SparkSession, value_dictionary: Map[String, String]) = {
   
				// Levantamos el polígono. Le aplicamos la geometría y lo unimos con los nombres...porque json y geospark
				val geojson_path_formated =      value_dictionary("path_to_polygons")

				//acá levantamos el geojson, nos quedamos con los nombres y el id
				val names = spark.read.json(geojson_path_formated).withColumn("rowId1", monotonically_increasing_id())

				//acá volvemos a levantar el json, pero nos quedamos con la geometría

				var polygonJsonDfFormated = spark.read      .format("csv")      .option("sep", "\t")      .option("header", "false")      .load(geojson_path_formated)
				      

				//le asignamos la geometría a lo que cargamos recién      
				polygonJsonDfFormated.createOrReplaceTempView("polygontable")
				var polygonDf = spark.sql("select ST_GeomFromGeoJSON(polygontable._c0) as myshape from polygontable"    ).withColumn("rowId1", monotonically_increasing_id())

				//unimos ambas en un solo dataframe
				val ageb_nse = names.join(polygonDf,Seq("rowId1"))
        .drop("rowId1").withColumn("CVEGEO",col("properties.CVEGEO"))
        .withColumn("NSE",col("properties.NSE_segment"))

				//ageb_nse.createOrReplaceTempView("ageb_nse_polygons")

				ageb_nse
  }


  def join(spark: SparkSession,value_dictionary: Map[String, String]) = {
    val polygonGDf = getPolygons(spark,value_dictionary)
    val homesGDF = get_processed_homes(spark,value_dictionary)

    polygonGDf.createOrReplaceTempView("ageb_nse_polygons")
    homesGDF.createOrReplaceTempView("user_homes")

   

    //hacemos el join propiamente dicho

val intersection = spark.sql(
      """SELECT  user_homes.ad_id,
      			user_homes.id_type,
      			user_homes.freq, 
      			user_homes.pointshape,
      			ageb_nse_polygons.myshape,
      			ageb_nse_polygons.NSE,
      			ageb_nse_polygons.CVEGEO
                
                FROM user_homes, ageb_nse_polygons
                WHERE ST_Contains(ageb_nse_polygons.myshape, user_homes.pointshape)""")
							.drop("pointshape","myshape")
                   
               
//intersection.drop("pointshape","myshape").write      .format("csv")      .option("sep", "\t")      .option("header", "true")      .mode(SaveMode.Overwrite)      .save("/datascience/geo/testPolygonsMX")                 


    intersection.write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/%s_w_NSE".format(value_dictionary("output_file")))
      
  }


  def main(args: Array[String]) {
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

    // Initialize the variables
    GeoSparkSQLRegistrator.registerAll(spark)
    Logger.getRootLogger.setLevel(Level.WARN)

    // Finally we perform the GeoJoin
    join(spark,value_dictionary)
}
//
}