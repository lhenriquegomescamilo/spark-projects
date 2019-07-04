package main.scala.matchers

import main.scala.Main

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

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.serializer.KryoRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator

object PolygonMatcher {

  /**
    * This method reads the safegraph data, selects the columns "ad_id" (device id), "id_type" (user id), "latitude", "longitude", creates a
    * geocode for each row and future spatial operations and finally removes duplicates users that were detected in the same
    * location (i.e. the same user in different lat long coordinates will be conserved, but the same user in same lat long coordinates will be dropped).
    *
    * @param spark: Spark session that will be used to load the data.
    * @param value_dictionary: Map that contains all the necessary information to run the match. The following fields are required:
    *        - nDays: number of days to be loaded from Safegraph data.
    *        - since: number of days to be skipped in Safegraph data.
    *        - country: country for which the Safegraph data is going to be extracted from.

    * @return df_safegraph: dataframe created with the safegraph data, filtered by the desired country, extracting the columns user id, device id, latitude and
    * longitude removing duplicate users that have repeated locations and with added geocode.
    */
  def get_safegraph_data(
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(value_dictionary("since").toInt)
    val days = (0 until value_dictionary("nDays").toInt)
      .map(end.minusDays(_))
      .map(_.toString(format))

    // Now we obtain the list of hdfs files to be read
    val path = "/data/geo/safegraph/"
    val hdfs_files = days
      .map(day => path + "%s/".format(day))
      .filter(
        path => fs.exists(new org.apache.hadoop.fs.Path("/data/geo/safegraph/"))
      )
      .map(day => day + "*.gz")

    // Finally we read, filter by country, rename the columns and return the data
    val df_safegraph = spark.read
      .option("header", "true")
      .csv(hdfs_files: _*)
      .filter("country = '%s'".format(value_dictionary("country")))
      .select("ad_id", "id_type", "latitude", "longitude", "utc_timestamp")

    df_safegraph.createOrReplaceTempView("data")
    var safegraphDf = spark
      .sql("""
          SELECT ad_id,
                  id_type,
                  latitude,longitude,
                  utc_timestamp,
                  ST_Point(CAST(data.longitude AS Decimal(24,20)), 
                                            CAST(data.latitude AS Decimal(24,20)), 
                                            data.ad_id) AS pointshape
              FROM data
      """)

    safegraphDf
  }

  /**
    * This method reads the user provided POI dataset, and renames the columns. The file provided must be correctly formatted as described below.
    *
    * @param spark: Spark session that will be used to load the data.
    * @param value_dictionary: Map that contains all the necessary information to run the match. The following fields are required:
    *        - max_radius: maximum distance allowed in the distance join.
    *        - path_to_pois: path where the POIs are stored.
    *
    * @param return df_pois_final: dataframe created from the one provided by the user containing the POIS: contains the geocode and renamed columns.
    */
  def getPolygons(
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {


    // Acá leemos el polígono que entrega python, en formato json. Va a tener unas columnas incorrectas, así que las tiramos
    val df_polygons_raw = spark.read.json(value_dictionary("path_to_pois")).drop("_corrupt_record").na.drop()

    val geojson_path_formated =      "/datascience/geo/polygons/AR/audiencias/geospark_format/%s".format(value_dictionary("path_to_pois").split("/").last)

    df_polygons_raw.write.format("json").mode(SaveMode.Overwrite).save (geojson_path_formated)

  
  // Levantamos el polígono. Le aplicamos la geometría y lo unimos con los nombres...porque json y geospark
        

  //acá levantamos el geojson, nos quedamos con los nombres y el id
    val names = spark.read.json(geojson_path_formated).withColumn("rowId1", monotonically_increasing_id())

  //acá volvemos a levantar el json, pero nos quedamos con la geometría
    var polygonJsonDfFormated = spark.read      .format("csv")      .option("sep", "\t")      .option("header", "false")      .load(geojson_path_formated)
              
  //le asignamos la geometría a lo que cargamos recién      
   polygonJsonDfFormated.createOrReplaceTempView("polygontable")
   var polygonDf = spark.sql("select ST_GeomFromGeoJSON(polygontable._c0) as myshape from polygontable"    ).withColumn("rowId1", monotonically_increasing_id())

  //unimos ambas en un solo dataframe
  val poligono = names.join(polygonDf,Seq("rowId1")).drop("geometry","rowId1")

  //poligono.createOrReplaceTempView("poligono_amigo")
  poligono

  }

  /**
    * This method performs the vincenty distance calculation between the POIs and the Safegraph data for the selected country and days.
    * To do this, we perform a join between both dataframes using GeoSpark. This is useful when both dataframes are large, since GeoSpark
    * creates and index that will basically map the points into buckets based on the Geo position.
    *
    * @param spark: Spark session that will be used to load the data.
    * @param value_dictionary: Map that contains all the necessary information to run the match. The following fields are required:
    *        - poi_output_file: path where the result is going to be stored.
    *        - max_radius: maximum distance allowed in the distance join.
    *        - nDays: number of days to be loaded from Safegraph data.
    *        - since: number of days to be skipped in Safegraph data.
    *        - country: country for which the Safegraph data is going to be extracted from.
    *        - path_to_pois: path where the POIs are stored.
    *
    * @param return df_pois_final: dataframe created from the one provided by the user containing the POIS: contains the geocode and renamed columns.
    */
  def join(spark: SparkSession, value_dictionary: Map[String, String]) = {
    
    val polygonGDf = getPolygons(spark,value_dictionary)
    val safegraphDf = get_safegraph_data(spark,value_dictionary)

    polygonGDf.createOrReplaceTempView("poligono_amigo")
    safegraphDf.createOrReplaceTempView("users")

    //hacemos el join propiamente dicho
    val intersection = spark.sql(
      """SELECT  *  FROM users, poligono_amigo       WHERE ST_Contains(poligono_amigo.myshape, users.pointshape)""")
          .withColumnRenamed("ad_id", "device_id")
          .withColumnRenamed("id_type", "device_type")
          .select(col("*"), col("properties.*")).drop(col("properties"))
                   
    intersection.write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/%s".format(value_dictionary("output_file")))
      
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
    // Parse the parameters
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
      // .config("geospark.global.index", "true")
      // .config("geospark.global.indextype", "rtree")
      .config("geospark.join.gridtype", "kdbtree")
      // .config("geospark.join.numpartition", 200)
      .appName("match_POI_geospark")
      .getOrCreate()

    // Initialize the variables
    GeoSparkSQLRegistrator.registerAll(spark)
    val value_dictionary = Main.get_variables(spark, path_geo_json)
    Logger.getRootLogger.setLevel(Level.WARN)

    // Now we remove the file if it exists already
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val outPutPath =
      "/datascience/geo/%s".format(value_dictionary("poi_output_file"))
    if (fs.exists(new Path(outPutPath)))
      fs.delete(new Path(outPutPath), true)

    // Finally we perform the GeoJoin
    join(spark, value_dictionary)
  }
}
