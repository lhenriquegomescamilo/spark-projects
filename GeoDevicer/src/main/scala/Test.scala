package main.scala

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

object Test {
  def get_safegraph_data(spark: SparkSession) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(1)
    val days = (0 until 3)
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
      .csv(
        "/data/geo/safegraph/2019/06/07/part-00000-tid-5892072171300048383-aeb39518-b59c-4473-82b6-4e06da388b9d-797647-c000.csv.gz"
      )
      .filter("country = '%s'".format("mexico"))
      .select("ad_id", "id_type", "latitude", "longitude", "utc_timestamp")

    df_safegraph.createOrReplaceTempView("data")
    var safegraphDf = spark
      .sql("""
              SELECT data.ad_id,
                     data.id_type,
                     data.utc_timestamp,
                     data.latitude,
                     data.longitude,
                     ST_Point(CAST(data.longitude AS Decimal(24,20)), 
                              CAST(data.latitude AS Decimal(24,20)), 
                              data.ad_id,
                              data.id_type,
                              data.utc_timestamp) AS pointshape
              FROM data
          """)

    safegraphDf
  }

  def getPolygons(spark: SparkSession) = {
    //Establecemos el path del geojson. Vamos a levantarlos dos veces.

    val geojson_path_formated =
      "/datascience/geo/polygons/MX/NSE/geojson/MX_ageb_NSE_formatted"

    //Primer carga. Tomamos nombres
    //acá levantamos el geojson como JSON,  lo vamos a usar para quedarnos con los nombres. Le asignamos un ID ficticio al dataframe.
    val names = spark.read
      .json(geojson_path_formated)
      .withColumn("rowId1", monotonically_increasing_id())
      .withColumn("CVEGEO", col("properties.CVEGEO"))
      .select("rowId1", "CVEGEO")

    //Segunda carga. Tomamos poligonos
    //acá volvemos a levantar el geojson como CSV. De esta manera geospark puede leerlo como tal y asignarle la geometría.
    var polygonJsonDfFormated = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "false")
      .load(geojson_path_formated)

    //le asignamos la geometría a lo que cargamos recién y le creamos un ID ficticio al dataframe para poder hacer un join con el anterior.
    polygonJsonDfFormated.createOrReplaceTempView("polygontable")
    var polygonDf = spark
      .sql(
        """SELECT ST_GeomFromGeoJSON(polygontable._c0) AS myshape 
           FROM polygontable"""
      )
      .withColumn("rowId1", monotonically_increasing_id())

    //Join entre nombres y polígonos
    //unimos ambas en un solo dataframe
    val ageb_nse = names
      .join(polygonDf, Seq("rowId1"))
      .drop("rowId1")

    ageb_nse
  }

  def join(spark: SparkSession) = {
    val polygonDf = getPolygons(spark)
    val sg_data = get_safegraph_data(spark)

    polygonDf.createOrReplaceTempView("polygons")
    sg_data.createOrReplaceTempView("safegraph")

    val getSafegraphData = (point: Geometry) =>
      Seq(
        Seq(point.asInstanceOf[Point].getX().toString),
        Seq(point.asInstanceOf[Point].getY().toString),
        point.getUserData().toString.replaceAll("\\s{1,}", ",").split(",").toSeq
      )
    spark.udf.register("getSafegraphData", getSafegraphData)
    val getPolygonData = (point: Geometry) =>
      point.getUserData().toString.replaceAll("\\s{1,}", ",").split(",").toSeq
    spark.udf.register("getPolygonData", getPolygonData)

    val intersection = spark.sql(
      """SELECT safegraph.ad_id,
                safegraph.id_type,
                safegraph.utc_timestamp,
                safegraph.latitude,
                safegraph.longitude, 
                polygons.CVEGEO as polygonId
         FROM safegraph, polygons
         WHERE ST_Contains(polygons.myshape, safegraph.pointshape)"""
    )

    intersection.write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/testPolygons")
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
      // .config("geospark.join.gridtype", "rtree")
      .appName("match_POI_geospark")
      .getOrCreate()

    // Initialize the variables
    GeoSparkSQLRegistrator.registerAll(spark)
    Logger.getRootLogger.setLevel(Level.WARN)

    // Finally we perform the GeoJoin
    join(spark)
  }
}
