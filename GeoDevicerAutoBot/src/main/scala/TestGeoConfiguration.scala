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

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.serializer.KryoRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
//import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator

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
//import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import org.apache.spark.sql.types.{DataType, StructType}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader

import org.datasyslab.geospark.utils.GeoSparkConf

object TestGeoConfiguration {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)
    var sparkSession: SparkSession = SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("geospark.global.index", "true")
      .config("geospark.join.gridtype", "kdbtree")
      .config("geospark.join.spatitionside", "right")
      .config(
        "spark.kryo.registrator",
        classOf[GeoSparkKryoRegistrator].getName
      )
      .appName("GeoSparkSQL-demo")
      .getOrCreate()

    GeoSparkSQLRegistrator.registerAll(sparkSession)

    // Initialize the variables
    val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)
    println(geosparkConf)
  }
}
