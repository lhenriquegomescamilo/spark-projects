package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{upper, col}
import org.apache.spark.sql.SaveMode

/**
 * The idea of this script is to run random stuff. Most of the times, the idea is
 * to run quick fixes, or tests.
 */
object Random {
  def main(args: Array[String]) {
    // First we obtain the Spark session
    val spark = SparkSession.builder.appName("Random").getOrCreate()
    
    val df = spark.read.format("parquet").load("/datascience/geo/geopoints/devices_58192")
    df.write.format("csv").option("sep", ",").save("/datascience/geo/geopoints/hcode")
  }
}