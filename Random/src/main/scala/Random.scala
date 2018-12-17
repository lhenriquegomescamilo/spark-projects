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

    val audience_name = "danone_20261"
    val geopoints = spark.read.format("parquet").load("/datascience/geo/geopoints/%s".format(audience_name))
    val geocodes = geopoints.withColumn("geocode", ((abs($"latitude".cast("float"))*100).cast("int")*100000)+(abs($"longitude".cast("float")*100).cast("int")))
    val counts = geocodes.groupBy("geocode").count()
    counts.write.format("csv").option("sep", ",").save("/datascience/geo/counts/%s".format(audience_name))

    val audience_name = "danone_35936"
    val geopoints = spark.read.format("parquet").load("/datascience/geo/geopoints/%s".format(audience_name))
    val geocodes = geopoints.withColumn("geocode", ((abs($"latitude".cast("float"))*100).cast("int")*100000)+(abs($"longitude".cast("float")*100).cast("int")))
    val counts = geocodes.groupBy("geocode").count()
    counts.write.format("csv").option("sep", ",").save("/datascience/geo/counts/%s".format(audience_name))

    val audience_name = "danone_35928"
    val geopoints = spark.read.format("parquet").load("/datascience/geo/geopoints/%s".format(audience_name))
    val geocodes = geopoints.withColumn("geocode", ((abs($"latitude".cast("float"))*100).cast("int")*100000)+(abs($"longitude".cast("float")*100).cast("int")))
    val counts = geocodes.groupBy("geocode").count()
    counts.write.format("csv").option("sep", ",").save("/datascience/geo/counts/%s".format(audience_name))
  }
}
