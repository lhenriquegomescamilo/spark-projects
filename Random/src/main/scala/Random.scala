package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{upper, col}
import org.apache.spark.sql.SaveMode
import scala.math.{abs}

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

    val audience_name2 = "danone_35936"
    val geopoints2 = spark.read.format("parquet").load("/datascience/geo/geopoints/%s".format(audience_name2))
    val geocodes2 = geopoints2.withColumn("geocode", ((abs($"latitude".cast("float"))*100).cast("int")*100000)+(abs($"longitude".cast("float")*100).cast("int")))
    val counts2 = geocodes2.groupBy("geocode").count()
    counts2.write.format("csv").option("sep", ",").save("/datascience/geo/counts/%s".format(audience_name2))

    val audience_name3 = "danone_35928"
    val geopoints3 = spark.read.format("parquet").load("/datascience/geo/geopoints/%s".format(audience_name3))
    val geocodes3 = geopoints3.withColumn("geocode", ((abs($"latitude".cast("float"))*100).cast("int")*100000)+(abs($"longitude".cast("float")*100).cast("int")))
    val counts3 = geocodes3.groupBy("geocode").count()
    counts3.write.format("csv").option("sep", ",").save("/datascience/geo/counts/%s".format(audience_name3))
  }
}
