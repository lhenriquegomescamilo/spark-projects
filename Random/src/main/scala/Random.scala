package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{upper, col,abs}
import org.apache.spark.sql.SaveMode


/**
 * The idea of this script is to run random stuff. Most of the times, the idea is
 * to run quick fixes, or tests.
 */
object Random {
  def main(args: Array[String]) {
    // First we obtain the Spark session
    val spark = SparkSession.builder.appName("Random").getOrCreate()

//    val udfFilter = udf((lat: String,lon: String) => (abs(lat.cast("float"))*100).cast("int")*100000+
//                                                      (abs(lon.cast("float")*100).cast("int")))
    val audiencias = List("danone_20261","danone_35936","danone_35928")
    for (audience_name <- audiencias)
    {
      val geopoints = spark.read.format("parquet").load("/datascience/geo/geopoints/%s".format(audience_name))
      val geocodes = geopoints.withColumn("geocode", abs(col("latitude").cast("float")*100).cast("int")*100000+
                                                            (abs(col("longitude").cast("float")*100).cast("int")))
      val counts = geocodes.groupBy("geocode").count()
      counts.write.format("csv").option("sep", ",").save("/datascience/geo/counts/%s".format(audience_name)
    }
}
}
