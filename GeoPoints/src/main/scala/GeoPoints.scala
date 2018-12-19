package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{upper, col}
import org.apache.spark.sql.SaveMode
import org.joda.time.{DateTime, Days}

object GeoPoints {
  /**
   * This method obtains the list of geopoints (latitude, longitude, and timestamp) for a given set of 
   * madids. The idea is to load all the madids at the beginning, and these madids originally come from
   * a cross-device. Once the madids are loaded, they are used to extract their information from
   * SafeGraph dataset for the last N days.
   */
  def get_geo_points(spark: SparkSession, audience_name: String, 
                     output_type: String = "csv", ndays: Int = 30) {
    // First of all, we load the cross-deviced audience
    val path_in = "/datascience/audiences/crossdeviced/%s_xd".format(audience_name)
    val cross_deviced = spark.read.format("csv").load(path_in)
                                  .withColumnRenamed("_c0", "index").withColumn("index", upper(col("index")))
                                  .withColumnRenamed("_c1", "device")
                                  .withColumnRenamed("_c2", "device_type")
                                  .select("device", "device_type").distinct()
    
    // Now we load the data from SafeGraph
    val path_sg = "/data/geo/safegraph/"
    val format = "yyyy/MM/dd"
    val start = DateTime.now.minusDays(20)
    val end   = DateTime.now.minusDays(0)
    val daysCount = Days.daysBetween(start, end).getDays()
    
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val range = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))
    val hdfs_files = range.map(day => path_sg+"%s/".format(day))
                          .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    
    val geodata = hdfs_files.map(file => spark.read.format("csv").option("header", "true").load(file))
                             .reduce((df1, df2) => df1.union(df2))
                             .withColumn("ad_id", upper(col("ad_id")))
    
    // Here we perform the join between both datasets
    val geo_points = geodata.join(cross_deviced, cross_deviced.col("index")===geodata.col("ad_id"))
                             .select("device", "device_type", "utc_timestamp", "latitude", "longitude")
    
    // Finally, we save all the information in a new folder
    val path_out = "/datascience/geo/geopoints/%s".format(audience_name)
    geo_points.write.format("csv").mode(SaveMode.Overwrite).save(path_out)
  }
  
  def main(args: Array[String]) {
    // First of all, we parse the parameters
    val usage = """
        Error while reading the parameters
        Usage: GetPoints.jar audience_name
      """
    if (args.length == 0) println(usage)
    val audience_name = args.last
    
    // First we obtain the Spark session
    val spark = SparkSession.builder.appName("audience generator by keywords").getOrCreate()
    
    // Finally, we obtain the geo points
    get_geo_points(spark, audience_name)
  }
}