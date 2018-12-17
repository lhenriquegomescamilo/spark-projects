package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{upper, col}
import org.apache.spark.sql.SaveMode

object GeoPoints {
  /**
   * This method generates the cross device of a given audience. It takes the audience given
   * by parameters, then loads the Cross-Device Index and performs a join. Also, it allows 
   * to perform a filter in the cross-device index, so that only a part of the devices are
   * used for the operation.
   * 
   * @param spark: Spark session object that will be used to load and store the DataFrames.
   * @param audience_name: name of the folder or file where the audience is stored. This folder
   * must exist in /datascience/audiences/output folder. It must be a csv file, separated by spaces.
   * Also the first column must be called _c0.
   * @param index_filter: this is a filter that can be used to use only a part of the index. For
   * example: index_filter = "index_type = 'c' AND device_type IN ('a', 'i')". This example is used
   * to pass from a cookie to android and ios devices.
   * 
   * As a result, this method stores the cross-deviced audience in /datascience/audiences/crossdeviced
   * using the same name as the one sent by parameter.
   */
  def cross_device(spark: SparkSession, audience_name: String, index_filter: String){
    // First we get the audience. Also, we transform the device id to be upper case.
    val path_audience = "/datascience/audiences/output/%s".format(audience_name)
    val audience = spark.read.format("csv").option("sep", " ").load(path_audience)
                                                              .withColumnRenamed("_c0", "device_id")
                                                              .withColumn("device_id", upper(col("device_id")))
    
    // Get DrawBridge Index. Here we transform the device id to upper case too.
    val db_data = spark.read.format("parquet").load("/datascience/crossdevice")
                                              .filter(index_filter)
                                              .withColumn("index", upper(col("index")))
                                              .select("index", "device", "device_type")
                                              
    // Here we do the cross-device per se.
    val cross_deviced = db_data.join(audience, db_data.col("index")===audience.col("device_id"))
                               .select("index", "device", "device_type")
    
    // Finally, we store the result obtained.
    val output_path = "/datascience/audiences/crossdeviced/%s_xd".format(audience_name)
    cross_deviced.write.format("csv").mode(SaveMode.Overwrite).save(output_path)
  }
  
  /**
   * This method obtains the list of geopoints (latitude, longitude, and timestamp) for a given set of 
   * madids. The idea is to load all the madids at the beginning. These madids are 
   */
  def get_geo_points(spark: SparkSession, audience_name: String) {
    // First of all, we load the cross-deviced audience
    val path_in = "/datascience/audiences/crossdeviced/%s_xd".format(audience_name)
    val cross_deviced = spark.read.format("csv").load(path_in).select("device", "device_type").distinct()
    
    // Now we load the data from SafeGraph
    val path_sg = "/data/geo/safegraph/2018/12/*/*.gz"
    val geo_data = spark.read.format("csv").option("header", "true").load(path_sg)
                                           .select("ad_id", "utc_timestamp", "latitude", "longitude")
                                           .withColumn("ad_id", upper(col("ad_id")))
    
    // Here we perform the join between both datasets
    val geo_points = geo_data.join(cross_deviced, cross_deviced.col("device")===geo_data.col("ad_id"))
                             .select("device", "device_type", "utc_timestamp", "latitude", "longitude")
    
    // Finally, we save all the information in a new folder
    val path_out = "/datascience/geo/geopoints/%s".format(audience_name)
    geo_points.write.format("parquet").mode(SaveMode.Overwrite).save(path_out)
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
    
    // Now we perform the cross-device
    val index_filter = "index_type = 'c' AND device_type IN ('a', 'i')"
    cross_device(spark, audience_name, index_filter)
    
    // Finally, we obtain the geo points
    get_geo_points(spark, audience_name)
  }
}