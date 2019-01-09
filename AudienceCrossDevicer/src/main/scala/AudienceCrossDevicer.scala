package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{upper, col}
import org.apache.spark.sql.SaveMode

/*
 * This object receives an audience and cross-device it using a cross-deviced index.
 * The result is stored in a new folder.
 */
object AudienceCrossDevicer {
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
  def cross_device(spark: SparkSession, path_audience: String, index_filter: String){
    // First we get the audience. Also, we transform the device id to be upper case.
    //val path_audience = "/datascience/audiences/output/%s".format(audience_name)
    val audience_name = path_audience.split("/").last
    val audience = spark.read.format("csv").option("sep", " ").load(path_audience)
                                                              .withColumnRenamed("_c0", "device_id")
                                                              .withColumn("device_id", upper(col("device_id")))
    
    // Get DrawBridge Index. Here we transform the device id to upper case too.
    val db_data = spark.read.format("parquet").load("/datascience/crossdevice/double_index")
                                              .filter(index_filter)
                                              .withColumn("index", upper(col("index")))
                                              .select("index", "device", "device_type")
                                              
    // Here we do the cross-device per se.
    val cross_deviced = db_data.join(audience, db_data.col("index")===audience.col("device_id"))
                               //.select("index", "device", "device_type")
    
    // Finally, we store the result obtained.
    val output_path = "/datascience/audiences/crossdeviced/%s_xd".format(audience_name)
    cross_deviced.write.format("csv").mode(SaveMode.Overwrite).save(output_path)
  }
  
  
  def main(args: Array[String]) {
    // First of all, we parse the parameters
    val usage = """
        Error while reading the parameters
        Usage: AudienceCrossDevicer.jar [index-filter] audience_name
      """
    if (args.length == 0) println(usage)
    val audience_name = args.last
    val index_filter = if (args.length>1) args(0) else ""
    
    // First we obtain the Spark session
    val spark = SparkSession.builder.appName("audience generator by keywords").getOrCreate()
    
    // Finally, we perform the cross-device
    cross_device(spark, audience_name, index_filter)
  }
  
}
