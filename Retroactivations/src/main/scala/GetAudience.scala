package main.scala

import org.apache.spark.sql.SparkSession
import org.joda.time.{Days, DateTime}
import org.apache.spark.sql.SaveMode

/*
 * This object receives an audience and cross-device it using a cross-deviced index.
 * The result is stored in a new folder.
 */
object GetAudience {
  def cross_device(spark: SparkSession, path_audience: String, index_filter: String){
    // First we get the audience. Also, we transform the device id to be upper case.
    //val path_audience = "/datascience/audiences/output/%s".format(audience_name)
    val audience_name = path_audience.split("/").last
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