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
  }
  
}