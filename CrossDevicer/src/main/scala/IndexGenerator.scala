package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split, lit, explode, col}
import org.apache.spark.sql.SaveMode

object IndexGenerator {
    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("audience generator by keywords").getOrCreate()
        
        // First we obtain the data from DrawBridge
        val db_data = spark.read.format("csv").load("/data/crossdevice/2018-11-15/*.gz")
        // Now we obtain a dataframe with only two columns: index (the device_id), and the device type
        val index = db_data.withColumn("all_devices", split(col("_c0"), "\\|")) // Primero obtenemos el listado de devices
                           .withColumn("index", explode(col("all_devices"))) // Now we generate the index column for every device in the list
                           .withColumn("index_type", col("index").substr(lit(1), lit(1))) // We obtain the device type just checking the first letter
                           .withColumn("index", split(col("index"), ":"))  
                           .withColumn("index", col("index").getItem(1)) // Now we obtain the device_id
                           .withColumn("device", explode(col("all_devices"))) // In this part we get the device that have matched
                           .withColumn("device_type", col("device").substr(lit(1), lit(1))) // We obtain the device type just checking the first letter
                           .withColumn("device", split(col("device"), ":"))  
                           .withColumn("device", col("device").getItem(1)) // Now we obtain the device_id
                           .filter("index_type != 'd' AND device_type != 'd'")
                           .select("index", "index_type", "device", "device_type")
        index.coalesce(120).write.mode(SaveMode.Overwrite).format("parquet")
                                .partitionBy("index_type", "device_type")
                                .save("/datascience/crossdevice")                           
    }
}
