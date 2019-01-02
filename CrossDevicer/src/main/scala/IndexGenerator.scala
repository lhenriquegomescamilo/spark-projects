package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split, lit, explode, col, concat, collect_list, udf}
import org.apache.spark.sql.SaveMode

object IndexGenerator {
  
    def generate_index_double(spark: SparkSession) {
        // First we obtain the data from DrawBridge
        val db_data = spark.read.format("csv").load("/data/crossdevice/2018-12-21/*.gz")
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
                                .save("/datascience/crossdevice/double_index")
    }
  
    def generate_index_lists(spark: SparkSession) {
        val df = spark.read.format("parquet").load("/datascience/crossdevice/double_index")
                                          .filter("index_type = 'c'")
                                          .withColumn("device",concat(col("device_type"),col("device")))
                                          .groupBy("index")
                                          .agg(collect_list("device"))

        val udfDevice = udf((segments: Seq[String], device_type: String) => segments.filter(segment => segment.charAt(0) == device_type)
                                                            .map(segment => segment.substring(1,segment.length)))

        val index_xd = df.withColumn("android",udfDevice(col("collect_list(device)"), lit("a")))
                         .withColumn("ios",     udfDevice(col("collect_list(device)"), lit("i")))
                         .withColumn("cookies", udfDevice(col("collect_list(device)"), lit("c")))
                         .withColumnRenamed("index","device_id")
                         .drop("collect_list(device)")


        index.coalesce(200).write.mode(SaveMode.Overwrite).format("parquet")
                                .save("/datascience/crossdevice/list_index")
    }

    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("audience generator by keywords").getOrCreate()
        
        generate_index_double(spark)                
    }
}
