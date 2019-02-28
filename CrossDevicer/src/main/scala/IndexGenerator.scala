package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split, lit, explode, col, concat, collect_list, udf}
import org.apache.spark.sql.SaveMode

object IndexGenerator {
  
    /**
    * This method generates an index with 4 columns: index, index_type, device, device_type.
    * The index is a device_id that will be used to match a given audience. The index_type
    * specifies the type of the device_id in the index column.
    * The device is the device_id associated to the given index. This is the device_id that
    * will be returned as a result of the cross-device. The device_type is the type of device
    * in the device column.
    *
    * @param spark: Spark Session that will be used to read the DataFrames.
    *
    * As a result it stores a DataFrame as a Parquet folder in /datascience/crossdevice/double_index.
    **/
    def generate_index_double(spark: SparkSession) {
        // This is the path to the last DrawBridge id
        val drawBridgePath = "/data/crossdevice/2019-02-21/*.gz"
        // First we obtain the data from DrawBridge
        val db_data = spark.read.format("csv").load(drawBridgePath)
        // Now we obtain a dataframe with only two columns: index (the device_id), and the device type
        val index = db_data.withColumn("all_devices", split(col("_c0"), "\\|")) // First we get the list of all devices
                           .withColumn("index", explode(col("all_devices"))) // Now we generate the index column for every device in the list
                           .withColumn("index_type", col("index").substr(lit(1), lit(3))) // We obtain the device type just checking the first 3 letters
                           .withColumn("index", split(col("index"), ":"))  
                           .withColumn("index", col("index").getItem(1)) // Now we obtain the device_id
                           .withColumn("device", explode(col("all_devices"))) // In this part we get the device that have matched
                           .withColumn("device_type", col("device").substr(lit(1), lit(3))) // We obtain the device type just checking the first 3 letters
                           .withColumn("device", split(col("device"), ":"))  
                           .withColumn("device", col("device").getItem(1)) // Now we obtain the device_id
                           .filter("index_type != 'd' AND device_type != 'd'")
                           .select("index", "index_type", "device", "device_type")

        // We don't want more than 120 files per folder
        index.coalesce(120).write.mode(SaveMode.Overwrite).format("parquet")
                                .partitionBy("index_type", "device_type")
                                .save("/datascience/crossdevice/double_index")
    }
  
    /**
    * This method generates an index where the key is a cookie id and it has 3 columns associated: androids, cookies, and ios.
    * Each of these lists is just the list of devices of every type associated to the key, given by DrawBridge.
    * 
    * @param spark: Spark Session that will be used to read the DataFrames.
    *
    * As a result it stores a DataFrame as a Parquet folder in /datascience/crossdevice/list_index.
    **/
    def generate_index_lists(spark: SparkSession) {
        // First of all, we generate a DataFrame with three columns. The first column is a cookie, 
        // the second one is a list of devices coming from such cookie,
        // the third column is the list of types that corresponds to the devices
        val df = spark.read.format("parquet").load("/datascience/crossdevice/double_index")
                                          .filter("index_type = 'coo'")
                                          .groupBy("index")
                                          .agg(collect_list("device") as "devices",
                                               collect_list("device_type") as "types")

        // This UDF takes two lists: devices and their corresponding types. As a result, it generates a tuple with three lists,
        // (cookies, androids, ios). Where cookies is the list of devices that are of type 'c' (a cookie).
        val udfDevice = udf((devices: Seq[String], types: Seq[String]) => 
                                                            ((devices zip types).filter(tuple => tuple._2.substring(0, 3)=="coo").map(tuple => tuple._1),
                                                             (devices zip types).filter(tuple => tuple._2.substring(0, 3)=="and").map(tuple => tuple._1),
                                                             (devices zip types).filter(tuple => tuple._2.substring(0, 3)=="ios").map(tuple => tuple._1),
                                                             (devices zip types).filter(tuple => tuple._2.substring(0, 3)=="con").map(tuple => tuple._1)))

        // Here we obtain the three lists and leave them as separate columns. Then we rename the index column as 'device_id'.
        val index_xd = df.withColumn("devices", udfDevice(col("devices"), col("types"))).cache
                         .withColumn("android", col("devices._2"))
                         .withColumn("ios",     col("devices._3"))
                         .withColumn("cookies", col("devices._1"))
                         .withColumn("tv", col("devices._4"))
                         .withColumnRenamed("index","device_id")
                         .drop("devices")

        // Finally, we store the index as a parquet folder, with no more than 200 files.
        index_xd.coalesce(200).write.mode(SaveMode.Overwrite).format("parquet")
                                .save("/datascience/crossdevice/list_index")
    }

    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("audience generator by keywords").getOrCreate()
        
        generate_index_double(spark)
        generate_index_lists(spark)     
    }
}
