package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs._

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
  def generate_index_double(spark: SparkSession, individual: Boolean) {
    // // This is the path to the last DrawBridge id
    // val drawBridgePath = "/data/crossdevice/2019-04-04/*.gz"
    // // First we obtain the data from DrawBridge
    // val db_data = spark.read.format("csv").load(drawBridgePath)
    // // Now we obtain a dataframe with only two columns: index (the device_id), and the device type
    // val index = db_data
    //   .withColumn("all_devices", split(col("_c0"), "\\|")) // First we get the list of all devices
    //   .withColumn("index", explode(col("all_devices"))) // Now we generate the index column for every device in the list
    //   .withColumn("index_type", col("index").substr(lit(1), lit(3))) // We obtain the device type just checking the first 3 letters
    //   .withColumn("index", split(col("index"), ":"))
    //   .withColumn("index", col("index").getItem(1)) // Now we obtain the device_id
    //   .withColumn("device", explode(col("all_devices"))) // In this part we get the device that have matched
    //   .withColumn("device_type", col("device").substr(lit(1), lit(3))) // We obtain the device type just checking the first 3 letters
    //   .withColumn("device", split(col("device"), ":"))
    //   .withColumn("device", col("device").getItem(1)) // Now we obtain the device_id
    //   .filter("index_type != 'd' AND device_type != 'd'")
    //   .select("index", "index_type", "device", "device_type")

    // // We don't want more than 120 files per folder
    // index
    //   .coalesce(120)
    //   .write
    //   .mode(SaveMode.Overwrite)
    //   .format("parquet")
    //   .partitionBy("index_type", "device_type")
    //   .save("/datascience/crossdevice/double_index")

    // First we retrieve the file that contains all the retargetly ids from TapAd
    val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val files = hdfs.listStatus(new Path("/data/crossdevice/"))
    val last_file = files
      .map(_.getPath())
      .map(_.toString)
      // .filter(_.contains("Retargetly_ids_full"))
      .filter(!_.contains("trigger"))
      .toList
      .sorted
      .last

    // Now we load the data and generate 3 columns: device, device_type, and tapad_id
    val mapUDF = udf(
      (device_type: String) =>
        if (device_type == "RTG") "coo"
        else if (device_type.toLowerCase.contains("android")) "and"
        else "ios"
    )
    val data = spark.read
      .format("csv")
      .option("sep", ";")
      .load(last_file)
      .repartition(300)
      .withColumn("device", explode(split(col("_c2"), "\t")))
      .withColumnRenamed(if (individual) "_c1" else "_c0", "tapad_id") // Here I will use the household id instead of the Individual id
      .withColumn("device", split(col("device"), "="))
      .withColumn("device_type", col("device").getItem(0))
      .withColumn("device", col("device").getItem(1))
      .withColumn("device_type", mapUDF(col("device_type")))
      .select("tapad_id", "device", "device_type")

    // Then we perform a self-join based on the tapad_id
    val index = data
      .withColumnRenamed("device", "index")
      .withColumnRenamed("device_type", "index_type")
      .join(data, Seq("tapad_id"))
      .na
      .fill("")
      .select("index", "index_type", "device", "device_type")

    println(index.printSchema)

    val path =
      if (individual) "/datascience/crossdevice/double_index_individual"
      else "/datascience/crossdevice/double_index"
    // Finally we store the results
    index
      .coalesce(120)
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .partitionBy("index_type", "device_type")
      .save(path)
  }

  /**
    * This method generates an index where the key is a cookie id and it has 3 columns associated: androids, cookies, and ios.
    * Each of these lists is just the list of devices of every type associated to the key, given by DrawBridge.
    *
    * @param spark: Spark Session that will be used to read the DataFrames.
    *
    * As a result it stores a DataFrame as a Parquet folder in /datascience/crossdevice/list_index.
    **/
  def generate_index_lists(spark: SparkSession, individual: Boolean) {
    // First of all, we generate a DataFrame with three columns. The first column is a cookie,
    // the second one is a list of devices coming from such cookie,
    // the third column is the list of types that corresponds to the devices
    val path =
      if (individual) "/datascience/crossdevice/double_index_individual"
      else "/datascience/crossdevice/double_index"
    val df = spark.read
      .format("parquet")
      .load(path)
      .filter("index_type = 'coo'")
      .groupBy("index")
      .agg(
        collect_list("device") as "devices",
        collect_list("device_type") as "types"
      )

    // This UDF takes two lists: devices and their corresponding types. As a result, it generates a tuple with three lists,
    // (cookies, androids, ios). Where cookies is the list of devices that are of type 'c' (a cookie).
    val udfDevice = udf(
      (devices: Seq[String], types: Seq[String]) =>
        (
          (devices zip types)
            .filter(tuple => tuple._2.substring(0, 3) == "coo")
            .map(tuple => tuple._1),
          (devices zip types)
            .filter(tuple => tuple._2.substring(0, 3) == "and")
            .map(tuple => tuple._1),
          (devices zip types)
            .filter(tuple => tuple._2.substring(0, 3) == "ios")
            .map(tuple => tuple._1)
        )
    )

    // Here we obtain the three lists and leave them as separate columns. Then we rename the index column as 'device_id'.
    val index_xd = df
      .withColumn("devices", udfDevice(col("devices"), col("types")))
      .cache
      .withColumn("android", col("devices._2"))
      .withColumn("ios", col("devices._3"))
      .withColumn("cookies", col("devices._1"))
      .withColumnRenamed("index", "device_id")
      .drop("devices")

    // Finally, we store the index as a parquet folder, with no more than 200 files.
    index_xd
      .coalesce(200)
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .save("/datascience/crossdevice/list_index")
  }

  type OptionMap = Map[Symbol, Int]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case "--individual" :: tail =>
        nextOption(map ++ Map('individual -> 0), tail)
    }
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("audience generator by keywords")
      .getOrCreate()

    val options = nextOption(Map(), args.toList)
    val individual = if (options.contains('individual)) true else false
    println("\n\nLOGGER: RUNNING INDEX DOUBLE\n\n")
    generate_index_double(spark, individual)
    println("\n\nLOGGER: RUNNING INDEX LIST\n\n")
    generate_index_lists(spark, individual)
  }
}
