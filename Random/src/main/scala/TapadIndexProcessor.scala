package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs._
import java.net.URLDecoder
import org.apache.log4j.{Level, Logger}

object TapadIndexProcessor {

  // Now we load the data and generate 3 columns: device, device_type, and tapad_id
  val mapUDF = udf(
    (device_type: String) =>
      if (device_type == "RTG") "coo"
      else if (device_type.toLowerCase.contains("android")) "and"
      else if (device_type == "SHT") "sht"
      else "ios"
  )

  def parseIndex(spark: SparkSession) {
    val data = spark.read
      .format("csv")
      .option("sep", ";")
      .load("/data/providers/Tapad/Retargetly_ids_full_20200203_083610.bz2")
      .repartition(300)
      .withColumn("device", explode(split(col("_c2"), "\t")))
      .withColumnRenamed("_c1", "tapad_id")
      .withColumn("device", split(col("device"), "="))
      .withColumn("device_type", col("device").getItem(0))
      .withColumn("device", col("device").getItem(1))
      .withColumn("device_type", mapUDF(col("device_type")))
      .select("tapad_id", "device", "device_type")

    data.write
      .format("csv")
      .mode("overwrite")
      .save("/datascience/custom/tapad_index_20200203")
  }

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("Get tapad index on demand")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    // Finally, we download the data
    parseIndex(spark)
  }

}
