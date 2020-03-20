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
      .load("/data/providers/Tapad/2020-03-06")
      //   .repartition(300)
      .withColumn("device", explode(split(col("_c2"), "\t")))
      .withColumnRenamed("_c1", "tapad_id")
      .withColumn("device", split(col("device"), "="))
      .withColumn("device_type", col("device").getItem(0))
      .withColumn("device", col("device").getItem(1))
      .withColumn("device_type", mapUDF(col("device_type")))
      .select("tapad_id", "device", "device_type")
      .distinct()

    data.write
      .format("csv")
      .mode("overwrite")
      .save("/datascience/custom/tapad_index_20200306")
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
    // parseIndex(spark)
    val january =
      spark.read
        .format("csv")
        .load("/datascience/custom/tapad_index_20200107")
        .select("_c0", "_c1")
        .withColumnRenamed("_c0", "tapad_jan")
        .withColumnRenamed("_c1", "device_id")
    val february =
      spark.read
        .format("csv")
        .load("/datascience/custom/tapad_index_20200203")
        .select("_c0", "_c1")
        .withColumnRenamed("_c0", "tapad_feb")
        .withColumnRenamed("_c1", "device_id")
    val march =
      spark.read
        .format("csv")
        .load("/datascience/custom/tapad_index_20200306")
        .select("_c0", "_c1")
        .withColumnRenamed("_c0", "tapad_mar")
        .withColumnRenamed("_c1", "device_id")

    january
      .join(february, Seq("device_id"))
      .write
      .format("parquet")
      .mode("overwrite")
      .save("/datascience/custom/tapad_change_jan_feb")
    february
      .join(march, Seq("device_id"))
      .write
      .format("parquet")
      .mode("overwrite")
      .save("/datascience/custom/tapad_change_feb_mar")
    january
      .join(march, Seq("device_id"))
      .write
      .format("parquet")
      .mode("overwrite")
      .save("/datascience/custom/tapad_change_jan_mar")
  }

}
