package main.scala

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.SparkSession


object FromEventqueuePII {

  /**
  * Given a particular day, this method downloads the data from the eventqueue to build a PII table. Basically, it takes the following columns:
  * device_id, device_type, country, id_partner, data_type, ml_sh2, mb_sh2, nid_sh2, day.
  * 
  * Only a small sample of the eventqueue is obtained, so it does not take so much memory space.
  */
  def getPII(spark: SparkSession, day: String) {
    // First we load the data
    val filePath = "/data/eventqueue/%s/*.tsv.gz".format(day)
    val data = spark.read.format("csv").option("sep", "\t").option("header", "true").load(filePath)

    // Now we process the data and store it
    data.withColumn("day", lit(day.replace("/", "")))
        .filter(col("data_type").contains("hash") && (col("ml_sh2").isNotNull ||  col("mb_sh2").isNotNull ||  col("nid_sh2").isNotNull ))
        .select( "device_id", "device_type","country","id_partner","data_type","ml_sh2", "mb_sh2", "nid_sh2","day")
        .write
        .format("parquet")
        .mode(SaveMode.Append)
        .partitionBy("day")
        .save("/datascience/pii_matching/pii_tuples")
	}


  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Get Pii from Eventqueue").getOrCreate()

    // Here we obtain the list of days to be downloaded
    val nDays = 15
    val from = 1
    val format = "yyyy/MM/dd"
    val end   = DateTime.now.minusDays(from)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    // Now we effectively download the data day by day
    days.map(day => getPII(spark, day))

  }

}
