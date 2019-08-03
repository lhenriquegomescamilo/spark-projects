package main.scala.devicer
import main.scala.crossdevicer.AudienceCrossDevicer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.conf.Configuration
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import java.time.DateTimeException

object GetCreatedSegments {

  def getCreatedSegments(spark: SparkSession) {
    val lastDay = DateTime.now.minusDays(1).toString("yyyyMMdd")
    spark.read
      .option("basePath", "/datascience/data_audiences_streaming/")
      .load("/datascience/data_audiences_streaming/hour=%s*/".format(lastDay))
      .filter("event_type IN ('campaign', 'retroactive')")
      .select("segments")
      .withColumn("segments", explode(col("segments")))
      .distinct()
      .write
      .format("csv")
      .save("/datascience/devicer/createdSegments/day=%s".format(lastDay))
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("Get created segments by the devicer")
      .getOrCreate()
    
    getCreatedSegments(spark)
  }
}
