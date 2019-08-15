package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._

import java.security.MessageDigest
import java.util
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import java.time.DateTimeException
import java.sql.Savepoint

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object IpgMaids {
  def getDataIdPartners(
      spark: SparkSession,
      partnerIds: List[String],
      nDays: Int = 30,
      since: Int = 1,
      pipe: String = "batch"
  ): DataFrame = {
    println("DEVICER LOG: PIPELINE ID PARTNERS")
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path =
      if (pipe == "batch") "/datascience/data_partner/"
      else "/datascience/data_partner_streaming/"
    // Now we obtain the list of hdfs folders to be read
    val hdfs_files =
      if (pipe == "batch")
        partnerIds
          .flatMap(
            partner =>
              days
                .map(
                  day => path + "id_partner=" + partner + "/day=%s".format(day)
                )
          )
          .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      else
        partnerIds
          .flatMap(
            partner =>
              days
                .flatMap(
                  day =>
                    (0 until 24).map(
                      hour =>
                        path + "hour=%s%02d/id_partner=%s"
                          .format(day, hour, partner)
                    )
                )
          )
          .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df =
      if (hdfs_files.length > 0)
        spark.read.option("basePath", path).parquet(hdfs_files: _*)
      else
        spark.createDataFrame(
          spark.sparkContext.emptyRDD[Row],
          StructType(Array(StructField("empty", StringType, true)))
        )
    //fs.close()
    df
  }

  def getDataAcxiom(spark: SparkSession) {
    val dataBase =
      getDataIdPartners(spark, List("1008", "349"), 20, 1, "streaming")
        .select("device_id", "device_type")
        .withColumn("device_id", upper(col("device_id")))
        .filter("device_type = 'android' or device_type = 'ios'")
        .distinct()
    val dataAcxiom = spark.read
      .format("csv")
      .load(
        "/data/providers/acxiom/acxiom_AR_MAID_ONLY_XREF_Extract_20190806.txt.gz"
      )
      .select("_c0")
      .withColumnRenamed("_c0", "device_id")
      .withColumn("device_id", upper(col("device_id")))

    dataBase
      .join(dataAcxiom, Seq("device_id"))
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/join_Acxiom_AR")

  }

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    getDataAcxiom(spark)

  }
}
