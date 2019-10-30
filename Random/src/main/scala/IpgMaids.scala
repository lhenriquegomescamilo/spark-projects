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
  private val SALT: String =
    "jMhKlOuJnM34G6NHkqo9V010GhLAqOpF0BePojHgh1HgNg8^72k"

  private val KEY: String = "a51hgaoqpgh5bcmhyt1zptys=="

  def keyToSpec(): SecretKeySpec = {
    var keyBytes: Array[Byte] =
      ("jMhKlOuJnM34G6NHkqo9V010GhLAqOpF0BePojHgh1HgNg8^72k" + "a51hgaoqpgh5bcmhyt1zptys")
        .getBytes("UTF-8")
    val sha: MessageDigest = MessageDigest.getInstance("SHA-1")
    keyBytes = sha.digest(keyBytes)
    keyBytes = util.Arrays.copyOf(keyBytes, 16)
    new SecretKeySpec(keyBytes, "AES")
  }

  def encrypt(value: String): String = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    cipher.init(Cipher.ENCRYPT_MODE, keyToSpec())
    Base64.encodeBase64String(cipher.doFinal(value.getBytes("UTF-8")))
  }

  def decrypt(encryptedValue: String): String = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING")
    cipher.init(Cipher.DECRYPT_MODE, keyToSpec())
    new String(cipher.doFinal(Base64.decodeBase64(encryptedValue)))
  }

  val encriptador = udf { (device_id: String) =>
    encrypt(device_id)
  }

  val desencriptador = udf { (device_id: String) =>
    decrypt(device_id)
  }

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
    df
  }

  def getDataAcxiom(spark: SparkSession) {
    val dataBase =
      getDataIdPartners(
        spark,
        List("1008", "640", "119", "412", "979", "1131"),
        40,
        1,
        "streaming"
      ).select("device_id", "device_type")
        .withColumn("device_id", upper(col("device_id")))
        .filter("device_type = 'android' or device_type = 'ios'")
        .drop("device_type")
        .distinct()

    val xdIndex = spark.read
      .format("parquet")
      .load("/datascience/crossdevice/double_index/")

    val dataXD = xdIndex
      .filter("device_type IN ('and', 'ios')")
      .select("device")
      .withColumnRenamed("device", "device_id")
      .distinct()

    val all_ids = dataBase
      .unionAll(dataXD)
      .distinct()

    all_ids.cache()

    all_ids
      .withColumn("salt", encriptador(col("device_id")))
      .repartition(300)
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/IPG_maids")

    all_ids
      .join(
        xdIndex
          .filter("index_type IN ('and', 'ios') AND device_type = 'coo'")
          .withColumnRenamed("index", "device_id"),
        Seq("device_id")
      )
      .withColumnRenamed("device_id", "index")
      .withColumnRenamed("device", "device_id")
      .select("index", "device_id")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/IPG_maids_xd")
  }

  def getDataTriplets(
      spark: SparkSession,
      country: String,
      nDays: Int = -1,
      path: String = "/datascience/data_triplets/segments/"
  ) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val df = if (nDays > 0) {
      // read files from dates
      val format = "yyyyMMdd"
      val endDate = DateTime.now.minusDays(1)
      val days =
        (0 until nDays.toInt).map(endDate.minusDays(_)).map(_.toString(format))
      // Now we obtain the list of hdfs folders to be read
      val hdfs_files = days
        .map(day => path + "/day=%s/country=%s".format(day, country))
        .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      spark.read.option("basePath", path).parquet(hdfs_files: _*)
    } else {
      // read all date files
      spark.read.load(path + "/day=*/country=%s/".format(country))
    }
    df
  }

  def getDataSegments(spark: SparkSession) = {
    val dataIpg = spark.read
      .format("csv")
      .load("/datascience/custom/IPG_maids")
      .withColumnRenamed("_c0", "device_id") // columns are ['_c0' (device_id), '_c1' (salted)]
    val dataIpgXd =
      spark.read
        .format("parquet")
        .load("/datascience/custom/IPG_maids_xd") // columns are ['index', 'device_id']

    val segments = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/segments_IPG.csv")
      .withColumnRenamed("ID", "feature")
      .withColumn("feature", col("feature").cast("int"))
      .select("feature")
    val data_triplets =
      getDataTriplets(spark, "MX")
        .select("device_id", "feature")
        .join(broadcast(segments), Seq("feature"))
        .select("device_id", "feature")
        .withColumnRenamed("feature", "segment")

    data_triplets.cache()

    dataIpg
      .join(
        data_triplets,
        Seq("device_id")
      )
      .select("device_id", "segment")
      .write
      .format("parquet")
      .mode("overwrite")
      .save("/datascience/custom/IPG_maids_segments")

    dataIpgXd
      .join(
        data_triplets,
        Seq("device_id")
      )
      .select("index", "segment")
      .withColumnRenamed("index", "device_id")
      .write
      .format("parquet")
      .mode("overwrite")
      .save("/datascience/custom/IPG_maids_xd_segments")

    data_triplets.unpersist()
  }

  def getSegmentsPerMaid(spark: SparkSession) = {
    val segmentsForMaids =
      spark.read
        .format("parquet")
        .load("/datascience/custom/IPG_maids_segments")

    val segmentsForCookies =
      spark.read
        .format("parquet")
        .load("/datascience/custom/IPG_maids_xd_segments")

    val segmentsForAll = segmentsForCookies.unionAll(segmentsForMaids)

    segmentsForAll
      .distinct()
      .groupBy("device_id")
      .agg(collect_list("segment") as "segments")
      .withColumn("segments", concat_ws(",", col("segments")))
      .withColumn("salt", encriptador(col("device_id")))
      .select("device_id", "salt", "segments")
      .repartition(300)
      .write
      .format("csv")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/IPG_maids_enriched")
  }

  def gzipOutput(spark: SparkSession) = {
    spark.read
      .format("csv")
      .option("sep", "\t")
      .load("/datascience/custom/IPG_maids_enriched")
      .select("_c1", "_c2")
      .write
      .format("csv")
      .option("sep", "\t")
      .option("compression", "gzip")
      .mode("overwrite")
      .save("/datascience/custom/IPG_maids_enriched_gz")
  }

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    // getDataAcxiom(spark)
    getDataSegments(spark)
    getSegmentsPerMaid(spark)
    gzipOutput(spark)

  }
}
