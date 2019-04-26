package main.scala

import org.joda.time.DateTime
import org.apache.spark.sql.functions.{col, udf, lit}
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import java.security.MessageDigest
import java.util
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64

object ByTwoGeoData {

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

  def keyToSpec(): SecretKeySpec = {
    var keyBytes: Array[Byte] = (SALT + KEY).getBytes("UTF-8")
    val sha: MessageDigest = MessageDigest.getInstance("SHA-1")
    keyBytes = sha.digest(keyBytes)
    keyBytes = util.Arrays.copyOf(keyBytes, 16)
    new SecretKeySpec(keyBytes, "AES")
  }

  private val SALT: String =
    "jMhKlOuJnM34G6NHkqo9V010GhLAqOpF0BePojHgh1HgNg8^72k"

  private val KEY: String = "a51hgaoqpgh5bcmhyt1zptys=="

  def getSampleATT(spark: SparkSession, day: String) = {
    val udfEncrypt = udf(
      (estid: String) => encrypt(estid)
    )

    println("LOGGER: processing day %s".format(day))
    spark.read
      .format("com.databricks.spark.csv")
      .load("/datascience/sharethis/loading/%s*.json".format(day))
      .filter("_c13 = 'san francisco' AND _c8 LIKE '%att%'")
      .select(
        "_c0",
        "_c1",
        "_c2",
        "_c3",
        "_c4",
        "_c5",
        "_c6",
        "_c7",
        "_c8",
        "_c9"
      )
      .withColumn("_c0", udfEncrypt(col("_c0")))
      .coalesce(100)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("sep", "\t")
      .save("/datascience/sharethis/sample_att_url/%s".format(day))
    println("LOGGER: day %s processed successfully!".format(day))
  }

  /**
    * This function takes the data in csv format, and transforms it into parquet files for a given day.
    * Basically, it loads the data into a dataframe, rename the columns and finally store it as
    * a parquet pipeline.
    */
  def getSTData(spark: SparkSession, day: String) = {
    val path = "/datascience/sharethis/loading/" + day + "*"
    spark.read
      .format("csv")
      .load(path)
      .withColumnRenamed("_c0", "estid")
      .withColumnRenamed("_c1", "utc_timestamp")
      .withColumnRenamed("_c2", "url")
      .withColumnRenamed("_c3", "ip")
      .withColumnRenamed("_c4", "device_type")
      .withColumnRenamed("_c5", "os")
      .withColumnRenamed("_c6", "browser")
      .withColumnRenamed("_c7", "isp")
      .withColumnRenamed("_c8", "connection_type")
      .withColumnRenamed("_c9", "latitude")
      .withColumnRenamed("_c10", "longitude")
      .withColumnRenamed("_c11", "zipcode")
      .withColumnRenamed("_c12", "city")
      .withColumn("day", lit(day))
      .write
      .format("parquet")
      .partitionBy("day")
      .mode("append")
      .save("/datascience/sharethis/historic/")
  }

  def getByTwoData(spark: SparkSession, day: String) = {
    // First we load the data for the given day
    val data = spark.read
      .load("/datascience/sharethis/historic/day=" + day)
      .drop("device_type")

    // Get the data from the estid mapper
    val estid_madid =
      spark.read.load("/datascience/sharethis/estid_madid_table")

    // Now we join both tables
    val join = data
      .join(estid_madid, Seq("estid"))
      .drop("estid")
      .select(
        "device",
        "utc_timestamp",
        "ip",
        "latitude",
        "longitude",
        "zipcode",
        "city",
        "device_type"
      )
      .withColumn("day", lit(day))
      .write
      .format("parquet")
      .partitionBy("day")
      .mode("append")
      .save("/datascience/data_bytwo")
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("ByTwo data").getOrCreate()
    val ndays = if (args.length > 0) args(0).toInt else 1
    val since = if (args.length > 1) args(1).toInt else 1

    val format = "yyyyMMdd"
    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy")
    val start = DateTime.now //formatter.parseDateTime("24/01/2019")
    // val day = start.toString(format)
    val days = (since until since+ndays).map(start.minusDays(_)).map(_.toString(format))

    days.foreach(day => getSTData(spark, day))
    days.foreach(day => getByTwoData(spark, day))
    // getSTData(spark, day)
    // getByTwoData(spark, day)
  }
}
