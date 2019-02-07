package main.scala

import org.joda.time.DateTime
import org.apache.spark.sql.functions.{col, udf}
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import java.security.MessageDigest
import java.util
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64

object ByTwoGeoData {

  def sampleSanti(spark: SparkSession, day: String) {
    spark.read
      .format("parquet")
      .load("/datascience/geo/US/day=%s".format(day))
      .coalesce(100)
      .write
      .format("com.databricks.spark.csv")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/geo/US/%s/".format(day))
    println("\nLOGGER: DAY %s HAS BEEN PROCESSED!\n\n".format(day))
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
    // .format("com.databricks.spark.csv")
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

  def main(Args: Array[String]) {
    val spark = SparkSession.builder.appName("ByTwo data").getOrCreate()

    val format = "yyyyMMdd"
    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy")
    val start = formatter.parseDateTime("14/11/2018")
    val days = (0 until 50).map(n => start.plusDays(n)).map(_.toString(format))

    // days.map(day => sampleSanti(spark, day))
    days.map(day => getSampleATT(spark, day))
  }
}
