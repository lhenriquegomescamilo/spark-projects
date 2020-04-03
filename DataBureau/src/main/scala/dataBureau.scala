package main.scala
import org.apache.spark.sql.SparkSession
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.SaveMode
import java.security.MessageDigest
import java.util
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import java.time.DateTimeException
import java.sql.Savepoint
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._

object dataBureau {
  
  private val SALT: String =
    "RgpWo8h8aHJFkZ_TEwybsnvmWe3rgn8L"

  private val KEY: String = "YE67YVcgE@Wm6TeZ"

  def keyToSpec(): SecretKeySpec = {
    var keyBytes: Array[Byte] =
      ("RgpWo8h8aHJFkZ_TEwybsnvmWe3rgn8L" + "YE67YVcgE")
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
  
  def process_day(spark: SparkSession, day: String) {
     
    spark.read
          .load("/datascience/geo/safegraph/day=%s/country=BR/".format(day))
          .select("ad_id","utc_timestamp","latitude","longitude")
          .withColumnRenamed("ad_id","device_id")
          .withColumnRenamed("utc_timestamp","timestamp")
          .withColumnRenamed("latitude","lat")
          .withColumnRenamed("longitude","lon")
          .withColumn("device_id",encriptador(col("device_id")))
          .withColumn("day",lit(day))
          .write
          .format("parquet")
          .partitionBy("day")
          .save("/datascience/data_bureau")
  }


  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder.appName("Data Bureau BR")
                                    .config("spark.sql.files.ignoreCorruptFiles", "true")
                                    .getOrCreate()

    /// Parseo de parametros
    val since = if (args.length > 0) args(0).toInt else 2
    val ndays = if (args.length > 1) args(1).toInt else 1

    val format = "YYYYMMdd"
    val start = DateTime.now.minusDays(since + ndays)
    val end = DateTime.now.minusDays(since)

    val daysCount = Days.daysBetween(start, end).getDays()
    val days =
      (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

    days.map(day => process_day(spark, day))
  }
}
