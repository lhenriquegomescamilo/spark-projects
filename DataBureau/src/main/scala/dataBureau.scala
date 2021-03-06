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
  
  def keyToSpec(key:String,salt:String): SecretKeySpec = {

    var keyBytes: Array[Byte] =
      (key + salt)
        .getBytes("UTF-8")
    val sha: MessageDigest = MessageDigest.getInstance("SHA-1")
    keyBytes = sha.digest(keyBytes)
    keyBytes = util.Arrays.copyOf(keyBytes, 16)
    new SecretKeySpec(keyBytes, "AES")
  }

  def encrypt(value: String, key:String, salt:String): String = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    cipher.init(Cipher.ENCRYPT_MODE, keyToSpec(key,salt))
    Base64.encodeBase64String(cipher.doFinal(value.getBytes("UTF-8")))
  }

  def decrypt(encryptedValue: String, key:String, salt:String): String = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING")
    cipher.init(Cipher.DECRYPT_MODE, keyToSpec(key,salt))
    new String(cipher.doFinal(Base64.decodeBase64(encryptedValue)))
  }

  val encriptador = udf { (device_id: String,key: String,salt: String) =>
    encrypt(device_id,key,salt)
  }

  val desencriptador = udf { (device_id: String,key:String, salt:String) =>
    decrypt(device_id,key,salt)
  }
  
  def process_day(spark: SparkSession, day: String) {
    val key = spark.read.format("json").load("/datascience/custom/keys_bureau.json").select("key").take(1)(0)(0).toString
    val salt = spark.read.format("json").load("/datascience/custom/keys_bureau.json").select("salt").take(1)(0)(0).toString
     
    spark.read
          .load("/datascience/geo/safegraph/day=%s/country=BR/".format(day))
          .select("ad_id","utc_timestamp","latitude","longitude")
          .withColumnRenamed("ad_id","device_id")
          .withColumnRenamed("utc_timestamp","timestamp")
          .withColumnRenamed("latitude","lat")
          .withColumnRenamed("longitude","lon")
          .withColumn("device_id",encriptador(col("device_id"),lit(key),lit(salt)))
          .withColumn("day",lit(day))
          .write
          .format("parquet")
          .partitionBy("day")
          .mode(SaveMode.Overwrite)
          .save("/datascience/data_bureau")
  }

  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder.appName("Data Bureau BR")
                                    .config("spark.sql.files.ignoreCorruptFiles", "true")
                                    .config("spark.sql.files.ignoreCorruptFiles", "true")
                                    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                                    .getOrCreate()

    /// Parseo de parametros
    val since = if (args.length > 0) args(0).toInt else 6
    val ndays = if (args.length > 1) args(1).toInt else 4

    val format = "YYYYMMdd"
    val start = DateTime.now.minusDays(since + ndays)
    val end = DateTime.now.minusDays(since)

    val daysCount = Days.daysBetween(start, end).getDays()
    val days =
      (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

    for (day <- days){
      try {
        process_day(spark,day)
      } catch {
        case e: Throwable => {
          println("No Data: %s".format(day))
        }
      }
    }
  }
}
