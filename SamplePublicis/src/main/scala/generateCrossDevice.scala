package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions.{
  explode,
  desc,
  lit,
  size,
  concat,
  col,
  concat_ws,
  collect_list,
  udf,
  broadcast
}
import org.joda.time.{Days, DateTime}
import java.security.MessageDigest

object generateCrossDevice {
  def generate_organic_xd(spark: SparkSession, organicPath: String) {
    // This function takes a list of ids, hashes all of them to SHA256, and then concatenates all of them separated by commas
    val hashUDF = udf(
      (ids: Seq[String]) =>
        ids
          .map(
            id =>
              MessageDigest
                .getInstance("SHA-256")
                .digest(id.getBytes("UTF-8"))
                .map("%02x".format(_))
                .mkString
          )
          .mkString(",")
    )

    // Here we load the android and ios ids, we hash them and then transform each column into a string
    val index_xd = spark.read
      .format("parquet")
      .load("/datascience/crossdevice/list_index")
      .withColumn("android", hashUDF(col("android")))
      .withColumn("ios", hashUDF(col("ios")))
      .select("device_id", "android", "ios")

    // Here we only load all the device ids that we have previously obtained
    val organic = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/datascience/data_publicis/organic")
      .select("device_id")

    val joint = organic.join(index_xd, Seq("device_id"), "inner")

    joint.write
      .format("com.databricks.spark.csv")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_publicis/idmap")
  }
  def main(args: Array[String]) {
    /// Configuracion spark
    val spark =
      SparkSession.builder.appName("Generate Cross device").getOrCreate()

    generate_organic_xd(spark)
  }
}
