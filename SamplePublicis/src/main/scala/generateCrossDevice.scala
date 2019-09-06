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
import org.apache.hadoop.fs._

object generateCrossDevice {
  def generate_organic_xd(spark: SparkSession, organicPath: String, runType: String, from: Int) {
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
      .withColumnRenamed("device_id", "rtgtly_uid")
      .select("rtgtly_uid", "android", "ios")

    // Here we only load all the device ids that we have previously obtained
    val organic = spark.read
      .format("json")
      .load(organicPath)
      .select("rtgtly_uid")

    val joint = organic.join(index_xd, Seq("rtgtly_uid"), "inner")

    // Now we store all the information
    val pathToJson =
      "hdfs://rely-hdfs/datascience/data_publicis/idmap/%s/dt=%s"
        .format(runType, DateTime.now.minusDays(from).toString("yyyyMMdd"))

    joint.write
      .format("com.databricks.spark.csv")
      .option("compression", "bzip2")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save(pathToJson)


    // Finally we rename all the generated files
    val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val files = hdfs.listStatus(new Path(pathToJson))
    val originalPath = files.map(_.getPath())

    val paths = originalPath.par
      .filter(!_.toString.contains("_SUCCESS"))
      .foreach(
        e =>
          hdfs.rename(
            e,
            new Path(
              pathToJson + "/retargetly_MX_idmap_%s_%s_%s.tsv.bz2".format(
                runType,
                e.toString.split("/").last.split("-")(1),
                DateTime.now.toString("yyyyMMdd")
              )
            )
          )
      )
  }


  def main(args: Array[String]) {
    /// Configuracion spark
    val spark =
      SparkSession.builder.appName("Generate Cross device").getOrCreate()

    //generate_organic_xd(spark)
  }
}
