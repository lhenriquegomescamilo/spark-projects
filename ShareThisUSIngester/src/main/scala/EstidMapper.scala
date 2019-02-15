package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.joda.time.DateTime
import org.apache.spark.sql.functions.{lit, col, upper}
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.{ SaveMode, DataFrame }

object EstidMapper {

  /**
    * This function takes a day as input, and produces a set of tuples, where every tuple has the
    * estid, device_id, device_type, and day.
    */
  def getEstIdsMatching(spark: SparkSession, day: String) = {
    val df = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/data/eventqueue/%s/*.tsv.gz".format(day))
      .filter(
        "d17 is not null and country = 'US' and event_type = 'sync'"
      )
      .select("d17", "device_id", "device_type")
      .withColumn("day", lit(day.replace("/", "")))
      .dropDuplicates()

    df.write
      .format("parquet")
      .partitionBy("day")
      .mode("append")
      .save("/datascience/sharethis/estid_table/")
  }


  def getEstidTable(spark: SparkSession, nDays: Int = 60): DataFrame = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end   = DateTime.now.minusDays(1)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/sharethis/estid_table"
    
    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days.map(day => path+"/day=%s".format(day))
                          .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files:_*)

    df
  }



  def crossDeviceTable(spark: SparkSession) = {
    // Get the estid table
    val estid_table = getEstidTable(spark)
      .withColumn("device_id", upper(col("device_id")))
      .select("device_id", "d17")

    // Get DrawBridge Index. Here we transform the device id to upper case too.
    val db_data = spark.read
      .format("parquet")
      .load("/datascience/crossdevice/double_index")
      .filter("index_type = 'coo' and device_type IN ('and', 'ios')")
      .withColumn("index", upper(col("index")))
      .select("index", "device", "device_type")

    // Here we do the cross-device per se.
    val cross_deviced = db_data
      .join(estid_table, db_data.col("index") === estid_table.col("device_id"))
      .select("d17", "device", "device_type")
      .withColumnRenamed("d17", "estid")

    // Finally, we store the result obtained.
    val output_path =
      "/datascience/sharethis/estid_madid_table/"
    cross_deviced.write.mode(SaveMode.Overwrite).save(output_path)
  }

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("Run matching estid-device_id").getOrCreate()

    val today = DateTime.now()
    val days = (1 until 1).map(
      days =>
        getEstIdsMatching(
          spark,
          today.minusDays(days).toString("yyyy/MM/dd")
        )
    )

    crossDeviceTable(spark)
  }
}
