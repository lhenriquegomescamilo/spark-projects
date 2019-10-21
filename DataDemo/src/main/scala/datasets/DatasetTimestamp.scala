package main.scala.datasets
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}

object DatasetTimestamp{
  def getDatasetTimestamp(
      spark: SparkSession,
      ga: DataFrame,
  ): DataFrame = {

    val myUDF = udf(
              (weekday: String, hour: String) =>
              if (weekday == "Sunday" || weekday == "Saturday") "%s1".format(hour)
              else "%s0".format(hour)
              )

    val dataset_timestamp = ga
                            .withColumnRenamed("timestamp", "Time")
                            .withColumn("Hour", date_format(col("Time"), "HH"))
                            .withColumn("Weekday", date_format(col("Time"), "EEEE"))
                            .withColumn("wd", myUDF(col("Weekday"), col("Hour")))
                            .groupBy("device_id", "wd")
                            .count()
                            .groupBy("device_id")
                            .pivot("wd")
                            .agg(sum("count"))
                            .orderBy(asc("device_id"))
    dataset_timestamp.write
                    .format("parquet")
                    .mode(SaveMode.Overwrite)
                    .save(
                      "/datascience/data_demo/name=%s/country=%s/ga_timestamp"
                        .format(name, country)
                    )
    dataset_timestamp
  }  
  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder
      .appName("Generate Data urls")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .getOrCreate()

    val ndays = if (args.length > 0) args(0).toInt else 30
    val since = if (args.length > 1) args(1).toInt else 1
  }
}
