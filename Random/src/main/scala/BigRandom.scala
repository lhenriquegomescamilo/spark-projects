package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types._

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object BigRandom {

def getDataEventQueue_27(
      spark: SparkSession,
      query_27: String,
      nDays: int,
      since: int) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val path = "/data/eventqueue"
        // Get the days to be loaded
    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/%s".format(day)) //
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read
        .option("basePath", path).load(hdfs_files: _*)
        .option("sep", "\t")
        .option("header", "true")
        .format("csv")
        .load(path)
        .select("country", "device_id","platforms")
        .na
        .drop()
        .withColumn("platforms", split(col("platforms"), "\u0001"))
        .filter(query_27)
        .select("country", "device_id").distinct()
        .write.format("csv")
        .option("header",true)
        .option("delimiter","\t")
        .mode(SaveMode.Overwrite)
        .save("/datascience/misc/pv_platform27.csv")
  }


 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("BigRandom").config("spark.sql.files.ignoreCorruptFiles", "true").getOrCreate()

    val query_27 = "country IN ('MX', 'BR') AND event_type = 'pv' AND array_contains(platforms, '27')"
    val nDays = 2
    val since = 1

    getDataEventQueue_27(
      spark=spark,
      query_27=query_27,
      nDays=nDays,
      since=since)
  }
}