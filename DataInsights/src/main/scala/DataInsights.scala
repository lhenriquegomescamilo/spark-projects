package main.scala
import org.apache.spark.sql.SparkSession
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{
  upper,
  count,
  col,
  abs,
  udf,
  regexp_replace,
  split,
  lit,
  explode,
  length,
  to_timestamp,
  from_unixtime,
  date_format,
  sum
}

object DataInsights {

  def get_data_user_agent(
      spark: SparkSession,
      ndays: Int,
      since: Int
  ): DataFrame = {
    // Spark configuration
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until ndays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_useragents"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)
                  .select("device_id","brand","model")
                  .distinct()

    df
  }


  def get_data(spark:SparkSession, day:String, df_ua: DataFrame){

    val data_eventqueue = spark.read.format("csv").option("sep", "\t").option("header", "true")
                                .load("/data/eventqueue/%s/*.tsv.gz".format(day))
                                .filter("id_partner = 879 and device_id is not null and event_type = 'tk'")
                                .select("time","id_partner","device_id","campaign_id","campaign_name","segments","device_type","country","data_type","nid_sh2")
                                
    data_eventqueue.join(df_ua,Seq("device_id"),"left")
                    .withColumn("segments", split(col("segments"), "")).withColumn("segments",explode(col("segments")))
                    .withColumn("day",lit(day.replace("/","")))
                    .write
                    .format("parquet")
                    .partitionBy("day")
                    .mode("append")
                    .save("/datascience/data_insights/")
  }

  
  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder.appName("Data Insights Process")
                                    .config("spark.sql.files.ignoreCorruptFiles", "true")
                                    .config("spark.sql.sources.partitionOverwriteMode","dynamic")
                                    .getOrCreate()

    /// Parseo de parametros
    val since = if (args.length > 0) args(0).toInt else 0
    val ndays = if (args.length > 1) args(1).toInt else 1

    val format = "YYYY/MM/dd"
    val start = DateTime.now.minusDays(since + ndays)
    val end = DateTime.now.minusDays(since)

    val daysCount = Days.daysBetween(start, end).getDays()
    val days =
      (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

    val df_ua = get_data_user_agent(spark,10,1)
    df_ua.cache()

    days.map(day => get_data(spark, day, df_ua))

  }
}
