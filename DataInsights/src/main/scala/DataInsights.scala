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
  lower,
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

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select("device_id", "brand", "model")
      .distinct()

    df
  }

  def get_data_first_party(spark: SparkSession, day: String, df_ua: DataFrame) {

    val data_eventqueue = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/data/eventqueue/%s/*.tsv.gz".format(day))
      .filter(
        "first_party is not null and event_type in ('pv','batch','data','retroactive') and id_partner = 643"
      )
      .select(
        "time",
        "id_partner",
        "device_id",
        "device_type",
        "country",
        "data_type",
        "nid_sh2",
        "first_party"
      )

    data_eventqueue
      .join(df_ua, Seq("device_id"), "left")
      .withColumn("first_party", split(col("first_party"), "\u0001"))
      .withColumn("first_party", explode(col("first_party")))
      .withColumn("day", lit(day.replace("/", "")))
      .write
      .format("parquet")
      .partitionBy("day", "id_partner")
      .mode("append")
      .save("/datascience/data_insights/data_first_party/")
  }

  def get_data(
      spark: SparkSession,
      day: String,
      df_ua: DataFrame,
      partners: List[String]
  ) {

    val data_eventqueue = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/data/eventqueue/%s/*.tsv.gz".format(day))
      .filter("campaign_id is not null and event_type = 'tk'")
      .filter(col("id_partner").isin(partners: _*))
      .select(
        "time",
        "id_partner",
        "device_id",
        "campaign_id",
        "campaign_name",
        "third_party",
        "device_type",
        "country",
        "data_type",
        "nid_sh2",
        "first_party"
      )
      .withColumn("device_id", lower(col("device_id")))

    //val data_geo = spark.read.load("/datascience/geo/NSEHomes/data_geo_homes_for_insights")

    data_eventqueue
      .join(df_ua, Seq("device_id"), "left")
      //.join(data_geo,Seq("device_id"),"left")
      .withColumn("third_party", split(col("third_party"), "\u0001"))
      .withColumnRenamed("third_party", "segments")
      .withColumn("day", lit(day.replace("/", "")))
      .write
      .format("parquet")
      .partitionBy("day", "id_partner")
      .mode("overwrite")
      .save("/datascience/data_insights/raw")
  }

  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder
      .appName("Data Insights Process")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
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

    val df_ua = get_data_user_agent(spark, 10, 1)
    df_ua.cache()

    val partners =
      """211, 215, 232, 233, 281, 346, 347, 507, 640, 644, 647, 682, 709, 753, 764, 
                    875, 879, 881, 900, 943, 955, 956, 984, 986, 993, 994, 1036, 1038, 1039, 1040, 
                    1041, 1042, 1055, 1122, 1157, 1159, 1179, 1239, 1251"""
        .replace(" ", "")
        .replace("\n", "")
        .split(",")
        .toList
    days.map(day => get_data(spark, day, df_ua, partners))

  }
}
