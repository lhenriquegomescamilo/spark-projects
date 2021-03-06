package main.scala
import org.apache.spark.sql.SparkSession
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType}
import java.util.zip.DataFormatException

object AggregateData {

  def getRawData(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      partners: List[String]
  ): DataFrame = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    var format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until ndays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_insights/raw"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .flatMap(
        day =>
          partners.map(
            partner => path + "/day=%s/id_partner=%s/".format(day, partner)
          )
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    hdfs_files.foreach(println)
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    format = "yyyy-MM-dd"
    val today = end.toString(format)

    val df_chkpt = df
      .withColumn("day", lit(today))
      .withColumn("datediff", datediff(col("day"), col("time")))
      .withColumn(
        "periodo",
        when(
          col("datediff") <= 1,
          lit(
            Array(
              "Last 1 day",
              "Last 7 day",
              "Last 14 day",
              "Last 21 day",
              "Last 30 day"
            )
          )
        ).otherwise(
          when(
            col("datediff") <= 7,
            lit(
              Array("Last 7 day", "Last 14 day", "Last 21 day", "Last 30 day")
            )
          ).otherwise(
              when(
                col("datediff") <= 14,
                lit(Array("Last 14 day", "Last 21 day", "Last 30 day"))
              ).otherwise(
                when(
                  col("datediff") <= 21,
                  lit(Array("Last 21 day", "Last 30 day"))
                ).otherwise(lit(Array("Last 30 day")))
              )
            )
        )
      )
      .withColumn("periodo", explode(col("periodo")))
      .withColumn("campaign_id", array(col("campaign_id"), lit(0)))
      .withColumn("campaign_id", explode(col("campaign_id")))
      .withColumn("ID", concat(col("periodo"), lit("-"), col("campaign_id")))
      .withColumn(
        "data_type_clk",
        when(col("data_type") === "clk", 1).otherwise(0)
      )
      .withColumn(
        "data_type_imp",
        when(col("data_type") === "imp", 1).otherwise(0)
      )
      .withColumn(
        "data_type_cnv",
        when(col("data_type") === "cnv", 1).otherwise(0)
      )

    df_chkpt
  }

  def aggregateKPIs(
      df_chkpt: DataFrame,
      today: String,
      agg_type: String = "kpis"
  ) = {
    // Data Agregada KPIS
    df_chkpt
      .groupBy("id_partner", "ID")
      .agg(
        approx_count_distinct(col("device_id"), 0.03).as("devices"),
        approx_count_distinct(col("nid_sh2"), 0.03).as("nids"),
        sum(col("data_type_imp")).as("impressions"),
        sum(col("data_type_clk")).as("clicks"),
        sum(col("data_type_cnv")).as("conversions")
      )
      .withColumn(
        "hits",
        col("impressions") + col("clicks") + col("conversions")
      )
      .withColumn("day", lit(today))
      .withColumn("type", lit(agg_type))
      .write
      .format("parquet")
      .partitionBy("day", "type", "id_partner")
      .mode("overwrite")
      .save("/datascience/data_insights/aggregated/")
  }

  def aggregateSegments(
      df_chkpt: DataFrame,
      today: String,
      spark: SparkSession,
      agg_type: String = "segments",
      first_party: Boolean = true
  ) = {
    // List of segments to filter
    val taxo_segments: Seq[String] = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/data_insights/taxo_pure.csv")
      .select("ID")
      .collect()
      .map(r => r(0).toString)
      .toSeq

    if (first_party) {
      // Get first_party segments
      df_chkpt
        .withColumn("first_party", split(col("first_party"), "\u0001"))
        .withColumn("segments", explode(col("first_party")))
        .groupBy("id_partner", "ID", "segments")
        .agg(
          approx_count_distinct(col("device_id"), 0.03).as("devices"),
          approx_count_distinct(col("nid_sh2"), 0.03).as("nids"),
          sum(col("data_type_imp")).as("impressions"),
          sum(col("data_type_clk")).as("clicks"),
          sum(col("data_type_cnv")).as("conversions")
        )
        .withColumn("day", lit(today))
        .withColumn("type", lit(agg_type + "_first_party"))
        .write
        .format("parquet")
        .partitionBy("day", "type", "id_partner")
        .mode("overwrite")
        .save("/datascience/data_insights/aggregated/")
    }

    // Totals per segment, id and campaign id
    df_chkpt
      .withColumn("segments", explode(col("segments")))
      .filter(col("segments").isin(taxo_segments: _*))
      .groupBy("id_partner", "ID", "segments")
      .agg(
        approx_count_distinct(col("device_id"), 0.03).as("devices"),
        approx_count_distinct(col("nid_sh2"), 0.03).as("nids"),
        sum(col("data_type_imp")).as("impressions"),
        sum(col("data_type_clk")).as("clicks"),
        sum(col("data_type_cnv")).as("conversions")
      )
      .withColumn("day", lit(today))
      .withColumn("type", lit(agg_type))
      .write
      .format("parquet")
      .partitionBy("day", "type", "id_partner")
      .mode("overwrite")
      .save("/datascience/data_insights/aggregated/")
  }

  def aggregateUserAgent(df_chkpt: DataFrame, today: String) = {
    df_chkpt
      .groupBy("id_partner", "ID", "brand")
      .agg(
        approx_count_distinct(col("device_id"), 0.03).as("devices"),
        approx_count_distinct(col("nid_sh2"), 0.03).as("nids")
      )
      .withColumn("day", lit(today))
      .withColumn("type", lit("brand"))
      .write
      .format("parquet")
      .partitionBy("day", "type", "id_partner")
      .mode("overwrite")
      .save("/datascience/data_insights/aggregated/")
  }

  def aggregateHour(df_chkpt: DataFrame, today: String) = {
    df_chkpt
      .withColumn("hour", date_format(col("time"), "HH"))
      .withColumn("hour", col("hour").cast(IntegerType))
      .groupBy("id_partner", "ID", "hour")
      .agg(
        approx_count_distinct(col("device_id"), 0.03).as("devices"),
        approx_count_distinct(col("nid_sh2"), 0.03).as("nids"),
        sum(col("data_type_imp")).as("impressions"),
        sum(col("data_type_clk")).as("clicks"),
        sum(col("data_type_cnv")).as("conversions")
      )
      .withColumn(
        "moment_day",
        when(col("hour") <= 12 && col("hour") >= 7, "Morning").otherwise(
          when(col("hour") <= 18 && col("hour") >= 13, "Afternoon")
            .otherwise(
              when(col("hour") <= 24 && col("hour") >= 19, "Evening")
                .otherwise("Night")
            )
        )
      )
      .withColumn("day", lit(today))
      .withColumn("type", lit("hour"))
      .write
      .format("parquet")
      .partitionBy("day", "type", "id_partner")
      .mode("overwrite")
      .save("/datascience/data_insights/aggregated/")
  }

  def aggregateDay(df_chkpt: DataFrame, today: String) = {
    df_chkpt
      .filter("periodo = 'Last 30 day'")
      .withColumn("dom", date_format(col("time"), "dd"))
      .withColumn("month", date_format(col("time"), "MM"))
      .withColumn("day_month", concat(col("dom"), lit("_"), col("month")))
      .groupBy("id_partner", "campaign_id", "day_month")
      .agg(
        approx_count_distinct(col("device_id"), 0.03).as("devices"),
        approx_count_distinct(col("nid_sh2"), 0.03).as("nids"),
        sum(col("data_type_imp")).as("impressions"),
        sum(col("data_type_clk")).as("clicks"),
        sum(col("data_type_cnv")).as("conversions")
      )
      .withColumn("day", lit(today))
      .withColumn("type", lit("day"))
      .write
      .format("parquet")
      .partitionBy("day", "type", "id_partner")
      .mode("overwrite")
      .save("/datascience/data_insights/aggregated/")
  }

  def getGeoData(spark: SparkSession, df_chkpt: DataFrame, today: String) = {
    val homes =
      spark.read.format("parquet").load("/datascience/data_insights/homes/")

    df_chkpt
      .select("device_id", "id_partner", "ID")
      .join(homes, Seq("device_id"))
      .groupBy("id_partner", "ID", "ESTATE", "country")
      .agg(
        approx_count_distinct(col("device_id"), 0.03).as("devices")
      )
      .withColumn("day", lit(today))
      .withColumn("type", lit("geo"))
      .write
      .format("parquet")
      .partitionBy("day", "type", "id_partner")
      .mode("overwrite")
      .save("/datascience/data_insights/aggregated/")
  }

  def get_aggregated_data(
      spark: SparkSession,
      df_chkpt: DataFrame,
      today: String
  ) {
    aggregateKPIs(df_chkpt, today)
    aggregateSegments(df_chkpt, today, spark)
    aggregateUserAgent(df_chkpt, today)
    aggregateHour(df_chkpt, today)
    aggregateDay(df_chkpt, today)
    getGeoData(spark, df_chkpt, today)
  }

  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder
      .appName("Data Insights Process")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    /// Parseo de parametros
    var format = "yyyy-MM-dd"
    val actual_day = DateTime.now.toString(format)
    val since = if (args.length > 0) args(0).toInt else 1
    val ndays = if (args.length > 1) args(1).toInt else 30

    format = "yyyyMMdd"
    val today = DateTime.now.minusDays(since).toString(format)
    val partners =
      """211, 215, 232, 233, 281, 346, 347, 507, 640, 644, 647, 682, 709, 753, 764, 
                    875, 879, 881, 900, 943, 955, 956, 984, 986, 993, 994, 1036, 1038, 1039, 1040, 
                    1041, 1042, 1055, 1122, 1157, 1159, 1179, 1239, 1251"""
        .replace(" ", "")
        .replace("\n", "")
        .split(",")
        .toList
    val df_chkpt = getRawData(
      spark,
      ndays,
      since,
      partners
    )
    val df_chkpt_previous =
      getRawData(spark, ndays, since + 30, partners)
    get_aggregated_data(spark, df_chkpt, today)
    aggregateSegments(
      df_chkpt_previous,
      today,
      spark,
      "segments_since30",
      true
    )
    aggregateKPIs(
      df_chkpt_previous,
      today,
      "kpis_since30"
    )
  }
}
