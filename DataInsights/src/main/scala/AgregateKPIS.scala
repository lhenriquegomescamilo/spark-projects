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
  when,
  first,
  countDistinct,
  length,
  to_timestamp,
  from_unixtime,
  date_format,
  sum
}

object AgregateKPIS {

  def get_data_kpis(spark:SparkSession, ndays:Int, since:Int){

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    var format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    
    val days = (0 until ndays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_insights"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/".format(day, country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    format = "yyyy-MM-dd"
    val today = end.toString(format)
    val magic_ratio = 6.798

    val udfClk = udf((keyword: String) => if (keyword == "clk") 1 else 0)
    val udfImp = udf((keyword: String) => if (keyword == "imp") 1 else 0)
    val udfCnv = udf((keyword: String) => if (keyword == "cnv") 1 else 0)
    val udfCTR = udf((impressions: String, click: String) => if (impressions.toInt > 0) (click.toInt)/(impressions.toInt) else 0)

    df.withColumn("data_type_clk",udfClk(col("data_type")))
      .withColumn("data_type_imp",udfImp(col("data_type")))
      .withColumn("data_type_cnv",udfCnv(col("data_type")))
      .withColumn("today",lit(actual_day))
      .withColumn("datediff",datediff(col("today"),col("time")))
      .withColumn("periodo",when(col("datediff") <= 1, "Last 1 day").otherwise(when(col("datediff") <= 7, "Last 7 days").otherwise("Last 30 days")))
      .withColumn("ID",concat(col("periodo"),lit("-"),col("campaign_id")))
      .groupBy("campaign_id")
      .agg(count(col("id_partner")).as("hits"),
            countDistinct(col("device_id")).as("devices"),
            countDistinct(col("nid_sh2")).as("nids"),
            sum(col("data_type_imp")).as("impressions"),
            sum(col("data_type_clk")).as("clicks"),
            sum(col("data_type_cnv")).as("conversions"),
            first("ID").as("ID"),
            first("periodo").as("periodo"))
      .withColumn("ctr",udfCTR(col("impressions"),col("clicks")))
      .withColumn("people",ceil((col("devices")/magic_ratio) + col("nids")))
      .withColumn("day",lit("actual_day"))
      .write
      .format("parquet")
      .partitionBy("day")
      .mode("append")
      .save("/datascience/data_insights/data_kpis/")

  }

  
  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder.appName("Data Insights Process")
                                    .config("spark.sql.files.ignoreCorruptFiles", "true")
                                    .config("spark.sql.sources.partitionOverwriteMode","dynamic")
                                    .getOrCreate()

    /// Parseo de parametros
    val format = "yyyy-MM-dd"
    val actual_day = DateTime.now.toString(format)
    val ndays = 30
    val since = 0

    get_data_kpis(spark,actual_day)

  }
}
