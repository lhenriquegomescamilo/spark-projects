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
  datediff,
  ceil,
  first,
  countDistinct,
  concat,
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
      .map(day => path + "/day=%s/".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    format = "yyyy-MM-dd"
    val today = end.toString(format)
    val magic_ratio = 6.798

    val udfClk = udf((keyword: String) => if (keyword == "clk") 1 else 0)
    val udfImp = udf((keyword: String) => if (keyword == "imp") 1 else 0)
    val udfCnv = udf((keyword: String) => if (keyword == "cnv") 1 else 0)
    val udfCTR = udf((impressions: String, click: String) => if (impressions.toInt > 0) (click.toInt)/(impressions.toInt) else 0)

    // df.withColumn("data_type_clk",udfClk(col("data_type")))
    //   .withColumn("data_type_imp",udfImp(col("data_type")))
    //   .withColumn("data_type_cnv",udfCnv(col("data_type")))
    //   .withColumn("day",lit(today))
    //   .withColumn("datediff",datediff(col("day"),col("time")))
    //   .withColumn("periodo",when(col("datediff") <= 1, "Last 1 day").otherwise(when(col("datediff") <= 7, "Last 7 days").otherwise("Last 30 days")))
    //   .withColumn("ID",concat(col("periodo"),lit("-"),col("campaign_id")))
    //   .write
    //   .format("parquet")
    //   .mode(SaveMode.Overwrite)
    //   .save("/datascience/data_insights/data_chkpt")

    val df_chkpt = spark.read.load("/datascience/data_insights/data_chkpt")
    df_chkpt.cache()

    // Data Agregada KPIS
    df_chkpt.groupBy("campaign_id")
      .agg(count(col("id_partner")).as("hits"),
            countDistinct(col("device_id")).as("devices"),
            countDistinct(col("nid_sh2")).as("nids"),
            sum(col("data_type_imp")).as("impressions"),
            sum(col("data_type_clk")).as("clicks"),
            sum(col("data_type_cnv")).as("conversions"),
            first("ID").as("ID"),
            first("day").as("day"),
            first("periodo").as("periodo"))
      .withColumn("ctr",udfCTR(col("impressions"),col("clicks")))
      .withColumn("people",ceil((col("devices")/magic_ratio) + col("nids")))
      .write
      .format("parquet")
      .partitionBy("day")
      .mode("append")
      .save("/datascience/data_insights/data_kpis/")

    // Data Agregada Age
    val age_segments = List(4, 5, 6, 7, 8, 9)
    df_chkpt.filter(col("feature").isin(age_segments: _*))
            .groupBy("campaign_id","segments")
            .agg(countDistinct(col("device_id")).as("devices"),
                  countDistinct(col("nid_sh2")).as("nids"),
                  first("ID").as("ID"),
                  first("campaign_name").as("campaign_name"),
                  first("day").as("day"),
                  first("periodo").as("periodo"))
            .withColumn("people",ceil((col("devices")/magic_ratio) + col("nids")))
            .write
            .format("parquet")
            .partitionBy("day")
            .mode("append")
            .save("/datascience/data_insights/data_age/")

    // Data Agregada Gender
    val gender_segments = List(2, 3)
    df_chkpt.filter(col("feature").isin(gender_segments: _*))
            .groupBy("campaign_id","segments")
            .agg(countDistinct(col("device_id")).as("devices"),
                  countDistinct(col("nid_sh2")).as("nids"),
                  first("ID").as("ID"),
                  first("campaign_name").as("campaign_name"),
                  first("day").as("day"),
                  first("periodo").as("periodo"))
            .withColumn("people",ceil((col("devices")/magic_ratio) + col("nids")))
            .write
            .format("parquet")
            .partitionBy("day")
            .mode("append")
            .save("/datascience/data_insights/data_gender/")

    // Data Agregada Device type
    df_chkpt.groupBy("campaign_id","device_type")
            .agg(countDistinct(col("device_id")).as("devices"),
                  countDistinct(col("nid_sh2")).as("nids"),
                  first("ID").as("ID"),
                  first("campaign_name").as("campaign_name"),
                  first("day").as("day"),
                  first("periodo").as("periodo"))
            .withColumn("people",ceil((col("devices")/magic_ratio) + col("nids")))
            .write
            .format("parquet")
            .partitionBy("day")
            .mode("append")
            .save("/datascience/data_insights/data_device_type/")
    
    // Data Agregada Brand
    df_chkpt.groupBy("campaign_id","brand")
            .agg(countDistinct(col("device_id")).as("devices"),
                  countDistinct(col("nid_sh2")).as("nids"),
                  first("ID").as("ID"),
                  first("campaign_name").as("campaign_name"),
                  first("day").as("day"),
                  first("periodo").as("periodo"))
            .withColumn("people",ceil((col("devices")/magic_ratio) + col("nids")))
            .write
            .format("parquet")
            .partitionBy("day")
            .mode("append")
            .save("/datascience/data_insights/data_brand/")

      
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
    val since = if (args.length > 0) args(0).toInt else 0
    val ndays = if (args.length > 1) args(1).toInt else 30

    get_data_kpis(spark,ndays,since)

  }
}
