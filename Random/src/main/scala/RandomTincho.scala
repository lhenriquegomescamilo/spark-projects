package main.scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.joda.time.Days
import org.joda.time.DateTime

object RandomTincho {

  def get_selected_keywords(
      spark: SparkSession,
      ndays: Int,
      since: Int
  ): DataFrame = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    /// Leemos la data de keywords de ndays hacia atras
    val format = "yyyy-MM-dd"
    val start = DateTime.now.minusDays(since + ndays)
    val end = DateTime.now.minusDays(since)

    val daysCount = Days.daysBetween(start, end).getDays()
    val days =
      (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

    val dfs = (0 until daysCount)
      .map(start.plusDays(_))
      .map(_.toString(format))
      .filter(
        day =>
          fs.exists(
            new Path("/datascience/selected_keywords/%s.csv".format(day))
          )
      )
      .map(day => "/datascience/selected_keywords/%s.csv".format(day))

    val df = spark.read
      .format("csv")
      .load(dfs: _*)
      .withColumnRenamed("_c0", "url")
      .withColumnRenamed("_c1", "count")
      .withColumnRenamed("_c2", "country_web")
      .withColumnRenamed("_c3", "kws")
      .withColumnRenamed("_c4", "scores")
      .withColumn("kws", split(col("kws"), " "))
      .select("url","kws")
      .dropDuplicates("url")

    df
  }

  def get_gt_new_taxo(spark: SparkSession) = {
    
    val selected_keywords = get_selected_keywords(spark,ndays = 10, since = 1)
    val queries = spark.read.format("csv")
                        .option("header","true")
                        .load("/datascience/custom/new_taxo_queries.csv")
    
    var dfs: DataFrame = null
    var first = true

    for (row <- queries.rdd.collect){   
      var segment = row(0).toInt
      var query = row(1).toString
      var local = df.filter(query).withColumn("segment",lit(segment)).select("url","segment")
      if (first) {
          dfs = local
          first = false
      } else {
          dfs = dfs.unionAll(local)
      }
    }

    dfs.write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .partitionBy("country")
    .save("/datascience/data_url_classifier/GT_new_taxo")


  }

  def main(args: Array[String]) {

    val spark = SparkSession.builder
        .appName("Run Randomcito")
        //.config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .getOrCreate()
    
    get_gt_new_taxo(spark)
  }

}
