package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions.{explode,desc,lit,size,concat,col,concat_ws,collect_list,udf,broadcast,upper,sha2}
import org.joda.time.{Days,DateTime}
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.SaveMode


/**
 * The idea of this script is to run random stuff. Most of the times, the idea is
 * to run quick fixes, or tests.
 */
object Random {
  def getMadidsFromShareThisWithGEO(spark: SparkSession) {
    val data_us = spark.read.format("csv").option("sep", ",")
                            .load("/datascience/test_us/loading/*.json")
                            .filter("_c2 is not null")
                            .withColumnRenamed("_c0", "d17")
                            .withColumnRenamed("_c5", "city")
                            .select("d17", "city")
    data_us.persist()
    println("LOGGING RANDOM: Total number of records: %s".format(data_us.count()))

    val estid_mapping = spark.read.format("csv")
                                  .option("sep", "\t")
                                  .option("header", "true")
                                  .load("/datascience/matching_estid")

    val joint = data_us.distinct()
                       .join(estid_mapping, Seq("d17"))
                       .select("device_id", "city")

    joint.write
         .mode(SaveMode.Overwrite)
         .save("/datascience/custom/shareThisWithGEO")
  }

  def getCrossDevice(spark: SparkSession) {
    val db_index = spark.read.format("parquet")
                             .load("/datascience/crossdevice/double_index/")
                             .filter("device_type IN ('a', 'i')")
                             .withColumn("index", upper(col("index")))

    val st_data = spark.read.format("parquet")
                            .load("/datascience/custom/shareThisWithGEO")
                            .withColumn("device_id", upper(col("device_id")))
    println("LOGGING RANDOM: Total number of records with GEO and device id: %s".format(st_data.count()))

    val cross_deviced = db_index.join(st_data, db_index.col("index")===st_data.col("device_id")).select("device", "device_type", "city")

    cross_deviced.write
                 .mode(SaveMode.Overwrite)
                 .save("/datascience/custom/madidsShareThisWithGEO")
  }

  def getEstIdsMatching(spark: SparkSession) = {
    val format = "yyyy/MM/dd"
    val start = DateTime.now.minusDays(30)
    val end   = DateTime.now.minusDays(15)

    val daysCount = Days.daysBetween(start, end).getDays()
    val days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

    val dfs = (0 until daysCount).map(start.plusDays(_))
      .map(_.toString(format))
      .map(x => spark.read.format("csv").option("sep", "\t")
                      .option("header", "true")
                      .load("/data/eventqueue/%s/*.tsv.gz".format(x))
                      .filter("d17 is not null and country = 'US' and event_type = 'sync'")
                      .select("d17","device_id")
                      .dropDuplicates())

    val df = dfs.reduce(_ union _)

    df.write.format("csv").option("sep", "\t")
                    .option("header",true)
                    .mode(SaveMode.Overwrite)
                    .save("/datascience/matching_estid_2")
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Run matching estid-device_id").getOrCreate()
    getCrossDevice(spark)
  }
}