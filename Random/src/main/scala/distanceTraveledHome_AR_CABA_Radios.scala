package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{
  round,
  broadcast,
  col,
  abs,
  to_date,
  to_timestamp,
  hour,
  date_format,
  from_unixtime,
  count,
  avg
}
import org.apache.spark.sql.SaveMode
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.input_file_name
//import com.github.davidallsopp.geohash.GeoHash._

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object distanceTraveled_AR_CABA_Radios {

  def getDataPipeline(
      spark: SparkSession,
      path: String,
      nDays: String,
      since: String,
      country: String
  ) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    //specifying country
    //val country_iso = "MX"

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days =
      (0 until nDays.toInt).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, country)) //
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    df
  }

  def get_ua_segments(spark: SparkSession) = {

    //
    //val ua = spark.read.format("parquet")
    //        .load("/datascience/data_useragents/day=*/country=AR")
    //       .filter("model != ''") //con esto filtramos los desktop
    //      .withColumn("device_id",upper(col("device_id")))
    //     .drop("user_agent","event_type","url")
    //    .dropDuplicates("device_id")

    val ua =
      getDataPipeline(spark, "/datascience/data_useragents/", "30", "1", "MX")
        .filter("model != ''") //con esto filtramos los desktop
        .withColumn("device_id", upper(col("device_id")))
        .drop("user_agent", "event_type", "url")
        .dropDuplicates("device_id")
    //.filter("(country== 'AR') OR (country== 'CL') OR (country== 'MX')")

    val segments = getDataPipeline(
      spark,
      "/datascience/data_triplets/segments/",
      "15",
      "1",
      "MX"
    ).withColumn("device_id", upper(col("device_id")))
      .groupBy("device_id")
      .agg(concat_ws(",", collect_set("feature")) as "segments")

    val joined = ua
      .join(segments, Seq("device_id"))
      .write
      .format("csv")
      .option("header", true)
      .option("delimiter", "\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/misc/ua_w_segments_30d_MX_II")

  }

  def get_safegraph_data(
      spark: SparkSession,
      nDays: String,
      since: String,
      country: String
  ) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt)
      .map(end.minusDays(_))
      .map(_.toString(format))

    // Now we obtain the list of hdfs files to be read
    val path = "/datascience/geo/safegraph/"
    val hdfs_files = days
      .map(day => path + "day=%s/country=%s/".format(day, country))
      .filter(
        path => fs.exists(new org.apache.hadoop.fs.Path(path))
      )
      .map(day => day + "*.snappy.parquet")

    // Finally we read, filter by country, rename the columns and return the data
    val df_safegraph = spark.read
      .option("header", "true")
      .parquet(hdfs_files: _*)
      .dropDuplicates("ad_id", "latitude", "longitude")
      .withColumnRenamed("ad_id", "device_id")
      .withColumnRenamed("id_type", "device_type")
      .withColumn("device_id", upper(col("device_id")))

    df_safegraph

  }

  def get_safegraph_all_country(
      spark: SparkSession,
      nDays: String,
      since: String
  ) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt)
      .map(end.minusDays(_))
      .map(_.toString(format))

    // Now we obtain the list of hdfs files to be read
    val path = "/datascience/geo/safegraph/"
    val hdfs_files = days
      .map(day => path + "day=%s/".format(day))
      .filter(
        path => fs.exists(new org.apache.hadoop.fs.Path(path))
      )
      .map(day => day + "*/*.snappy.parquet")

    // Finally we read, filter by country, rename the columns and return the data
    val df_safegraph = spark.read
      .option("header", "true")
      .parquet(hdfs_files: _*)
      .withColumn("input", input_file_name)
      .withColumn("day", split(col("input"), "/").getItem(6))
      .withColumn("country", split(col("input"), "/").getItem(7))

    df_safegraph

  }

  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    //Esta función obtiene los geohashes los últimos 30 días y mira una desagregacióon por barrio para Argentina.

    val country = "argentina"

    val timezone = Map(
      "argentina" -> "GMT-3",
      "mexico" -> "GMT-5",
      "CL" -> "GMT-3",
      "CO" -> "GMT-5",
      "PE" -> "GMT-5"
    )

    //setting timezone depending on country
    spark.conf.set("spark.sql.session.timeZone", timezone(country))

    val today = (java.time.LocalDate.now).toString
    val getGeoHash = udf(
      (latitude: Double, longitude: Double) =>
        com.github.davidallsopp.geohash.GeoHash.encode(latitude, longitude, 8)
    )

    val risky_devices = spark.read
      .format("csv")
      .load("/datascience/custom/devices_risk_generation.csv")
      .withColumnRenamed("_c0", "device_id")
      .withColumn("device_id", lower(col("device_id")))

    // Primero obtenemos la data raw que sera de utilidad para los calculos siguientes
    val raw = get_safegraph_data(spark, "60", "0", "AR")
      .unionAll(get_safegraph_data(spark, "60", "0", country))
      .withColumnRenamed("ad_id", "device_id")
      .withColumn("device_id", lower(col("device_id")))
      .withColumn("device_id", lower(col("device_id")))
      .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
      .withColumn("Day", date_format(col("Time"), "dd-MM-YY"))
      .withColumn("Hour", date_format(col("Time"), "HH"))
      .withColumn(
        "DayPeriod",
        when(col("Hour") >= 0 && col("Hour") < 6, "0 - EarlyMorning")
          .otherwise(
            when(col("Hour") >= 6 && col("Hour") < 11, "1 - Morning")
              .otherwise(
                when(col("Hour") >= 11 && col("Hour") < 14, "2 - Noon")
                  .otherwise(
                    when(col("Hour") >= 14 && col("Hour") < 18, "3 - Evening")
                      .otherwise(when(col("Hour") >= 18, "4 - Night"))
                  )
              )
          )
      )
      .withColumn("geo_hash", getGeoHash(col("latitude"), col("longitude")))
      .withColumn("geo_hash_7", substring(col("geo_hash"), 0, 7))

    val geo_hash_visits = raw
      .groupBy("device_id", "Day", "DayPeriod", "Hour", "geo_hash")
      .agg(count("utc_timestamp") as "detections")
      .withColumn("country", lit(country))

    val output_file =
      "/datascience/geo/Reports/GCBA/Coronavirus/%s/geohashes_by_user_hourly_%s"
        .format(today, country)

    geo_hash_visits.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .option("header", true)
      .save(output_file)

    // Barrios y radios censales

    val geo_hash_table = spark.read
      .format("csv")
      .option("header", true)
      .option("delimiter", ",")
      .load("/datascience/geo/geohashes_tables/AR_CABA_GeoHash_to_Entity.csv")

    // Levantamos la data y la pegamos a los barrios
    val geo_labeled_users = spark.read
      .format("parquet")
      .load(output_file)
      .withColumn("geo_hash_7", substring(col("geo_hash"), 0, 7))
      .join(geo_hash_table, Seq("geo_hash_7"))

    geo_labeled_users.persist()

    //Agregamos por día
    val output_file_tipo_2a =
      "/datascience/geo/Reports/GCBA/Coronavirus/%s/geohash_travel_barrio_radio_CLASE2_%s"
        .format(today, country)

    geo_labeled_users
      .groupBy("BARRIO", "Day", "device_id")
      .agg(
        approxCountDistinct("geo_hash", 0.02) as "geo_hash",
        approxCountDistinct("geo_hash_7", 0.02) as "geo_hash_7",
        sum("detections") as "detections"
      )
      .withColumn("geo_hash_1", when(col("geo_hash") === 1, 1).otherwise(0))
      .withColumn("geo_hash_2", when(col("geo_hash") >= 2, 1).otherwise(0))
      .withColumn("geo_hash_3", when(col("geo_hash") >= 3, 1).otherwise(0))
      .withColumn("geo_hash_4", when(col("geo_hash") >= 4, 1).otherwise(0))
      .withColumn("geo_hash_5", when(col("geo_hash") >= 5, 1).otherwise(0))
      .groupBy("BARRIO", "Day")
      .agg(
        count("device_id") as "devices",
        avg("detections") as "detections_avg",
        avg("geo_hash_7") as "geo_hash_7_avg",
        avg("geo_hash") as "geo_hash_avg",
        sum("geo_hash_1") as "geo_hash_1",
        sum("geo_hash_2") as "geo_hash_2",
        sum("geo_hash_3") as "geo_hash_3",
        sum("geo_hash_4") as "geo_hash_4",
        sum("geo_hash_5") as "geo_hash_5"
      )
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", true)
      .save(output_file_tipo_2a)

    // Obtenemos el listado de barrios que recorrieron los usuarios de un radio censal particular.
    val w = Window
      .partitionBy(col("BARRIO"), col("RADIO"), col("Day"))
      .orderBy(col("device_unique").desc)

    geo_labeled_users
      .select("BARRIO", "RADIO", "Day", "device_id")
      .distinct()
      .join(
        geo_labeled_users
          .select("BARRIO", "Day", "device_id")
          .distinct()
          .withColumnRenamed("BARRIO", "BARRIO2"),
        Seq("device_id", "Day")
      ) // BARRIO, RADIO, Day, device_id, BARRIO2
      .groupBy("BARRIO", "RADIO", "Day", "BARRIO2")
      .agg(approxCountDistinct("device_id", 0.02) as "device_unique")
      .filter("BARRIO != BARRIO2")
      .withColumn("rn", row_number.over(w))
      .where(col("rn") < 6)
      .drop("rn")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", true)
      .save(
        "/datascience/geo/Reports/GCBA/Coronavirus/%s/geohash_travel_barrio_radio_CLASE2_%s_otros_barrios"
          .format(today, country)
      )
    // //Agregamos por día y horario
    // val output_file_tipo_2b =
    //   "/datascience/geo/Reports/GCBA/Coronavirus/%s/geohash_travel_barrio_radio_CLASE2_hourly_%s"
    //     .format(today, country)

    // geo_labeled_users
    //   .groupBy("BARRIO", "RADIO", "Day", "DayPeriod", "device_id")
    //   .agg(
    //     countDistinct("geo_hash_7") as "geo_hash_7",
    //     sum("detections") as "detections"
    //   )
    //   .groupBy("BARRIO", "RADIO", "Day", "DayPeriod")
    //   .agg(
    //     count("device_id") as "devices",
    //     avg("detections") as "detections_avg",
    //     avg("geo_hash_7") as "geo_hash_7_avg"
    //   )
    //   .repartition(1)
    //   .write
    //   .mode(SaveMode.Overwrite)
    //   .format("csv")
    //   .option("header", true)
    //   .save(output_file_tipo_2b)

  }

}
