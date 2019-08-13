package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.hadoop.conf.Configuration
import org.apache.spark.ml.classification.{
  RandomForestClassificationModel,
  RandomForestClassifier
}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.classification.{
  GBTClassificationModel,
  GBTClassifier
}

//import org.apache.spark.mllib.feature.Stemmer

import java.security.MessageDigest
import java.util
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import java.time.DateTimeException
import java.sql.Savepoint

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object Random {
  def getMadidsFromShareThisWithGEO(spark: SparkSession) {
    val data_us = spark.read
      .format("csv")
      .option("sep", ",")
      .load("/datascience/test_us/loading/*.json")
      .filter("_c2 is not null")
      .withColumnRenamed("_c0", "d17")
      .withColumnRenamed("_c5", "city")
      .select("d17", "city")
    data_us.persist()
    println(
      "LOGGING RANDOM: Total number of records: %s".format(data_us.count())
    )

    val estid_mapping = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/datascience/matching_estid")

    val joint = data_us
      .distinct()
      .join(estid_mapping, Seq("d17"))
      .select("device_id", "city")

    joint.write
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/shareThisWithGEO")
  }

  def getCrossDevice(spark: SparkSession) {
    val db_index = spark.read
      .format("parquet")
      .load("/datascience/crossdevice/double_index/")
      .filter("device_type IN ('a', 'i')")
      .withColumn("index", upper(col("index")))

    val st_data = spark.read
      .format("parquet")
      .load("/datascience/custom/shareThisWithGEO")
      .withColumn("device_id", upper(col("device_id")))
    println(
      "LOGGING RANDOM: Total number of records with GEO and device id: %s"
        .format(st_data.count())
    )

    val cross_deviced = db_index
      .join(st_data, db_index.col("index") === st_data.col("device_id"))
      .select("device", "device_type", "city")

    cross_deviced.write
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/madidsShareThisWithGEO")
  }

  def getEstIdsMatching(spark: SparkSession, day: String) = {
    val df = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/data/eventqueue/%s/*.tsv.gz".format(day))
      .filter(
        "d17 is not null and country = 'US' and event_type = 'sync'"
      )
      .select("d17", "device_id", "device_type")
      .withColumn("day", lit(day))
      .dropDuplicates()

    df.write
      .format("parquet")
      .partitionBy("day")
      .mode("append")
      .save("/datascience/sharethis/estid_table/")
  }

  def process_geo(spark: SparkSession) {
    val path = "/datascience/test_us/loading/"
    val data = spark.read
      .csv(path)
      .withColumnRenamed("_c0", "estid")
      .withColumnRenamed("_c1", "utc_timestamp")
      .withColumnRenamed("_c2", "latitude")
      .withColumnRenamed("_c3", "longitude")
      .withColumnRenamed("_c4", "zipcode")
      .withColumnRenamed("_c5", "city")
      .withColumn("day", lit(DateTime.now.toString("yyyyMMdd")))
    data.write
      .format("parquet")
      .mode("append")
      .partitionBy("day")
      .save("/datascience/geo/US/")
  }

  def generate_index(spark: SparkSession) {
    val df = spark.read
      .format("parquet")
      .load("/datascience/data_demo/triplets_segments")
      .limit(50000)
    val indexer1 =
      new StringIndexer().setInputCol("device_id").setOutputCol("deviceIndex")
    val indexed1 = indexer1.fit(df).transform(df)
    val indexer2 =
      new StringIndexer().setInputCol("feature").setOutputCol("featureIndex")
    val indexed2 = indexer2.fit(indexed1).transform(indexed1)

    val tempDF = indexed2
      .withColumn("deviceIndex", indexed2("deviceIndex").cast("long"))
      .withColumn("featureIndex", indexed2("featureIndex").cast("long"))
      .withColumn("count", indexed2("count").cast("double"))

    tempDF.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_demo/triplets_segments_indexed")
  }

  def generate_data_leo(spark: SparkSession) {
    val xd_users = spark.read
      .format("parquet")
      .load("/datascience/crossdevice/double_index")
      .filter("device_type = 'c'")
      .limit(2000)

    val xd = spark.read
      .format("csv")
      .option("sep", "\t")
      .load("/datascience/audiences/crossdeviced/taxo")
      .withColumnRenamed("_c0", "index")
      .withColumnRenamed("_c1", "device_type")
      .withColumnRenamed("_c2", "segments_xd")
      .drop("device_type")

    val joint = xd.join(broadcast(xd_users), Seq("index"))
    joint
      .select("segments_xd", "device", "index")
      .limit(200000)
      .write
      .format("csv")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_leo")

  }

  def getXD(spark: SparkSession) {
    val data = spark.read
      .format("csv")
      .option("sep", "\t")
      .load("/datascience/devicer/processed/am_wall")
      .select("_c1")
      .withColumnRenamed("_c1", "index")
    val index = spark.read
      .format("parquet")
      .load("/datascience/crossdevice/double_index")
      .filter("index_type = 'c'")
    val joint = data.join(index, Seq("index"))

    joint
      .select("device", "device_type")
      .write
      .format("csv")
      .save("/datascience/audiences/crossdeviced/am_wall")
  }

  def getTapadIndex(spark: SparkSession) {
    val data = spark.read
      .format("csv")
      .option("sep", ";")
      .load("/data/crossdevice/tapad/Retargetly_ids_full_20190128_161746.bz2")
    val devices = data
      .withColumn("ids", split(col("_c2"), "\t"))
      .withColumn("device", explode(col("ids")))
      .withColumn("device_type", split(col("device"), "=").getItem(0))
      .withColumn("device", split(col("device"), "=").getItem(1))
      .withColumnRenamed("_c0", "HOUSEHOLD_CLUSTER_ID")
      .withColumnRenamed("_c1", "INDIVIDUAL_CLUSTER_ID")
      .select(
        "HOUSEHOLD_CLUSTER_ID",
        "INDIVIDUAL_CLUSTER_ID",
        "device",
        "device_type"
      )

    devices.write.format("parquet").save("/datascience/custom/tapad_index/")
  }

  def getTapadNumbers(spark: SparkSession) {
    val data = spark.read.load("/datascience/custom/tapad_index")
    println("LOGGER RANDOM")
    //data.groupBy("device_type").count().collect().foreach(println)
    //println("LOGGER RANDOM: HOUSEHOLD_CLUSTER_ID: %d".format(data.select("HOUSEHOLD_CLUSTER_ID").distinct().count()))
    //println("LOGGER RANDOM: INDIVIDUAL_CLUSTER_ID: %d".format(data.select("INDIVIDUAL_CLUSTER_ID").distinct().count()))
    //println("LOGGER RANDOM: Number of devices: %d".format(data.count()))
    val counts_id = data
      .groupBy("INDIVIDUAL_CLUSTER_ID", "device_type")
      .count()
      .groupBy("device_type")
      .agg(
        count(col("count")),
        avg(col("count")),
        min(col("count")),
        max(col("count"))
      )
    counts_id.collect().foreach(println)
  }

  /**
    * This method returns a DataFrame with the data from the audiences data pipeline, for the interval
    * of days specified. Basically, this method loads the given path as a base path, then it
    * also loads the every DataFrame for the days specified, and merges them as a single
    * DataFrame that will be returned.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.
    *
    * @return a DataFrame with the information coming from the data read.
  **/
  def getDataAudiences(
      spark: SparkSession,
      nDays: Int = 30,
      since: Int = 1
  ): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_audiences_streaming/"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/hour=%s*".format(day))
      // .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)
    fs.close()

    df
  }

  def getTapadOverlap(spark: SparkSession) {
    val data = spark.read
      .load("/datascience/custom/tapad_index")
      .withColumnRenamed("device", "device_id")
    val users =
      getDataAudiences(spark, 40, 15).select("device_id", "country").distinct()

    val joint = data.join(users, Seq("device_id"))
    //joint.cache()
    // Total matching
    //println("\n\nLOGGER TOTAL MATCHING: %s\n\n".format(joint.count()))
    // Devices count by country
    //joint.groupBy("country").count().collect().foreach(println)
    // Individuals per country
    //println("\n\nLOGGER TOTAL MATCHING: %s\n\n".format(joint.select("INDIVIDUAL_CLUSTER_ID").distinct().count()))

    // Device types per country
    val total_joint = data.join(
      joint.select("INDIVIDUAL_CLUSTER_ID", "country").distinct(),
      Seq("INDIVIDUAL_CLUSTER_ID")
    )
    total_joint.cache()
    // Total matching
    println("\n\nTotal devices matched: %s\n\n".format(total_joint.count()))
    // Devices count by country
    total_joint
      .groupBy("country", "device_type")
      .count()
      .collect()
      .foreach(println)
  }

  def getNetquest(spark: SparkSession) {
    val nDays = 10
    val from = 1
    // Now we get the list of days to be downloaded
    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(from)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val files = days.map(day => "/data/eventqueue/%s/*.tsv.gz".format(day))

    val data = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load(files: _*)
      .select(
        "device_id",
        "device_type",
        "id_partner_user",
        "id_partner",
        "event_type",
        "country"
      )
      .filter(
        "country in ('AR', 'MX') AND event_type = 'sync' AND id_partner = 31"
      )
      .groupBy("device_id", "device_type", "id_partner_user", "country")
      .count()
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/netquest_match")
  }

  def get_data_leo_third_party(spark: SparkSession) {
    val join_leo = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/datascience/join_leo.tsv")
      .withColumn("device_id", split(col("device_id"), ","))
      .withColumn("device_id", explode(col("device_id")))

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(86)
    val end = DateTime.now.minusDays(26)

    val daysCount = Days.daysBetween(start, end).getDays()
    val days =
      (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

    val dfs = days
      .filter(
        x =>
          fs.exists(
            new org.apache.hadoop.fs.Path(
              "/datascience/data_audiences_p/day=%s".format(x)
            )
          )
      )
      .map(
        x =>
          spark.read.parquet("/datascience/data_audiences_p/day=%s".format(x))
      )
    fs.close()
    val df = dfs.reduce((df1, df2) => df1.union(df2))

    val joint = join_leo.join(df, Seq("device_id"))

    val udfJoin = udf(
      (lista: Seq[String]) =>
        if (lista.length > 0) lista.reduce((seg1, seg2) => seg1 + "," + seg2)
        else ""
    )
    val to_csv = joint
      .select("device_id", "third_party", "xd_drawbridge", "user")
      .withColumn("third_party", udfJoin(col("third_party")))
      .write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .save("/datascience/data_leo_third_party.tsv")
  }

  /**
    *
    *
    *
    *
    *
    * SAFEGRAPH STATISTICS
    *
    *
    *
    *
   **/
  def get_safegraph_metrics(spark: SparkSession) = {
    //This function calculates montly metrics from safegraph

    val country = "mexico"

    val df_safegraph = spark.read
      .option("header", "true")
      .csv("hdfs://rely-hdfs/data/geo/safegraph/2018/12/*")
      .filter("country = '%s'".format(country))
      .select("ad_id", "id_type", "latitude", "longitude", "utc_timestamp")
      .withColumnRenamed("latitude", "latitude_user")
      .withColumnRenamed("longitude", "longitude_user")
      .withColumn(
        "geocode",
        ((abs(col("latitude_user").cast("float")) * 10)
          .cast("int") * 10000) + (abs(
          col("longitude_user").cast("float") * 100
        ).cast("int"))
      )

    val df_safe = df_safegraph
      .select("ad_id", "id_type", "utc_timestamp")
      .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
      .withColumn("Day", date_format(col("Time"), "d"))
      .withColumn("Month", date_format(col("Time"), "M"))

    df_safe.cache()

    //usarios unicos por día
    val df_user_day = (df_safe
      .select(col("ad_id"), col("Day"))
      .distinct())
      .groupBy(col("Day"))
      .count()

    //usuarios unicos en un mes
    val df_user_month = (df_safe
      .select(col("ad_id"), col("Month"))
      .distinct())
      .groupBy(col("Month"))
      .count()

    //promedio de señales por usuario por dia
    val df_user_signal = df_safe
      .groupBy(col("ad_id"), col("Month"))
      .agg(count("id_type").alias("signals"))
      .agg(avg(col("signals")))

    println("Unique users per day")
    df_user_day.show(30)

    println("Mean signals per day")
    df_user_signal.show(30)

    println("Unique users per Month")
    df_user_month.show(2)

    //

    val df_user_count_signals =
      df_safe.groupBy(col("ad_id"), col("Day")).count()
    df_user_count_signals.cache()

    val mayor2 = df_user_count_signals.filter(col("count") >= 2).count()
    println("signals >=2", mayor2)

    val mayor20 = df_user_count_signals.filter(col("count") >= 20).count()
    println("signals >=20", mayor20)

    val mayor80 = df_user_count_signals.filter(col("count") >= 80).count()
    println("signals >=80", mayor80)
  }

  /**
    *
    *
    *
    * KOCHAVA
    *
    *
    *
    */
  def kochava_metrics(spark: SparkSession) = {

    val df_safegraphito = spark.read
      .option("header", "true")
      .csv("hdfs://rely-hdfs/data/geo/safegraph/2018/12/*")
      .filter("country = 'mexico'")
    val kocha = spark.read
      .option("header", "false")
      .csv("hdfs://rely-hdfs/data/geo/kochava/Mexico")
      .withColumnRenamed("_c0", "ad_id")
      .withColumnRenamed("_c2", "timestamp")
      .withColumn("Day", date_format(col("timestamp"), "d"))
      .withColumn("Month", date_format(col("timestamp"), "M"))

    kocha.cache()

    //usarios unicos por día
    val df_user_day = (kocha
      .select(col("ad_id"), col("Day"))
      .distinct())
      .groupBy(col("Day"))
      .count()

    //usuarios unicos en un mes
    val df_user_month = (kocha
      .select(col("ad_id"), col("Month"))
      .distinct())
      .groupBy(col("Month"))
      .count()

    //promedio de señales por usuario por dia
    val df_user_signal = kocha
      .groupBy(col("ad_id"), col("Month"))
      .agg(count("_c1").alias("signals"))
      .agg(avg(col("signals")))

    //total unique users kochava

    //common users
    val common = df_safegraphito
      .select(col("ad_id"))
      .join(kocha.select(col("ad_id")), Seq("ad_id"))
      .distinct()
      .count()

    println("Months of Data in Kochava")
    println(kocha.select(col("Month")).distinct().count())

    println("Unique users per day")
    df_user_day.show(32)

    println("Unique users per Month")
    df_user_month.show()

    println("Mean signals per day")
    df_user_signal.show(32)

    println("Common Users")
    println(common)

    val df_user_count_signals = kocha.groupBy(col("ad_id"), col("Day")).count()
    df_user_count_signals.cache()

    val mayor2 = df_user_count_signals.filter(col("count") >= 2).count()
    println("signals >=2", mayor2)

    val mayor20 = df_user_count_signals.filter(col("count") >= 20).count()
    println("signals >=20", mayor20)

    val mayor80 = df_user_count_signals.filter(col("count") >= 80).count()
    println("signals >=80", mayor80)
  }

  /**
    *
    *
    *
    * Geo Metrics Sample
    *
    *
    *
    */
  def get_geo_sample_data(spark: SparkSession) = {
    //loading user files with geolocation, added drop duplicates to remove users who are detected in the same location
    // Here we load the data, eliminate the duplicates so that the following computations are faster, and select a subset of the columns
    // Also we generate a new column call 'geocode' that will be used for the join

    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val nDays = 10
    val since = 10
    val country = "argentina"

    // Get the days to be loaded
    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val path = "/data/geo/safegraph/"

    // Now we obtain the list of hdfs folders to be read

    val hdfs_files = days
      .map(day => path + "%s/".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      .map(day => day + "*.gz")
    fs.close()

    val df_safegraph = spark.read
      .option("header", "true")
      .csv(hdfs_files: _*)
      .filter("country = '%s'".format(country))
      .select("ad_id", "id_type", "utc_timestamp")
      .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
      .withColumn("Day", date_format(col("Time"), "d"))
      .withColumn("Month", date_format(col("Time"), "M"))
      .write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .save("/datascience/geo/safegraph_10d.tsv")
    /*
    df_safegraph.cache()
    //usarios unicos por día
    val df_user_day_safe = (df_safegraph
                        .select(col("ad_id"), col("Day"))
                        .distinct())
                        .groupBy(col("Day"))
                        .count()

            println("Unique users per day - safegraph")
            df_user_day_safe.show()

    //usuarios unicos en un mes
//   val df_user_month = (df_safegraph  .select(col("ad_id"), col("Month")).distinct()).groupBy(col("Month")).count()

    //promedio de señales por usuario por dia
    val df_user_signal_safe = df_safegraph
                            .groupBy(col("ad_id"), col("Month"))
                            .agg(count("id_type").alias("signals"))
                            .agg(avg(col("signals")))


    println("Mean signals per user - safegraph",df_user_signal_safe)

////////////////////////
//Now the sample dataset

val df_sample = spark.read.option("header", "true").option("delimiter", ",").csv("hdfs://rely-hdfs/datascience/geo/sample")
          .withColumn("day", date_format(to_date(col("timestamp")), "d"))
          .withColumn("month", date_format(to_date(col("timestamp")), "M"))

        df_sample.cache()

            //Unique MAIDs
            val unique_MAIDSs = df_sample.select(col("identifier")).distinct().count()
            println("Unique MAIDs - sample dataset",unique_MAIDSs)

            //Total number of events
            val number_events = df_sample.select(col("record_id")).distinct().count()
            println("Total Events - sample dataset",number_events)

            //usarios unicos por día
            val df_user_day = (df_sample.select(col("identifier"),col("day"))
                              .distinct())
                              .groupBy(col("Day"))
                              .count()

            println("Unique users per day - sample dataset")
            df_user_day.show()

//Mean, median, mode y range de events/MAID

//promedio de señales por usuario por dia
      val df_user_signal = df_sample.groupBy(col("identifier"),col("day"))
                                    .agg(count("identifier_type")
                                    .alias("signals"))
                                    .agg(avg(col("signals")))

 println("Mean signals per user - sample data",df_user_signal)

//Overlap de MAIDs con current data set (buscamos entender el incremental de IDs)

//common users
val the_join = df_sample.join(df_safegraph, df_sample.col("identifier")===df_safegraph.col("ad_id"),"inner")

val common = the_join.distinct().count()

println("Common Users with Safegraph",common)

//Overlap de events/MAID con current data set (buscamos entender si nos provee más puntos por ID aunque no necesariamente traiga mas IDs).

 val records_common_safegraph = the_join.groupBy(col("identifier"))
                      .agg(count("utc_timestamp").alias("records_safegraph"))
                      .agg(avg(col("records_safegraph")))

println("Records in Sample in safegraph",records_common_safegraph)

val records_common_sample = the_join.select(col("ad_id"),col("timestamp"))
                .groupBy(col("ad_id")).agg(count("timestamp").alias("records_sample"))
                      .agg(avg(col("records_sample")))

println("Records in Sample in common users",records_common_sample)


val records_common = the_join.select(col("identifier"))
                      .groupBy(col("identifier")).agg(count("timestamp").alias("records_safegraph"))
                      .agg(count("utc_timestamp").alias("records_sample"))
                      .withColumn("ratio", col("records_safegraph") / col("records_sample"))


   the_join.write
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/sample_metrics")

   */

  }

  /**
    *
    *
    *
    * Downloading safegraph data for GCBA study (porque no hay job que haga polygon matching aun)
    * Recycled for McDonalds
    *
    *
    */
  def get_safegraph_data(
      spark: SparkSession,
      nDays: Integer,
      country: String,
      since: Integer = 1
  ) = {
    //loading user files with geolocation, added drop duplicates to remove users who are detected in the same location
    // Here we load the data, eliminate the duplicates so that the following computations are faster, and select a subset of the columns
    // Also we generate a new column call 'geocode' that will be used for the join
    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    //dictionary for timezones
    val timezone = Map("argentina" -> "GMT-3", "mexico" -> "GMT-5")

    //setting timezone depending on country
    spark.conf.set("spark.sql.session.timeZone", timezone(country))

    // Now we obtain the list of hdfs folders to be read
    val path = "/data/geo/safegraph/"
    val hdfs_files = days.map(day => path + "%s/*.gz".format(day))
    val df_safegraph = spark.read
      .option("header", "true")
      .csv(hdfs_files: _*)
      .filter("country = '%s'".format(country))
      .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
      .withColumn("Hour", date_format(col("Time"), "HH"))
      .withColumn("Day", date_format(col("Time"), "d"))
      .withColumn("Month", date_format(col("Time"), "M"))

    /* val geo_audience = spark.read
      .option("delimiter", "\t")
      .csv("/datascience/geo/McDonaldsCalleARG_90d_argentina_19-3-2019-6h")

    df_safegraph
      .join(
        geo_audience.select(col("_c1")),
        geo_audience("_c1") === df_safegraph("ad_id"),
        "leftanti"
      )
     */

    val high_user = df_safegraph
      .groupBy("ad_id")
      .agg(count("latitude").as("freq"))
      .filter("freq > 500")

    val high_data =
      high_user.join(df_safegraph, Seq("ad_id"), "inner").distinct()

    high_data.write
      .mode(SaveMode.Overwrite)
      .option("header", "false")
      .format("csv")
      .option("sep", ",")
      .save("/datascience/geo/AR/safegraph_sample_15d_high")

  }

  /**
    *
    *
    *
    * TAPAD STUDY
    *
    *
    *
    */
  def getTapadPerformance(spark: SparkSession) = {
    // First we load the index with the association for TapAd
    // This file has 4 columns: "HOUSEHOLD_CLUSTER_ID", "INDIVIDUAL_CLUSTER_ID", "device", and "device_type"
    val tapadIndex = spark.read
      .format("parquet")
      .load("/datascience/custom/tapad_index/")
      .select("INDIVIDUAL_CLUSTER_ID", "device")
      .withColumnRenamed("device", "device_id")
      .withColumn("device_id", upper(col("device_id")))

    // Now we load the PII data.
    // This file contains 10 columns: "device_id", "device_type","country","id_partner","data_type","ml_sh2", "mb_sh2", "nid_sh2","day"
    val piiData = spark.read
      .format("parquet")
      .load("/datascience/pii_matching/pii_tuples")
      .select("device_id", "nid_sh2", "country")
      .filter("nid_sh2 IS NOT NULL and length(nid_sh2)>0")
      .withColumn("device_id", upper(col("device_id")))
      .distinct()

    piiData
      .join(tapadIndex, Seq("device_id"))
      .select("INDIVIDUAL_CLUSTER_ID", "nid_sh2", "country", "device_id")
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("sep", " ")
      .save("/datascience/custom/tapad_pii")
  }

  def getDBPerformance(spark: SparkSession) = {
    // First we load the index with the association for Drawbridge
    val tapadIndex = spark.read
      .format("parquet")
      .load("/datascience/crossdevice/double_index/")
      .filter("device_type = 'dra'")
      .withColumnRenamed("device", "INDIVIDUAL_CLUSTER_ID")
      .withColumnRenamed("index", "device_id")
      .select("INDIVIDUAL_CLUSTER_ID", "device_id")
      .withColumn("device_id", upper(col("device_id")))

    // Now we load the PII data.
    // This file contains 10 columns: "device_id", "device_type","country","id_partner","data_type","ml_sh2", "mb_sh2", "nid_sh2","day"
    val piiData = spark.read
      .format("parquet")
      .load("/datascience/pii_matching/pii_tuples")
      .select("device_id", "nid_sh2", "country")
      .filter("nid_sh2 IS NOT NULL and length(nid_sh2)>0")
      .withColumn("device_id", upper(col("device_id")))
      .distinct()

    piiData
      .join(tapadIndex, Seq("device_id"))
      .select("INDIVIDUAL_CLUSTER_ID", "nid_sh2", "country", "device_id")
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("sep", " ")
      .save("/datascience/custom/db_pii")
  }

  /**
    *
    *
    *
    * Segments for Geo Data
    *
    *
    *
    */
  def getSegmentsforUsers(spark: SparkSession) = {

    //este es el resultado del crossdevice, se pidieron solo cookies
    val estacion_xd = spark.read
      .option("header", "false")
      .option("delimiter", ",")
      .csv(
        "hdfs://rely-hdfs/datascience/audiences/crossdeviced/estaciones_servicio_12_02_19_poimatcher_60d_DISTINCT_xd"
      )
      .select(col("_c1"))
      .distinct()
      .withColumnRenamed("_c1", "device_id")

    // Ahora levantamos los datos que estan en datascience keywords

    val since = 6
    val nDays = 7
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString("yyyyMMdd"))

    val lista_files = (0 until nDays)
      .map(end.minusDays(_))
      .map(
        day =>
          "hdfs://rely-hdfs/datascience/data_keywords/day=%s"
            .format(day.toString("yyyyMMdd"))
      )

    val segments = spark.read
      .format("parquet")
      .option("basePath", "hdfs://rely-hdfs/datascience/data_keywords/")
      .load(lista_files: _*)
      .withColumn("segmentos", concat_ws(",", col("segments")))
      .select("device_id", "segmentos")

    val estajoin = segments.join(estacion_xd, Seq("device_id"))

    val seg_group = estajoin
      .withColumn("segment", explode(split(col("segmentos"), ",")))
      .select("device_id", "segment")
      .groupBy("segment")
      .agg(countDistinct("device_id"))

    seg_group.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/AR/estaciones_servicio_MP_DRAW_segmentos_7d")

  }

  /**
    *
    *
    *
    * Segments for Geo Report Sarmiento
    *
    *
    *
    */
  def get_sarmiento_segments(
      spark: SparkSession,
      nDays: Integer,
      since: Integer = 1
  ) = {

    // Ahora levantamos los datos que estan en datascience keywords

    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=AR".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    fs.close()

    val segments = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    // Importamos implicits para que funcione el as[String]

    import spark.implicits._

    //cargamos la data de los usuarios XD. Sólo nos quedamos con los códigos y el device_id
    val pois = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .csv(
        "hdfs://rely-hdfs/datascience/geo/geo_processed/Sarmiento_2797_puntos_60d_argentina_20-5-2019-7h_aggregated"
      )
      .select("device_id", "name")
      .distinct()
    //.withColumnRenamed("_c0", "device_id")
    //.withColumnRenamed("_c1", "Codigo")

    //hacemos el join
    val joint = pois.join(segments, Seq("device_id")) //.withColumn("segments", explode(col("segments")))

    //explotamos
    val exploded = joint.withColumn("segments", explode(col("segments")))

    //reemplazamos para filtrar
    // val filtered = exploded
    //   .withColumn("segments", regexp_replace(col("segments"), "s_", ""))
    //  .withColumn("segments", regexp_replace(col("segments"), "as_", ""))

    // val taxo_general = spark.read
    //  .format("csv")
    // .option("sep", ",")
    //.option("header", "True")
    // .load("/datascience/data_publicis/taxonomy_publicis.csv")

    // val taxo_segments = taxo_general.select("Segment Id").as[String].collect()

//.filter(col("segments").isin(taxo_segments: _*))
    exploded
      .groupBy("name", "segments")
      .count()
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(
        "/datascience/geo/AR/sarmiento_pois_actualizado_60d_30dsegments_20_05"
      )

  }

  /**
    *
    *
    *
    * Segments for Geo Data for TAPAD
    *
    *
    *
    */
  def getSegmentsforTapad(spark: SparkSession) = {

    //este es el resultado del crossdevice, se pidieron solo cookies
    //val users = spark.read.format("csv").load("/datascience/geo/AR/estaciones_servicio_jue_vie")
    val users = spark.read
      .format("csv")
      .load(
        "hdfs://rely-hdfs/datascience/geo/AR/estaciones_servicio_12_02_19_poimatcher_60d_DISTINCT"
      )

    val tapad_index = spark.read.load("/datascience/custom/tapad_index")

    val joined =
      users.join(tapad_index, tapad_index.col("device") === users.col("_c0"))
    val clusters = joined.select(col("INDIVIDUAL_CLUSTER_ID")).distinct()
    val all_devices = clusters.join(tapad_index, Seq("INDIVIDUAL_CLUSTER_ID"))
    val cookies = all_devices
      .filter(col("device_type") === "RTG")
      .dropDuplicates("device")
      .withColumnRenamed("device_type", "RTG")

    // Ahora levantamos los datos que estan en datascience keywords

    val since = 6
    val nDays = 7
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString("yyyyMMdd"))

    val lista_files = (0 until nDays)
      .map(end.minusDays(_))
      .map(
        day =>
          "hdfs://rely-hdfs/datascience/data_keywords/day=%s"
            .format(day.toString("yyyyMMdd"))
      )

    val segments = spark.read
      .format("parquet")
      .option("basePath", "hdfs://rely-hdfs/datascience/data_keywords/")
      .load(lista_files: _*)
      .withColumn("segmentos", concat_ws(",", col("segments")))
      .select("device_id", "segmentos")

    /**
      *
  // Ahora levantamos los datos que estan en datascience keywords
    val segments = spark.read
      .format("parquet")
      .parquet("hdfs://rely-hdfs/datascience/data_keywords/day=20190227")
      .withColumn("segmentos", concat_ws(",", col("segments")))
      .select("device_id", "segmentos")
      .withColumn("device_id", upper(col("device_id")))
      */
    val finaljoin = segments.join(
      cookies,
      cookies.col("device") === segments.col("device_id")
    )

    finaljoin.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/AR/estaciones_servicio_MP_segmentos_TAPAD_7d")

  }

  /**
    *
    *
    *
    * Get Data for Safegraph to evaluate quality
    *
    *
    *
    */
  def get_safegraph_counts(spark: SparkSession) = {
    //loading user files with geolocation, added drop duplicates to remove users who are detected in the same location
    // Here we load the data, eliminate the duplicates so that the following computations are faster, and select a subset of the columns
    // Also we generate a new column call 'geocode' that will be used for the join

    //hardcoded variables
    val nDays = 60
    val since = 2
    val country = "argentina"

    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(since.toInt)
    val days =
      (0 until nDays.toInt).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val path = "/data/geo/safegraph/"

    // Now we obtain the list of hdfs folders to be read

    val hdfs_files = days
      .map(day => path + "%s/".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      .map(day => day + "*.gz")
    fs.close()

    val df_safegraph = spark.read
      .option("header", "true")
      .csv(hdfs_files: _*)
      .dropDuplicates("ad_id", "latitude", "longitude")
      .filter("country = '%s'".format(country))
      .select("ad_id", "id_type", "latitude", "longitude", "utc_timestamp")
      .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
      .withColumn("Hour", date_format(col("Time"), "HH"))
      .withColumn("Day", date_format(col("Time"), "d"))
      .withColumn("Week", date_format(col("Time"), "w"))
      .withColumn("Weekday", date_format(col("Time"), "EEEE"))
      .withColumn("Month", date_format(col("Time"), "M"))

    //number of users by day of month
    val user_day = df_safegraph
      .select(col("ad_id"), col("Day"), col("Month"))
      .distinct()
      .groupBy("Month", "Day")
      .count()

    println("Users by Day", user_day)

    //number of users by day of week, note that we need the week number (we can have more mondays than fridays in a month)
    val user_week_day = df_safegraph
      .select(col("ad_id"), col("Weekday"), col("Week"))
      .distinct()
      .groupBy("Weekday", "Week")
      .count()

    println("Users by Weekday", user_week_day)

    //number of users by hour by weekday
    val user_hour = df_safegraph
      .select(col("ad_id"), col("Hour"), col("Weekday"), col("Week"))
      .distinct()
      .groupBy("Hour", "Weekday")
      .count()

    user_hour.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/AR/safegraph_user_hours_11_03_60d")

    //users and number of timestamps per user
    val user_pings = df_safegraph
      .select(col("ad_id"), col("Week"), col("utc_timestamp"))
      .distinct()
      .groupBy("ad_id")
      .agg(count("utc_timestamp").alias("signals"))

    user_pings.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/AR/safegraph_user_pings_11_03_60d")

    //users and timestamps for that user
    val user_timestamp = df_safegraph
      .select(
        col("ad_id"),
        col("latitude"),
        col("longitude"),
        col("utc_timestamp")
      )
      .distinct()
      .withColumn("time_pings", concat_ws(",", col("utc_timestamp")))
      .groupBy("ad_id", "latitude", "longitude", "time_pings")
      .agg(count("utc_timestamp").alias("signals"))

//.withColumn("segmentos",concat_ws(",",col("segments"))).select("device_id","segmentos")

    user_timestamp.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/AR/safegraph_user_timestamps_11_03_60d")

  }

  def get_tapad_vs_drawbridge(spark: SparkSession) = {

    val tapad_index = spark.read
      .load("/datascience/custom/tapad_index")
      .withColumn("device", upper(col("device")))
    val draw_index = spark.read
      .load("/datascience/crossdevice/double_index")
      .withColumn("device", upper(col("device")))

    val overlap =
      tapad_index.select(col("device")).join(draw_index, Seq("device"))
    overlap
      .groupBy("device_type")
      .agg(countDistinct("device"))
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/overlap_tapad_drawbridge")

    val only_tapad = tapad_index.except(draw_index)

    only_tapad
      .groupBy("device_type")
      .agg(countDistinct("device"))
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/only_tapad")

    val only_draw = draw_index.except(tapad_index)

    only_draw
      .groupBy("device_type")
      .agg(countDistinct("device"))
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/only_draw")

  }

  /**
    *
    *
    *
    *
    * AUDIENCE for US
    *
    *
    *
    *
   **/
  def getUSaudience(spark: SparkSession) = {

    val nDays = 30
    val today = DateTime.now()
    val lista_files = (2 until nDays)
      .map(today.minusDays(_))
      .map(
        day =>
          "/datascience/sharethis/historic/day=%s"
            .format(day.toString("yyyyMMdd"))
      )

    val segments = spark.read
      .format("parquet")
      .option("basePath", "/datascience/sharethis/historic/")
      .load(lista_files: _*)

    segments
      .filter(
        "(((((((((((((((((((((((((((((((((((((((((((((((url LIKE '%pipeline%' and url LIKE '%data%') or (url LIKE '%colab%' and url LIKE '%google%')) or (url LIKE '%shiny%' and url LIKE '%rstudio%')) or (url LIKE '%spark%' and url LIKE '%apache%')) or url LIKE '%hadoop%') or url LIKE '%scala%') or url LIKE '%kubernetes%') or url LIKE '%gitlab%') or url LIKE '%mongodb%') or url LIKE '%netbeans%') or url LIKE '%elasticsearch%') or url LIKE '%nginx%') or url LIKE '%angularjs%') or url LIKE '%bootstrap%') or url LIKE '%atlassian%') or url LIKE '%jira%') or url LIKE '%gitlab%') or url LIKE '%docker%') or url LIKE '%github%') or url LIKE '%bitbucket%') or (url LIKE '%lake%' and url LIKE '%data%')) or (url LIKE '%warehouse%' and url LIKE '%data%')) or url LIKE '%mariadb%') or (url LIKE '%machines%' and url LIKE '%virtual%')) or url LIKE '%appserver%') or (url LIKE '%learning%' and url LIKE '%machine%')) or url LIKE '%nosql%') or url LIKE '%mysql%') or (url LIKE '%sagemaker%' and url LIKE '%amazon%')) or (url LIKE '%lightsail%' and url LIKE '%amazon%')) or (url LIKE '%lambda%' and url LIKE '%aws%')) or (url LIKE '%aurora%' and url LIKE '%amazon%')) or (url LIKE '%dynamodb%' and url LIKE '%amazon%')) or (url LIKE '%s3%' and url LIKE '%amazon%')) or ((url LIKE '%platform%' and url LIKE '%cloud%') and url LIKE '%google%')) or (url LIKE '%storage%' and url LIKE '%amazon%')) or (url LIKE '%ec2%' and url LIKE '%amazon%')) or (url LIKE '%server%' and url LIKE '%web%')) or (url LIKE '%servers%' and url LIKE '%cloud%')) or (url LIKE '%hosting%' and url LIKE '%cloud%')) or url LIKE '%azure%') or url LIKE '%aws%') or ((url LIKE '%services%' and url LIKE '%cloud%') and url LIKE '%amazon%')) or ((url LIKE '%services%' and url LIKE '%web%') and url LIKE '%amazon%')) or (url LIKE '%server%' and url LIKE '%windows%')) or (url LIKE '%azure%' and url LIKE '%windows%')) or (url LIKE '%azure%' and url LIKE '%microsoft%'))"
      )
      .select(col("estid"))
      .distinct()
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/audiences/output/micro_azure_06-03")

  }

  def getUSmapping(spark: SparkSession) = {

//eso era para obtener las cookies
// val nDays = 30
//val today = DateTime.now()
//val other_files = (2 until nDays).map(today.minusDays(_))  .map(day => "/datascience/sharethis/estid_table/day=%s"
//  .format(day.toString("yyyyMMdd")))

//val estid_mapping = spark.read.format("parquet") .option("basePath","/datascience/sharethis/estid_table/")
//.load(other_files: _*)

    val estid_mapping = spark.read
      .format("parquet")
      .load("/datascience/sharethis/estid_madid_table")

    val output = spark.read
      .format("csv")
      .load("/datascience/audiences/output/micro_azure_06-03/")

    val joint = output
      .distinct()
      .join(estid_mapping, output.col("_c0") === estid_mapping.col("estid"))

    joint.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/audiences/output/micro_azure_06-03_xd_madid/")
  }

  /**
    *
    *
    *
    *
    * ATT  SAMPLE
    *
    *
    *
    *
   **/
  def encrypt(value: String): String = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    cipher.init(Cipher.ENCRYPT_MODE, keyToSpec())
    Base64.encodeBase64String(cipher.doFinal(value.getBytes("UTF-8")))
  }

  def decrypt(encryptedValue: String): String = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING")
    cipher.init(Cipher.DECRYPT_MODE, keyToSpec())
    new String(cipher.doFinal(Base64.decodeBase64(encryptedValue)))
  }

  def keyToSpec(): SecretKeySpec = {
    var keyBytes: Array[Byte] = (SALT + KEY).getBytes("UTF-8")
    val sha: MessageDigest = MessageDigest.getInstance("SHA-1")
    keyBytes = sha.digest(keyBytes)
    keyBytes = util.Arrays.copyOf(keyBytes, 16)
    new SecretKeySpec(keyBytes, "AES")
  }

  private val SALT: String =
    "jMhKlOuJnM34G6NHkqo9V010GhLAqOpF0BePojHgh1HgNg8^72k"

  private val KEY: String = "a51hgaoqpgh5bcmhyt1zptys=="

  def getSampleATT(spark: SparkSession) {
    val udfEncrypt = udf(
      (estid: String) => encrypt(estid)
    )

    val format = "yyyyMMdd"
    val from = 72
    val start = DateTime.now.minusDays(from)
    val days = (0 until 30).map(start.minusDays(_)).map(_.toString(format))

    def parseDay(day: String) = {
      println("LOGGER: processing day %s".format(day))
      spark.read
      // .format("com.databricks.spark.csv")
        .load("/datascience/sharethis/loading/%s*.json".format(day))
        .filter("_c13 = 'san francisco' AND _c8 LIKE '%att%'")
        .select(
          "_c0",
          "_c1",
          "_c2",
          "_c3",
          "_c4",
          "_c5",
          "_c6",
          "_c7",
          "_c8",
          "_c9"
        )
        .withColumn("_c0", udfEncrypt(col("_c0")))
        .coalesce(100)
        .write
        .mode(SaveMode.Overwrite)
        .format("csv")
        .option("sep", "\t")
        .save("/datascience/sharethis/sample_att_url/%s".format(day))
      println("LOGGER: day %s processed successfully!".format(day))
    }

    days.map(day => parseDay(day))
  }

  /**
    *
    *
    *
    *      Data GCBA for campaigns
    *
    *
    *
    */
  def gcba_campaign_day(spark: SparkSession, day: String) {

    val df = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", true)
      .load("/data/eventqueue/%s/*.tsv.gz".format(day))
      .select("id_partner", "all_segments", "url")
      .filter(
        col("id_partner") === "349" && col("all_segments")
          .isin(List("76522,76536,76543"): _*)
      )

    df.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/AR/gcba_campaign_%s".format(day))
  }

  /**
    *
    *
    *
    *      US GEO Sample
    *
    *
    *
    */
  def getSTGeo(spark: SparkSession) = {
    val format = "yyyyMMdd"
    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy")
    val start = formatter.parseDateTime("01/24/2019")
    val days =
      (0 until 40).map(n => start.plusDays(n)).map(_.toString(format))
    val path = "/datascience/sharethis/loading/"
    days.map(
      day =>
        spark.read
          .format("csv")
          .load(path + day + "*")
          .select("_c0", "_c1", "_c10", "_c11", "_c3", "_c5", "_c12", "_c13")
          .withColumnRenamed("_c0", "estid")
          .withColumnRenamed("_c1", "utc_timestamp")
          .withColumnRenamed("_c3", "ip")
          .withColumnRenamed("_c10", "latitude")
          .withColumnRenamed("_c11", "longitude")
          .withColumnRenamed("_c12", "zipcode")
          .withColumnRenamed("_c13", "city")
          .withColumnRenamed("_c5", "device_type")
          .withColumn("day", lit(day))
          .write
          .format("parquet")
          .partitionBy("day")
          .mode("append")
          .save("/datascience/geo/US/")
    )
    // spark.read.format("csv")
    //      .load("/datascience/sharethis/loading/*")
    //      .select("_c0", "_c1", "_c10", "_c11", "_c3", "_c5")
    //      .withColumnRenamed("_c0", "estid")
    //      .withColumnRenamed("_c1", "utc_timestamp")
    //      .withColumnRenamed("_c3", "ip")
    //      .withColumnRenamed("_c10", "latitude")
    //      .withColumnRenamed("_c11", "longitude")
    //      .withColumnRenamed("_c5", "device_type")
    //      .write
    //      .format("csv")
    //      .save("/datascience/custom/geo_st")
  }

  /**
    *
    *
    *
    *      Users Colectivo Trenes Transportandose
    *
    *
    *
    */
  def get_moving_users(spark: SparkSession) = {
    //Levantamos los usuarios generados por el proceso
    val trenes = spark.read
      .format("csv")
      .option("header", true)
      .option("delimiter", "\t")
      .load("/datascience/geo/trenes_GBA_365d_argentina_30-7-2019-10h/")

    //elegimos las columnas que queremos para el final
    val final_columns = Seq("device_id", "device_type", "user")

    //spliteamos columnas de nombre para poder agrupar por la línea (hay distintos ramales, pero bueno, perdemos un poco de falsa presición)
    val trenes_s = trenes
      .withColumn("temp", split(upper(col("name")), ";"))
      .select(
        col("*") +: (0 until 3)
          .map(i => col("temp").getItem(i).as(s"name$i")): _*
      )

    //acá le creamos las columnas de array de tiempos y de estaciones detectadas. filtramos para que sean más de una, si no, no podemos identificarlos
    val tren_line = trenes_s
      .groupBy("device_id", "device_type", "name0")
      .agg(
        collect_list(col("timestamp")).as("times_array"),
        collect_list("name2").as("location_array")
      )
      .withColumn("line_detect", size(col("times_array")))
      .withColumn("location_detect", size(col("location_array")))
      .filter("(line_detect >1) AND (location_detect >1)")

    //nos quedamos con los usuarios que estuvieron muy cerca de la estacion para tener mayor volumen
    val tren_low_proba = trenes_s
      .filter("distance<25")
      .withColumn("user", lit(false))
      .select(final_columns.map(c => col(c)): _*)

    //esta es la función para filtrar usuarios que: estuvieron en dos estaciones distintas con menos de 60 minutos de diferencia
    val hasUsedLine = udf(
      (timestamps: Seq[String], stopids: Seq[String], threshold: Integer) =>
        ((timestamps.slice(1, timestamps.length) zip timestamps)
          .map(t => t._1.toInt - t._2.toInt < threshold) zip (stopids
          .slice(1, stopids.length) zip stopids)
          .map(s => s._1 != s._2))
          .exists(b => b._1 & b._2)
    )

    //aplicamos la función
    val tren_user = tren_line
      .withColumn(
        "user",
        hasUsedLine(
          tren_line("times_array"),
          tren_line("location_array"),
          lit(60)
        )
      )
      .filter("user== true")
      .filter((col("name0") =!= "") && ((col("name0").isNotNull)))
      .select(final_columns.map(c => col(c)): _*)

    // juntamos los dos sets de usuarios
    val the_users_of_the_trains = List(tren_user, tren_low_proba)
      .reduce(_.unionByName(_))
      .distinct()

    the_users_of_the_trains.write
      .format("csv")
      .option("header", true)
      .option("delimiter", "\t")
      .mode(SaveMode.Overwrite)
      .save(
        "/datascience/audiences/crossdeviced/trenes_GBA_365d_argentina_30-7-2019-10h_mobile"
      )

  }

  def get_pii_AR(spark: SparkSession) {

    val df_pii = spark.read
      .option("header", "true")
      .parquet("hdfs://rely-hdfs/datascience/pii_matching/pii_table")
      .filter(col("country") === "AR")
      .write
      .format("csv")
      .option("sep", ",")
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .save("/datascience/audiences/output/pii_table_AR_11_02_19")

  }

  def get_urls_sharethis(spark: SparkSession, ndays: Int) {

    /**
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(ndays)
    val end   = DateTime.now.minusDays(0)

    val daysCount = Days.daysBetween(start, end).getDays()
    val days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))
    **/
    val path = "/datascience/sharethis/loading/"
    val days = List(
      "20181220",
      "20181221",
      "20181222",
      "20181223",
      "20181224",
      "20181225",
      "20181226",
      "20181227",
      "20181228",
      "20181229",
      "20181230",
      "20181231",
      "20190101",
      "20190103",
      "20190105",
      "20190107",
      "20190103",
      "20190109",
      "20190111",
      "20190113",
      "20190115",
      "20190117",
      "20190119",
      "20190121",
      "20190123",
      "20190124",
      "20190125",
      "20190126"
    )
    days.map(
      day =>
        spark.read
          .format("csv")
          .load(path + day + "*")
          .select("_c0", "_c1", "_c2", "_c5")
          .withColumnRenamed("_c0", "estid")
          .withColumnRenamed("_c1", "utc_timestamp")
          .withColumnRenamed("_c2", "url")
          .withColumnRenamed("_c5", "device_type")
          .withColumn("day", lit(day))
          .write
          .format("parquet")
          .partitionBy("day")
          .mode("append")
          .save("/datascience/sharethis/urls/")
    )
//        filter(day => fs.exists(new org.apache.hadoop.fs.Path(path + day + "*")))

  }

  /**
    *
    *
    *
    *
    *              GOOGLE ANALYTICS
    *
    *
    *
    *
    */
  def join_cadreon_google_analytics(spark: SparkSession) {
    val ga = spark.read
      .format("csv")
      .load("/datascience/data_demo/join_google_analytics/")
      .withColumnRenamed("_c1", "device_id")
    val cadreon = spark.read
      .format("csv")
      .option("sep", " ")
      .load("/datascience/devicer/processed/cadreon_age")
      .withColumnRenamed("_c1", "device_id")

    ga.join(cadreon, Seq("device_id"))
      .write
      .format("csv")
      .save("/datascience/custom/cadreon_google_analytics")
  }

  // def join_gender_google_analytics(spark: SparkSession) {
  //   val ga = spark.read
  //     .load("/datascience/data_demo/join_google_analytics/")
  //     .withColumnRenamed("_c1", "device_id")
  //   val gender = spark.read
  //     .format("csv")
  //     .option("sep", " ")
  //     .load("/datascience/devicer/processed/ground_truth_*male")
  //     .withColumnRenamed("_c1", "device_id")
  //     .withColumnRenamed("_c2", "label")
  //     .select("device_id", "label")
  //     .distinct()

  //   ga.join(gender, Seq("device_id"))
  //     .write
  //     .format("csv")
  //     .mode(SaveMode.Overwrite)
  //     .save("/datascience/data_demo/gender_google_analytics")
  // }

  // for MX "/datascience/devicer/processed/ground_truth_*male"
  // for AR "/datascience/devicer/processed/equifax_demo_AR_grouped"
  def join_gender_google_analytics(
      spark: SparkSession,
      gtPath: String,
      sep: String = "\t",
      gaPath: String = "join_google_analytics_path",
      country: String = "AR"
  ) {
    val ga = spark.read
      .load("/datascience/data_demo/%s/country=%s".format(gaPath, country))
    val gender = spark.read
      .format("csv")
      .option("sep", sep)
      .load(gtPath)
      .withColumnRenamed("_c1", "device_id")
      .withColumnRenamed("_c2", "label")
      .select("device_id", "label")

    ga.join(gender, Seq("device_id"))
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_demo/gender_%s_%s".format(gaPath, country))
  }

  def get_google_analytics_stats(spark: SparkSession) {
    val cols = (2 to 9).map("_c" + _).toSeq
    val data = spark.read
      .format("csv")
      .load("/datascience/data_demo/join_google_analytics")
      .select(cols.head, cols.tail: _*)
    data.registerTempTable("data")

    val q_std =
      cols.map(c => "stddev(%s) as stddev%s".format(c, c)).mkString(", ")
    val q_median = cols
      .map(c => "percentile_approx(%s, 0.5) as median%s".format(c, c))
      .mkString(", ")
    val query = "SELECT %s, %s FROM data".format(q_std, q_median)

    println("\n\n\n")
    spark.sql(query).show()
    println("\n\n\n")
  }

  /**
    *
    *
    *
    *
    *                  ATT - STATISTICS
    *
    *
    *
    *
    *
   **/
  def get_att_stats(spark: SparkSession) = {
    // val data = spark.read
    //   .load("/datascience/sharethis/historic/day=201902*/")
    //   .filter("isp LIKE '%att%' AND city = 'san francisco'")
    //   .select("estid", "url", "os", "ip", "utc_timestamp")
    //   .withColumn("utc_timestamp", col("utc_timestamp").substr(0, 10))
    //   .withColumnRenamed("utc_timestamp", "day")
    val data = spark.read.load("/datascience/custom/metrics_att_sharethis")
    // data.persist()
    // data.write
    //   .mode(SaveMode.Overwrite)
    //   .save("/datascience/custom/metrics_att_sharethis")

    // println("Total number of rows: %s".format(data.count()))
    // println(
    //   "Total Sessions (without duplicates): %s"
    //     .format(data.select("url", "estid").distinct().count())
    // )
    // println(
    //   "Total different ids: %s".format(data.select("estid").distinct().count())
    // )
    // println("Mean ids per day:")
    // data.groupBy("day").count().show()
    // data
    //   .select("url", "estid")
    //   //.distinct()
    //   .groupBy("estid")
    //   .count()
    //   .write
    //   .mode(SaveMode.Overwrite)
    //   .save("/datascience/custom/estid_sessions")
    // data
    //   .select("day", "estid")
    //   .distinct()
    //   .groupBy("day")
    //   .count()
    //   .write
    //   .mode(SaveMode.Overwrite)
    //   .save("/datascience/custom/estid_days")

    val estid_table =
      spark.read
        .load("/datascience/sharethis/estid_table/day=20190*")
        .withColumnRenamed("d17", "estid")

    val joint = estid_table.join(data, Seq("estid"))
    println("Total number of rows: %s".format(joint.count()))
    println(
      "Total Sessions (without duplicates): %s"
        .format(joint.select("url", "device_id").distinct().count())
    )
    println(
      "Total different device ids: %s"
        .format(joint.select("device_id").distinct().count())
    )
    println("Mean device ids per day:")
    joint.groupBy("day").count().show()
    joint
      .select("url", "device_id")
      //.distinct()
      .groupBy("device_id")
      .count()
      .write
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/device_id_sessions")
    joint
      .select("day", "device_id")
      .distinct()
      .groupBy("day")
      .count()
      .write
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/device_id_days")
  }

  def typeMapping(spark: SparkSession) = {
    val typeMap = Map(
      "coo" -> "web",
      "and" -> "android",
      "ios" -> "ios",
      "con" -> "TV",
      "dra" -> "drawbridge"
    )
    val mapUDF = udf((dev_type: String) => typeMap(dev_type))

    spark.read
      .format("csv")
      .option("sep", "\t")
      .load("/datascience/audiences/crossdeviced/taxo_gral_joint")
      .filter("_c0 IN ('coo', 'ios', 'and')")
      .withColumn("_c0", mapUDF(col("_c0")))
      .write
      .format("csv")
      .option("sep", "\t")
      .save("/datascience/audiences/crossdeviced/taxo_gral_joint_correct_types")
  }

  /**
    *
    *
    *
    *
    *
    *
    *
    *                NETQUEST  REPORT
    *
    *
    *
    *
    *
    *
    *
    */
  def getNetquestReport(spark: SparkSession) = {
    def processDay(day: String) = {
      println(day)
      val data = spark.read
        .format("csv")
        .option("sep", "\t")
        .option("header", "true")
        .load("/data/eventqueue/%s/*.tsv.gz".format(day))
        .filter("event_type = 'sync' AND id_partner = '31'")
        .select("device_id", "id_partner_user", "country")
        .write
        .format("csv")
        .save(
          "/datascience/custom/netquest_report/day=%s"
            .format(day.replace("/", ""))
        )
      println("Day processed: %s".format(day))
    }

    val format = "yyyy/MM/dd"
    val days =
      (0 until 61).map(n => DateTime.now.minusDays(n + 1).toString(format))
    days.map(processDay(_))
  }

  /**
    *
    *
    *
    *                PITCH DANONE
    *
    *
    *


  def kw_pitch_danone(
      spark: SparkSession,
      nDays: Integer = 1,
      since: Integer = 0
  ) = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=AR".format(day)) //para cada dia de la lista day devuelve el path del día
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //es como if os.exists

    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*) //lee todo de una

    val df_keys = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/content_keys_danone.csv")
    
    val data = df.join(broadcast(df_keys), Seq("content_keys"))
    data
      .drop("country_t")
      .drop("_c0")
      .drop("count")
      .dropDuplicates()
      .groupBy("device_id")
      .agg(collect_list("content_keys").as("kws"))
      .withColumn("device_type", lit("web"))
      .select("device_type", "device_id", "seg_id")
    data.cache()

    val job_name = "pitch_danone"

    val queries = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/queries_danone.csv")
      .select("seg_id", "query")
      .collect()
      .map(r => (r(0).toString, r(1).toString))
    for (t <- queries) {
      data
        .filter(t._2)
        .withColumn("seg_id", lit(t._1))
        .select("device_type", "device_id", "seg_id")
        .write
        .format("csv")
        .option("sep", "\t")
        .mode(SaveMode.Overwrite)
        .save("/datascience/devicer/processed/%s_%s".format(job_name,t._1))
      val os = fs.create(
        new Path("/datascience/ingester/ready/%s_%s".format(job_name,t._1))
      )
      val content =
        """{"filePath":"/datascience/devicer/processed/%s_%s", "priority": 20, "partnerId": 0, "queue":"highload", "jobid": 0, "description":"%s"}"""
          .format(job_name,t._1,job_name)
      println(content)
      os.write(content.getBytes)
      os.close()
    }
  }
  */
  
  /**
    *
    *
    *
    *                PITCH DATA_KEYWORDS
    *
    *
    *
    */

  // es _days porque podría ser por hora (hacer un if para seleccionar esto sino)
  def read_data_kw_days(
      spark: SparkSession,
      nDays: Integer,
      since: Integer) : DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=AR".format(day)) //para cada dia de la lista day devuelve el path del día
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //es como if os.exists

    val df = spark.read
      .option("basePath", path).parquet(hdfs_files: _*)
      .select("content_keys","device_id")
      .na.drop() //lee todo de una

    df
  }


  def get_joint_keys(
      df_keys: DataFrame,
      df_data_keywords: DataFrame) : DataFrame = {

    val df_joint = df_data_keywords.join(broadcast(df_keys), Seq("content_keys"))
    df_joint
      .select("content_keys","device_id")
      .dropDuplicates()
      .groupBy("device_id")
      .agg(collect_list("content_keys").as("kws"))
      //.withColumn("device_type", lit("web")) para empujar
      //.select("device_type", "device_id", "seg_id")
      .select("device_id","kws")
  }
    
  def save_query_results(
      df_queries: DataFrame,
      df_joint: DataFrame,
      job_name: String) = {
  
    df_joint.cache()

    val tuples = df_queries.select("seg_id", "query")
      .collect()
      .map(r => (r(0).toString, r(1).toString))
    for (t <- tuples) {
      df_joint
        .filter(t._2)
        .select("device_id")
        .write
        .format("csv")
        .option("sep", "\t")
        .mode(SaveMode.Overwrite)
        .save("/datascience/devicer/processed/%s_%s".format(job_name,t._1))
    }
  }
  


  /**
  //create df from list of tuples

  // leer values de algun lado

  // Create `Row` from `Seq`
  val row = Row.fromSeq(values)

  // Create `RDD` from `Row`
  val rdd = spark.sparkContext.makeRDD(List(row))

  // Create schema fields
  val fields = List(
    StructField("query", StringType, nullable = false),
    StructField("seg_id", Integerype, nullable = false)
  )

  // Create `DataFrame`
  val dataFrame = spark.createDataFrame(rdd, StructType(fields))

   */


  //main method:
  
  //kw_list: List[String],     pasarle estos params a la funcion para pedidos futuros
  //tuple_list: List[String],

  def get_pitch(
      spark: SparkSession,
      nDays: Integer,
      since: Integer,
      job_name: String) = {
    
    val df_data_keywords = read_data_kw_days(spark = spark,
                                             nDays = nDays,
                                             since = since)
    
    // a get_joint_keys pasarle un df con la columna content_keys,
    // creado a partir de una lista de keywords (levanto la lista de un json)
    //la siguiente linea es temp:  

    val df_keys = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/content_keys_danone.csv")
    
    val df_joint = get_joint_keys(df_keys = df_keys,
                                  df_data_keywords = df_data_keywords)

    //pasarle una lista de tuplas del tipo (query,ID)
    //la siguiente linea es temp:
    
    val df_queries = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/queries_danone.csv")


    save_query_results(df_queries = df_queries,
                       df_joint = df_joint,
                       job_name = job_name)

  }

   

  /**
    *
    *
    *
    *
    *
    *
    *
    *                ISP Directv - Equifax AR
    *
    *
    *
    *
    *
    *
    *
    */
  def get_device_IDS(spark: SparkSession) = {
    //PII TABLE

    var data = spark.read.load("/datascience/pii_matching/pii_tuples/")

    var mails = data
      .filter("ml_sh2 is not null")
      .select(
        "device_id",
        "device_type",
        "country",
        "id_partner",
        "ml_sh2",
        "day"
      )
      .withColumnRenamed("ml_sh2", "pii")
      .withColumn("pii_type", lit("mail"))
    var dnis = data
      .filter("nid_sh2 is not null")
      .select(
        "device_id",
        "device_type",
        "country",
        "id_partner",
        "nid_sh2",
        "day"
      )
      .withColumnRenamed("nid_sh2", "pii")
      .withColumn("pii_type", lit("nid")) 

    val pii_table = mails.unionAll(dnis).withColumnRenamed("pii", "valor_atributo_hash")

    // AUDIENCIA FB
    //flat_tc == 0  sin tarjeta de credito
    val df_aud = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/aud_directv_doc.csv")
      .filter("flag_tc == 0")

    val joint = pii_table.join((df_aud), Seq("valor_atributo_hash"))
    joint.write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/devices_ISP_directtv_13aug")
  }

  def get_ISP_directtv(
      spark: SparkSession,
      nDays: Integer = 1,
      since: Integer = 1
  ) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Segments to consider from cluster 61 (ISPs)
    val segments_cluster_61 =
      """-1,1190,1191,1192,1193,1194,1195,1325,1326,1335,1339,1346,1350,1351,3226,3228"""
        .split(",")
        .map(_.toInt)
        .toSet
    val arrIntersect = udf(
      (all_segments: Seq[Int]) =>
        (all_segments ++ Seq(-1)).filter(s => segments_cluster_61.contains(s))(0)
    )

    // Reading data_audiences_streaming and array intersecting.
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val path = "/datascience/data_audiences_streaming/"
    val hdfs_files = days
      .flatMap(
        day =>
          (0 until 24).map(
            hour =>
              path + "/hour=%s%02d/country=AR/"
                .format(day, hour)
          )
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    //cargamos el df de audiences_streaming y lo filtramos por segmentos
    val df_audiences = spark.read
      .parquet(hdfs_files: _*)
      .select("device_id", "all_segments", "datetime")
      .na
      .drop()
      .withColumn("ISP", arrIntersect(col("all_segments")))
      .filter("ISP > 0")

    //filtering by "horario hogareño" de 19 a 8hs, lunes a sabado (brai) y domingo todo el dia??
    // restarle 3 horas a todos porque es horario UTC 0
    val df_audiences_time = df_audiences
      .withColumn("datetime", to_timestamp(col("datetime")))
      .withColumn("Hour", date_format(col("datetime"), "HH"))
      .filter(
        col("Hour") >= 16 || col("Hour") <= 5 || date_format(
          col("datetime"),
          "EEEE"
        ).isin(List("Sunday"): _*)
      )
      .select("device_id", "ISP")
      .distinct()

    // we load the joint file from fb_audience and PII table
    val audience_fb = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/devices_ISP_directtv_9aug")
      .select("device_id", "valor_atributo_hash")

    val joint = df_audiences_time.join(broadcast(audience_fb), Seq("device_id"))

    joint.write
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/directtv_ISP_9aug_final")
  }

  /**
    *
    *
    *
    *
    *
    *
    *
    *                Telecentro Homes
    *
    *
    *
    *
    *
    *
    *
    */
  def get_ISP_Homes(
      spark: SparkSession,
      nDays: Integer,
      since: Integer = 1
  ) = {

    import spark.implicits._
    import org.apache.hadoop.fs.{FileSystem, Path}
    import org.joda.time.DateTime
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

    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Now we obtain the list of hdfs folders to be read
    val path = "/datascience/data_audiences/"
    val hdfs_files = days
      .map(day => path + "day=%s/country=AR/".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    fs.close()

    //cargamos el df de audiences
    val df_audiences = spark.read.parquet(hdfs_files: _*)

    val daud = df_audiences
      .select("device_id", "segments", "timestamp", "device_type")
      .withColumn(
        "ISP",
        when(array_contains(col("segments"), 1192), "Telecentro")
          .otherwise(
            when(array_contains(col("segments"), 1191), "Fibertel")
              .otherwise(
                when(array_contains(col("segments"), 1190), "Arnet")
                  .otherwise(
                    when(array_contains(col("segments"), 1069), "Speedy")
                      .otherwise(0)
                  )
              )
          )
      )
      .filter("ISP != '0'")

    val country = "argentina"

    //dictionary for timezones
    val timezone = Map("argentina" -> "GMT-3", "mexico" -> "GMT-5")

    //setting timezone depending on country
    spark.conf.set("spark.sql.session.timeZone", timezone(country))

    val daud_time = daud
      .withColumn("Time", to_timestamp(from_unixtime(col("timestamp"))))
      .withColumn("Hour", date_format(col("Time"), "HH"))
      .filter(
        (col("Hour") >= 19 || col("Hour") <= 8) || (date_format(
          col("Time"),
          "EEEE"
        ).isin(List("Saturday", "Sunday"): _*))
      )

    val audience_final = daud_time
      .groupBy("device_type", "device_id", "ISP")
      .agg(count("timestamp") as "home_detections")

    audience_final.write
      .format("csv")
      .option("header", true)
      .option("delimiter", "\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/audiences/crossdeviced/Telecentro_Test_ISP")
  }

  /**
    *
    *
    *
    *
    *
    *                  Audiencia Juli - McDonald's
    *
    *
    *
    *
    */
  def getDataKeywords(
      spark: SparkSession,
      nDays: Int = 30,
      since: Int = 1
  ): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    fs.close()
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)
    df
  }

  def getUsersMcDonalds(spark: SparkSession) {
    val data_keys = getDataKeywords(spark, 30, 1)

    val udfSegments = udf(
      (segments: Seq[String]) => segments.filter(_.contains("as")).mkString(",")
    )

    data_keys
      .filter(
        "country = 'AR' AND (array_contains(segments, 'as_81488') OR array_contains(segments, 'as_81489') OR array_contains(segments, 'as_83788'))"
      )
      .withColumn("segments", udfSegments(col("segments")))
      .select("device_id", "segments")
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("sep", ",")
      .save("/datascience/custom/mcdonalds_users")
  }

  /**
    *
    *
    *
    *
    *
    *                SHARETHIS STATS
    *
    *
    *
    *
    */
  def stStats(spark: SparkSession) = {
    val users_rely = spark.read
      .format("csv")
      .option("sep", "\t")
      .load(
        "/datascience/devicer/processed/US_xd-0_partner-_2019-03-21T04-20-48-364205"
      )
      .select("_c1", "_c2")
      .withColumnRenamed("_c1", "device_id")
      .withColumnRenamed("_c2", "segment")
    val users_st = spark.read
      .format("json")
      .load("/datascience/sharethis/demo/dt=2019012*/*")
      .select("estid", "domain_male")
    val estid_table = spark.read
      .load("/datascience/sharethis/estid_table")
      .select("d17", "device_id")
      .withColumnRenamed("d17", "estid")

    estid_table
      .join(users_st, Seq("estid"))
      .join(users_rely, Seq("device_id"))
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .save("/datascience/custom/sharethis_stats")
  }

  /**
    *
    *
    *
    *
    *
    *          DATA LEO 2 - URLS PARA USUARIOS CON GROUND TRUTH
    *
    *
    *
    *
    */
  def joinURLs(spark: SparkSession) = {
    val df = getDataAudiences(spark)
      .filter("country = 'AR' AND event_type IN ('pv', 'batch')")
      .select("device_id", "url", "timestamp")
    val gt = spark.read
      .format("csv")
      .option("sep", "\t")
      .load("/datascience/devicer/processed/equifax_demo_AR_grouped/*")
      .withColumnRenamed("_c1", "device_id")
      .withColumnRenamed("_c2", "label")
      .select("device_id", "label")

    df.join(gt, Seq("device_id"))
      .distinct()
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("sep", "\t")
      .save("/datascience/custom/urls_gt_ar")
  }

  /**
    *
    *
    *
    *
    *
    *          Para informe de ISP (pedido por Seba, hecho por Julián
    09-04-2019)
    *
    */
  def get_ISP_users(
      spark: SparkSession,
      nDays: Integer,
      since: Integer = 1
  ) = {

    //loading user files with geolocation, added drop duplicates to remove users who are detected in the same location
    // Here we load the data, eliminate the duplicates so that the following computations are faster, and select a subset of the columns
    // Also we generate a new column call 'geocode' that will be used for the join
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Now we obtain the list of hdfs folders to be read
    val path = "/datascience/data_audiences/"
    val hdfs_files = days
      .map(day => path + "day=%s/country=AR/".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    fs.close()

    //cargamos el df de audiences
    val df_audiences = spark.read.parquet(hdfs_files: _*)

    //al df de audiences le añadimos una columna que dice si se conectó en horario laboral o en hogareño
    val geo_hour = df_audiences
      .select("device_id", "third_party", "timestamp", "device_type")
      .withColumn("Time", to_timestamp(from_unixtime(col("timestamp"))))
      .withColumn("Hour", date_format(col("Time"), "HH"))
      .filter(
        !date_format(col("Time"), "EEEE").isin(List("Saturday", "Sunday"): _*)
      )
      .withColumn(
        "Period",
        when((col("Hour") >= 20 || col("Hour") <= 8), "Hogar")
          .otherwise("Trabajo")
      )

    //nos quedamos sólo con los usuarios que tengan algunos de los isp de interés
    val users_isp = geo_hour
      .filter(
        array_contains(col("third_party"), 1192) || array_contains(
          col("third_party"),
          1191
        ) || array_contains(col("third_party"), 1193) || array_contains(
          col("third_party"),
          1190
        ) || array_contains(col("third_party"), 1194) || array_contains(
          col("third_party"),
          1069
        ) || array_contains(col("third_party"), 1195)
      )

    //a esos usuarios les contamos cuántas veces aparecen y...lo dividimos por cuatro por cada mes
    //ña variable esa "frequencer" lo que hace es regular según la cantidad de días que se elijan:
    //si se eligió 30, queda en 4 y nos da la cantidad por semana (dividimos al mes por 4)

    val frequencer = (4 * nDays) / 30
    val user_frequency = users_isp
      .groupBy("device_id")
      .count()
      .withColumn("Freq_connections", col("count") / frequencer)
    //.filter("Freq>4") sacamos el filtro para perder menos usuarios. Después podemos fitrarlo luego

    //joineamos con los que tienen la info de ISP
    val high_freq_isp = user_frequency.join(users_isp, Seq("device_id"))

    /*
    high_freq_isp.distinct()
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "false")
      .format("csv")
      .option("sep", ",")
      .save("/datascience/geo/AR/high_freq_isp_30D")
     */

    //ahora levantamos el resultado del crossdevice, estas son solo cookies.
    val user_location = spark.read
      .csv(
        "/datascience/audiences/crossdeviced/zona_norte_users_15-04.csv_xd/"
      )
      .withColumn("device_id", upper(col("_c1")))

    //hacemos el join entre ambos
    val isp_location = high_freq_isp
      .join(user_location, Seq("device_id"))
      .withColumn("third_party", concat_ws(",", col("third_party")))
//fin de cookies

//ahora levantamos las madid de los usuarios, estas son solo madid.
    val user_location_madid = spark.read
      .option("header", true)
      .csv("/datascience/geo/AR/zona_norte_users_15-04.csv")
      .withColumn("device_id", upper(col("user")))

    //hacemos el join entre ambos
    val isp_location_madid = high_freq_isp
      .join(user_location_madid, Seq("device_id"))
      .withColumn("third_party", concat_ws(",", col("third_party")))

//fin de madid

    isp_location
      .distinct()
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "false")
      .format("csv")
      .option("sep", ",")
      .save("/datascience/geo/AR/high_freq_isp_cookie_90D")

    isp_location_madid
      .distinct()
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "false")
      .format("csv")
      .option("sep", ",")
      .save("/datascience/geo/AR/high_freq_isp_madid_90D")

  }

  /**
    *
    *
    *
    *
    *
    *          Para informe de Votantes (pedido por Seba, hecho por Julián
    23-05-2019)
    *
    */
  def get_voto_users(
      spark: SparkSession,
      nDays: Integer,
      since: Integer = 1
  ) = {

    //loading user files with geolocation, added drop duplicates to remove users who are detected in the same location
    // Here we load the data, eliminate the duplicates so that the following computations are faster, and select a subset of the columns
    // Also we generate a new column call 'geocode' that will be used for the join

    //comento acá, había corrido bien esta parte pero no la de levantar y guardar la data
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Now we obtain the list of hdfs folders to be read
    val path = "/datascience/data_audiences/"
    val hdfs_files = days
      .map(day => path + "day=%s/country=AR/".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    //cargamos el df de audiences
    //filtramos por los segmentos de income de equifax
    //filtramos para que la url no sea nula
    val data_audience = spark.read
      .parquet(hdfs_files: _*)
      .filter(
        "array_contains(third_party,'20107') OR array_contains(third_party,'20108') OR array_contains(third_party,'20109') OR array_contains(third_party,'20110')"
      )
      .filter(col("url").isNotNull)
      .select("device_id")
      .distinct()
      .withColumn("device_id", upper(col("device_id")))

    //Cargamos la audiencia de voto
    val voto_audience = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .load(
        "/datascience/devicer/processed/AR_112568000000_2019-07-10T15-05-23-694787_grouped"
      )
      .toDF("device_type", "device_id", "crm_audience")
      .select("device_id", "crm_audience")
      .withColumn("device_id", upper(col("device_id")))
      .distinct()

    //hacemos el join
    val voto_url = voto_audience
      .join(data_audience, Seq("device_id"))
    // .distinct()
    // .groupBy("device_id")
    // .agg(count(col("url")) as "url_count")

//guardamos
    voto_url.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .format("csv")
      .option("sep", "\t")
      .save("/datascience/geo/audiences/voto_url_30d_10_07_19")

//levantamos el resultado del join en random y contamos los usuarios

//esto va a haber que comentarlo para reusarlo entero...
//val conf = spark.sparkContext.hadoopConfiguration
//val fs = FileSystem.get(conf)
//....hasta acá

    val user_count = spark.read
      .format("csv")
      .option("header", true)
      .load("/datascience/geo/audiences/voto_url_180_24-05/")
      .filter(col("url").isNotNull)
      .select("device_id")
      .distinct()
      .count()

    val url_by_user = spark.read
      .format("csv")
      .option("header", true)
      .load("/datascience/geo/audiences/voto_url_180_24-05/")
      .filter(col("url").isNotNull)
      .groupBy("device_id")
      .agg(countDistinct("url") as "detections")

    val user_count_plus_10 = url_by_user.filter("detections > 10").count()

    val user_avg = url_by_user
      .agg(avg("detections") as "average")
      .select("average")
      .collect()(0)
      .toString

//guardamos las metricas
    conf.set("fs.defaultFS", "hdfs://rely-hdfs")
    val os = fs.create(
      new Path("/datascience/geo/audiences/voto_url_180_24-05_metrics.log")
    )

    val json_content =
      """{"user_w_url": "%s", "user_count_plus10": "%s", "user_avg":"%s" }"""
        .format(user_count, user_count_plus_10, user_avg)

    os.write(json_content.getBytes)
    fs.close()

  }

  /**
    *
    *
    *
    *
    *
    *          DATA JULI - ALL-SEGMENTS PARA AUDIENCIA GEO
    *
    *
    *
    *
    */
  def joinGeo(spark: SparkSession) = {
    val df = getDataAudiences(spark, 5, 18)
      .filter("country = 'AR'")
      .select("device_id", "all_segments")
      .withColumn("all_segments", concat_ws(",", col("all_segments")))
      .withColumn("device_id", upper(col("device_id")))
    val geo = spark.read
      .format("csv")
      .load("/datascience/geo/AR/ar_home_90_13-03-19_USERS.csv")
      .withColumnRenamed("_c0", "device_id")
      .withColumn("device_id", upper(col("device_id")))

    df.join(geo, Seq("device_id"))
      .distinct()
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("sep", "\t")
      .save("/datascience/custom/geo_equifax_ar")
  }

  /**
    *
    *
    *
    *                  DATASET FOR EXPANSION MX
    *
    *
    *
    *
    *

  def getExpansionDataset(spark: SparkSession) {
    // val ga = spark.read
    //   .load(
    //     "/datascience/data_demo/join_google_analytics/country=MX/"
    //   )
    //   .dropDuplicates("url", "device_id")
    // val users =
    //   ga.groupBy("device_id").count().filter("count >= 2").select("device_id")

    // users.cache()
    // users.write
    //   .format("csv")
    //   .mode(SaveMode.Overwrite)
    //   .save("/datascience/data_demo/users_to_expand_ga_MX")

    // // val users = spark.read.format("csv").load("/datascience/data_demo/users_to_expand_ga_MX").withColumnRenamed("_c0", "device_id")
    // ga.join(users, Seq("device_id"))
    //   .write
    //   .format("csv")
    //   .mode(SaveMode.Overwrite)
    //   .save("/datascience/data_demo/expand_ga_dataset")

    val segments =
      """26,32,36,59,61,82,85,92,104,118,129,131,141,144,145,147,149,150,152,154,155,158,160,165,166,177,178,210,213,218,224,225,226,230,245,
      247,250,264,265,270,275,276,302,305,311,313,314,315,316,317,318,322,323,325,326,352,353,354,356,357,358,359,363,366,367,374,377,378,379,380,384,385,
      386,389,395,396,397,398,399,401,402,403,404,405,409,410,411,412,413,418,420,421,422,429,430,432,433,434,440,441,446,447,450,451,453,454,456,457,458,
      459,460,462,463,464,465,467,895,898,899,909,912,914,915,916,917,919,920,922,923,928,929,930,931,932,933,934,935,937,938,939,940,942,947,948,949,950,
      951,952,953,955,956,957,1005,1116,1159,1160,1166,2064,2623,2635,2636,2660,2719,2720,2721,2722,2723,2724,2725,2726,2727,2733,2734,2735,2736,2737,2743,
      3010,3011,3012,3013,3014,3015,3016,3017,3018,3019,3020,3021,3022,3023,3024,3025,3026,3027,3028,3029,3030,3031,3032,3033,3034,3035,3036,3037,3038,3039,
      3040,3041,3042,3043,3044,3045,3046,3047,3048,3049,3050,3051,3055,3076,3077,3084,3085,3086,3087,3302,3303,3308,3309,3310,3388,3389,3418,3420,3421,3422,
      3423,3450,3470,3472,3473,3564,3565,3566,3567,3568,3569,3570,3571,3572,3573,3574,3575,3576,3577,3578,3579,3580,3581,3582,3583,3584,3585,3586,3587,3588,
      3589,3590,3591,3592,3593,3594,3595,3596,3597,3598,3599,3600,3730,3731,3732,3733,3779,3782,3843,3844,3913,3914,3915,4097,
      5025,5310,5311""".replace("\n", "").split(",").toList.toSeq

    val triplets =
      spark.read
        .load("/datascience/data_demo/triplets_segments/country=MX/")
        .filter(col("feature").isin(segments: _*))

    triplets
      .join(users, Seq("device_id"))
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_demo/expand_triplets_dataset")

    // val myUDF = udf(
    //   (weekday: String, hour: String) =>
    //     if (weekday == "Sunday" || weekday == "Saturday") "%s1".format(hour)
    //     else "%s0".format(hour)
    // )
    // val data =
    //   spark.read.load("/datascience/data_demo/join_google_analytics/country=MX")
    // data
    //   .withColumn("Time", to_timestamp(from_unixtime(col("timestamp"))))
    //   .withColumn("Hour", date_format(col("Time"), "HH"))
    //   .withColumn("Weekday", date_format(col("Time"), "EEEE"))
    //   .withColumn("wd", myUDF(col("Weekday"), col("Hour")))
    //   .groupBy("device_id", "wd")
    //   .count()
    //   .groupBy("device_id")
    //   .pivot("wd")
    //   .agg(sum("count"))
    //   .write
    //   .format("csv")
    //   .option("header", "true")
    //   .mode(SaveMode.Overwrite)
    //   .save("/datascience/data_demo/expand_ga_timestamp")
  }  */
  /**
    *
    *
    *
    *
    *
    *          PEDIDO DUNNHUMBY
    *
    *
    *
    *
    *
    */
  def getDunnhumbyMatching(spark: SparkSession) = {
    val pii_table = spark.read
      .load("/datascience/pii_matching/pii_table")
      .withColumn("pii", lower(col("pii")))
    pii_table.cache()

    // EXITO CO FILE
    val exito_co_ml = spark.read
      .format("csv")
      .option("sep", ",")
      .option("header", "true")
      .load("/datascience/custom/Exito-CO.csv")
      .select("distinct_correo")
      .withColumnRenamed("distinct_correo", "pii")
      .withColumn("pii", lower(col("pii")))
      .distinct()

    pii_table
      .join(exito_co_ml, Seq("pii"))
      .select("device_id", "pii", "country")
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .save("/datascience/custom/dunnhumby/exito_co_ml")

    val exito_co_nid = spark.read
      .format("csv")
      .option("sep", ",")
      .option("header", "true")
      .load("/datascience/custom/Exito-CO.csv")
      .select("distinct_documento")
      .withColumnRenamed("distinct_documento", "pii")
      .withColumn("pii", lower(col("pii")))
      .distinct()

    pii_table
      .join(exito_co_nid, Seq("pii"))
      .select("device_id", "pii", "country")
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .save("/datascience/custom/dunnhumby/exito_co_nid")

    // GPA BR
    val gpa_br_ml = spark.read
      .format("csv")
      .option("sep", ",")
      .option("header", "true")
      .load("/datascience/custom/GPA-BR.csv")
      .select("DS_EMAIL_LOWER")
      .withColumnRenamed("DS_EMAIL_LOWER", "pii")
      .withColumn("pii", lower(col("pii")))
      .distinct()

    pii_table
      .join(gpa_br_ml, Seq("pii"))
      .select("device_id", "pii", "country")
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .save("/datascience/custom/dunnhumby/gpa_br_ml")

    val gpa_br_nid = spark.read
      .format("csv")
      .option("sep", ",")
      .option("header", "true")
      .load("/datascience/custom/GPA-BR.csv")
      .select("NR_CPF")
      .withColumnRenamed("NR_CPF", "pii")
      .withColumn("pii", lower(col("pii")))
      .distinct()

    pii_table
      .join(gpa_br_nid, Seq("pii"))
      .select("device_id", "pii", "country")
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .save("/datascience/custom/dunnhumby/gpa_br_nid")

    // RD BR
    val rd_br_ml = spark.read
      .format("csv")
      .option("sep", ",")
      .option("header", "true")
      .load("/datascience/custom/RD-BR.csv")
      .select("ds_email_lower")
      .withColumnRenamed("ds_email_lower", "pii")
      .withColumn("pii", lower(col("pii")))
      .distinct()

    pii_table
      .join(rd_br_ml, Seq("pii"))
      .select("device_id", "pii", "country")
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .save("/datascience/custom/dunnhumby/rd_br_ml")

    val rd_br_nid = spark.read
      .format("csv")
      .option("sep", ",")
      .option("header", "true")
      .load("/datascience/custom/GPA-BR.csv")
      .select("nr_cpf")
      .withColumnRenamed("nr_cpf", "pii")
      .withColumn("pii", lower(col("pii")))
      .distinct()

    pii_table
      .join(rd_br_nid, Seq("pii"))
      .select("device_id", "pii", "country")
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .save("/datascience/custom/dunnhumby/rd_br_nid")
  }

  /**
    *
    *
    *
    * Segments for User Agent
    *
    *
    *
    */
  def get_celulares_segments(
      spark: SparkSession,
      nDays: Integer,
      since: Integer = 1
  ) = {

    // Ahora levantamos los datos que estan en datascience keywords

    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=AR".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val segments = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    // Importamos implicits para que funcione el as[String]

    import spark.implicits._

    //cargamos la data de los usuarios XD. Sólo nos quedamos con los códigos y el device_id
    val pois = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .csv("/datascience/audiences/crossdeviced/telefonica_raw.csv")
      .select("Retargetly ID")
      .distinct()
      .withColumnRenamed("Retargetly ID", "device_id")
    //.withColumnRenamed("_c1", "Codigo")

    //hacemos el join
    val joint = pois.join(segments, Seq("device_id")) //.withColumn("segments", explode(col("segments")))

    //explotamos
    val exploded = joint
      .withColumn("segments", explode(col("segments")))
      .select("device_id", "segments")

    //reemplazamos para filtrar
    // val filtered = exploded
    //   .withColumn("segments", regexp_replace(col("segments"), "s_", ""))
    //  .withColumn("segments", regexp_replace(col("segments"), "as_", ""))

    // val taxo_general = spark.read
    //  .format("csv")
    // .option("sep", ",")
    //.option("header", "True")
    // .load("/datascience/data_publicis/taxonomy_publicis.csv")

    // val taxo_segments = taxo_general.select("Segment Id").as[String].collect()

//.filter(col("segments").isin(taxo_segments: _*))
    exploded.write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .save("/datascience/geo/AR/celulares_argentina_sample-14-05")

  }

  /**
    *
    *
    *
    *
    *              MEDIABRANDS - CALCULO DE OVERLAP
    *
    *
    *
    *
    */
  def getOverlap(spark: SparkSession) = {
    val data = spark.read
      .format("csv")
      .option("sep", "\t")
      .load("/datascience/devicer/processed/MX_-5_2019-04-11T00-52-32-149564")
      .select("_c1", "_c2")
      .withColumnRenamed("_c1", "device_id")
      .withColumnRenamed("_c2", "segment")

    val myUDF = udf((segments: Seq[String]) => segments.sorted.mkString(","))

    val grouped = data
      .groupBy("device_id")
      .agg(collect_list(col("segment")).as("segments"))
      .withColumn("segments", myUDF(col("segments")))

    grouped.cache()

    grouped.write
      .format("csv")
      .option("sep", "\t")
      .save("/datascience/custom/overlap_mediabrands")

    grouped
      .groupBy("segments")
      .count()
      .write
      .format("csv")
      .option("sep", "\t")
      .save("/datascience/custom/overlap_mediabrands_count")
  }

  /**
    *
    *
    *
    *
    *
    *                PRESENTACION GCBA
    *
    *
    *
    *
    *
    */
  def getGCBAReport(spark: SparkSession) {
    val group_keywords: Map[String, List[String]] = Map(
      "Inflacion" -> "inflacion devaluacion suba,precios aumentos ganancias invertir"
        .split(" ")
        .toList,
      "Desempleo" -> "desempleo busqueda,empleo trabajo falta,empleo cae,empleo"
        .split(" ")
        .toList,
      "Inseguridad" -> "inseguridad robo asalto secuestro motochorros detuvieron sospechoso ladron violacion violador preso"
        .split(" ")
        .toList,
      "Cultura" -> "cultura musica pintura teatro taller,arte esculturas"
        .split(" ")
        .toList,
      "Transporte" -> "transporte metrobus subte colectivos trenes"
        .split(" ")
        .toList,
      "Ambiente" -> "medio-ambiente medioambiente greenpeace bioguia.com/ambiente"
        .split(" ")
        .toList
    )

    val data = getDataAudiences(spark, 60, 1)

    for ((group, keywords) <- group_keywords) {
      println(group)
      val query =
        keywords
          .map(
            key =>
              "lower(url) LIKE '%" + key
                .replace(",", "%' AND lower(url) LIKE '%") + "%'"
          )
          .mkString(" OR ")

      val filtered = data
        .filter(
          "country = 'AR' AND event_type = 'pv' AND (" + query + ") AND NOT lower(url) LIKE '%mapa.buenosaires%' AND NOT lower(url) LIKE '%miba.buenosaires%' AND NOT lower(url) LIKE '%www.buenosaires%transporte%'"
        )
        .withColumn("group", lit(group))
        .select("device_id", "url", "day")
        .groupBy("device_id", "url", "day")
        .count()
      filtered.cache()
      filtered.write
        .format("csv")
        .mode(SaveMode.Overwrite)
        .save("/datascience/custom/reporte_gcba/%s".format(group))
      filtered
        .groupBy("url", "day")
        .agg(sum(col("count")).as("count"))
        .write
        .format("csv")
        .mode(SaveMode.Overwrite)
        .save("/datascience/custom/reporte_gcba/%s_url_count".format(group))
      filtered
        .groupBy("url")
        .agg(sum(col("count")).as("count"))
        .orderBy(desc("count"))
        .write
        .format("csv")
        .mode(SaveMode.Overwrite)
        .save("/datascience/custom/reporte_gcba/%s_top_url".format(group))

      filtered
        .groupBy("day")
        .agg(sum(col("count")).as("count"))
        .write
        .format("csv")
        .mode(SaveMode.Overwrite)
        .save("/datascience/custom/reporte_gcba/%s_count_day".format(group))

      filtered
        .groupBy("device_id")
        .agg(countDistinct(col("day")).as("count"))
        .write
        .format("csv")
        .mode(SaveMode.Overwrite)
        .save("/datascience/custom/reporte_gcba/%s_device_n_days".format(group))

      filtered
        .groupBy("device_id")
        .agg(countDistinct(col("url")).as("count"))
        .write
        .format("csv")
        .mode(SaveMode.Overwrite)
        .save(
          "/datascience/custom/reporte_gcba/%s_device_distinct_urls"
            .format(group)
        )

      filtered
        .groupBy("device_id")
        .agg(sum(col("count")).as("count"))
        .write
        .format("csv")
        .mode(SaveMode.Overwrite)
        .save("/datascience/custom/reporte_gcba/%s_device_n_urls".format(group))
    }
  }

  def getDataTaringa(spark: SparkSession, ndays: Int) {
    val spark =
      SparkSession.builder.appName("Getting data for Taringa").getOrCreate()

    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(0)
    val days = (0 until ndays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_audiences/"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    val filtered = df
      .filter(
        col("url")
          .contains("taringa.net") && not(col("url").contains("api.retargetly"))
      )
      .orderBy("timestamp")
      .select("device_id", "url", "timestamp")
    filtered.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_taringa")
  }

///////////

  /**
    *
    *
    *
    *
    *
    *
    *                    drawbridge_montly
    *
    *
    *
    *
    *
    */
  def getDrawMonthly(spark: SparkSession) {
    val spark =
      SparkSession.builder.appName("Getting data for Taringa").getOrCreate()

    var path_draw_1 = "2019-01-17"

    var myUdf_1a = udf(
      (ids: Seq[String]) => ids.filter(id => id.contains("cookie"))
    )
    var cookie_1 = spark.read
      .format("csv")
      .load("/data/crossdevice/%s/".format(path_draw_1))
      .withColumn("db_id", split(col("_c0"), "\\|"))
      .withColumn("db_id", size(myUdf_1a(col("db_id"))))
      .agg(sum(col("db_id")) as "total")
      .withColumn("id_type", lit("cookie"))

    var myUdf_1b = udf(
      (ids: Seq[String]) => ids.filter(id => id.contains("android_idfa"))
    )
    var android_1 = spark.read
      .format("csv")
      .load("/data/crossdevice/%s/".format(path_draw_1))
      .withColumn("db_id", split(col("_c0"), "\\|"))
      .withColumn("db_id", size(myUdf_1b(col("db_id"))))
      .agg(sum(col("db_id")) as "android")
      .withColumn("id_type", lit("android"))

    var myUdf_1c = udf(
      (ids: Seq[String]) => ids.filter(id => id.contains("ios_idfa"))
    )
    var ios_1 = spark.read
      .format("csv")
      .load("/data/crossdevice/%s/".format(path_draw_1))
      .withColumn("db_id", split(col("_c0"), "\\|"))
      .withColumn("db_id", size(myUdf_1c(col("db_id"))))
      .agg(sum(col("db_id")) as "total")
      .withColumn("id_type", lit("ios"))

    var myUdf_1d = udf(
      (ids: Seq[String]) => ids.filter(id => id.contains("drawbridge_consumer"))
    )
    var drawbridge_1 = spark.read
      .format("csv")
      .load("/data/crossdevice/%s/".format(path_draw_1))
      .withColumn("db_id", split(col("_c0"), "\\|"))
      .withColumn("db_id", size(myUdf_1d(col("db_id"))))
      .agg(sum(col("db_id")) as "total")
      .withColumn("id_type", lit("drawbridge"))

    var dfs_1 = Seq(cookie_1, android_1, ios_1, drawbridge_1)
    var file_2019_01_17_1 =
      dfs_1.reduce(_ union _).withColumn("file", lit("2019-01-17"))
////////////////

    var path_draw_2 = "2019-02-21"

    var myUdf_2a = udf(
      (ids: Seq[String]) => ids.filter(id => id.contains("cookie"))
    )
    var cookie_2 = spark.read
      .format("csv")
      .load("/data/crossdevice/%s/".format(path_draw_2))
      .withColumn("db_id", split(col("_c0"), "\\|"))
      .withColumn("db_id", size(myUdf_2a(col("db_id"))))
      .agg(sum(col("db_id")) as "total")
      .withColumn("id_type", lit("cookie"))

    var myUdf_2b = udf(
      (ids: Seq[String]) => ids.filter(id => id.contains("android_idfa"))
    )
    var android_2 = spark.read
      .format("csv")
      .load("/data/crossdevice/%s/".format(path_draw_2))
      .withColumn("db_id", split(col("_c0"), "\\|"))
      .withColumn("db_id", size(myUdf_2b(col("db_id"))))
      .agg(sum(col("db_id")) as "android")
      .withColumn("id_type", lit("android"))

    var myUdf_2c = udf(
      (ids: Seq[String]) => ids.filter(id => id.contains("ios_idfa"))
    )
    var ios_2 = spark.read
      .format("csv")
      .load("/data/crossdevice/%s/".format(path_draw_2))
      .withColumn("db_id", split(col("_c0"), "\\|"))
      .withColumn("db_id", size(myUdf_2c(col("db_id"))))
      .agg(sum(col("db_id")) as "total")
      .withColumn("id_type", lit("ios"))

    var myUdf_2d = udf(
      (ids: Seq[String]) => ids.filter(id => id.contains("drawbridge_consumer"))
    )
    var drawbridge_2 = spark.read
      .format("csv")
      .load("/data/crossdevice/%s/".format(path_draw_2))
      .withColumn("db_id", split(col("_c0"), "\\|"))
      .withColumn("db_id", size(myUdf_2d(col("db_id"))))
      .agg(sum(col("db_id")) as "total")
      .withColumn("id_type", lit("drawbridge"))

    var dfs_2 = Seq(cookie_2, android_2, ios_2, drawbridge_2)
    var file_2019_02_21_2 =
      dfs_2.reduce(_ union _).withColumn("file", lit("2019-02-21"))
/////////////////

    var path_draw_3 = "2019-04-04"

    var myUdf_3a = udf(
      (ids: Seq[String]) => ids.filter(id => id.contains("cookie"))
    )
    var cookie_3 = spark.read
      .format("csv")
      .load("/data/crossdevice/%s/".format(path_draw_3))
      .withColumn("db_id", split(col("_c0"), "\\|"))
      .withColumn("db_id", size(myUdf_3a(col("db_id"))))
      .agg(sum(col("db_id")) as "total")
      .withColumn("id_type", lit("cookie"))

    var myUdf_3b = udf(
      (ids: Seq[String]) => ids.filter(id => id.contains("android_idfa"))
    )
    var android_3 = spark.read
      .format("csv")
      .load("/data/crossdevice/%s/".format(path_draw_3))
      .withColumn("db_id", split(col("_c0"), "\\|"))
      .withColumn("db_id", size(myUdf_3b(col("db_id"))))
      .agg(sum(col("db_id")) as "android")
      .withColumn("id_type", lit("android"))

    var myUdf_3c = udf(
      (ids: Seq[String]) => ids.filter(id => id.contains("ios_idfa"))
    )
    var ios_3 = spark.read
      .format("csv")
      .load("/data/crossdevice/%s/".format(path_draw_3))
      .withColumn("db_id", split(col("_c0"), "\\|"))
      .withColumn("db_id", size(myUdf_3c(col("db_id"))))
      .agg(sum(col("db_id")) as "total")
      .withColumn("id_type", lit("ios"))

    var myUdf_3d = udf(
      (ids: Seq[String]) => ids.filter(id => id.contains("drawbridge_consumer"))
    )
    var drawbridge_3 = spark.read
      .format("csv")
      .load("/data/crossdevice/%s/".format(path_draw_3))
      .withColumn("db_id", split(col("_c0"), "\\|"))
      .withColumn("db_id", size(myUdf_3d(col("db_id"))))
      .agg(sum(col("db_id")) as "total")
      .withColumn("id_type", lit("drawbridge"))

    var dfs_3 = Seq(cookie_3, android_3, ios_3, drawbridge_3)
    var file_2019_04_04_3 =
      dfs_3.reduce(_ union _).withColumn("file", lit("2019-04-04"))

///////////////
    val dfs_all = Seq(file_2019_01_17_1, file_2019_02_21_2, file_2019_04_04_3)
    val every_month = dfs_all.reduce(_ union _)

    every_month.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/drawbridge_monthly")
  }

  /**
    *
    *
    *
    *
    *
    *
    *                    Metricas Sample Data
    *
    *
    * */
  def sample_metrics_geo_brco(
      spark: SparkSession
  ) = {
    /*
//hardcoded variables
    val nDays = 16
    val since = 9

    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days =
      (0 until nDays.toInt).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val path = "/datascience/geo/samples/startapp/"

    // Now we obtain the list of hdfs folders to be read

    val hdfs_files = days      .map(day => path + "%s/".format(day))      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))      .map(day => day + "*.csv.gz")



 val sample_data = spark.read      .option("header", "true")  .option("delimiter","\t")    .csv(hdfs_files: _*)      .toDF("ad_id", "timestamp", "country", "longitude","latitude","etc")


//usuarios únicos por día
val user_detections = sample_data.withColumn("Day", date_format(col("timestamp"), "d")).groupBy("country","day").agg(countDistinct("ad_id") as "unique_users")

//detecciones por día
val day_detections = sample_data.withColumn("Day", date_format(col("timestamp"), "d")).groupBy("country","day").agg(count("timestamp") as "detections")

//detecciones por usuario por día
val user_granularity = sample_data.withColumn("Day", date_format(col("timestamp"), "d")).groupBy("ad_id","country","day").agg(count("timestamp") as "time_granularity")

//detecciones por usuario por día, promedio
val mean_user_granularity = user_granularity.groupBy("ad_id","country").agg(mean("time_granularity") as "avg_granularity")

//guardamos los dataframes generados para posterior análisis
user_detections.write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("sep", ",")
      .option("header", "true")
      .save("/datascience/geo/samples/metrics/sample_user_detections")

day_detections.write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("sep", ",")
      .option("header", "true")
      .save("/datascience/geo/samples/metrics/sample_day_detections")


user_granularity.write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("sep", ",")
      .option("header", "true")
      .save("/datascience/geo/samples/metrics/sample_user_granularity")

     */

//Estaba mal hecho el cálculo de granularidad promedio, volvemos a correrlo
    val granularity = spark.read
      .format("csv")
      .option("header", true)
      .load("/datascience/geo/samples/metrics/sample_user_granularity")

    val avg_granularity = granularity
      .groupBy("ad_id", "country")
      .agg(avg("time_granularity") as "avg_granularity")

//usuarios por deteccion BR
    val plus2bra = avg_granularity
      .filter("country == 'BR'")
      .filter("avg_granularity > 2")
      .count()
    val plus20bra = avg_granularity
      .filter("country == 'BR'")
      .filter("avg_granularity > 20")
      .count()
    val plus80bra = avg_granularity
      .filter("country == 'BR'")
      .filter("avg_granularity > 80")
      .count()

//usuarios por deteccion CO
    val plus2co = avg_granularity
      .filter("country == 'CO'")
      .filter("avg_granularity > 2")
      .count()
    val plus20co = avg_granularity
      .filter("country == 'CO'")
      .filter("avg_granularity > 20")
      .count()
    val plus80co = avg_granularity
      .filter("country == 'CO'")
      .filter("avg_granularity > 80")
      .count()

//guardamos las metricas
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://rely-hdfs")
    val fs = FileSystem.get(conf)
    val os = fs.create(new Path("/datascience/geo/samples/metrics.log"))

    val json_content =
      """{"plus2bra": "%s", "plus20bra": "%s", "plus80bra":"%s", "plus2co":"%s","plus20co":"%s","plus80co":"%s"}"""
        .format(plus2bra, plus20bra, plus80bra, plus2co, plus20co, plus80co)

    os.write(json_content.getBytes)
    fs.close()

  }

  /**
    *
    *
    *
    *
    *
    *
    *                    USER GEO POR PAIS
    *
    *
    *
    *
    *
    */
  def safegraph_by_country(
      spark: SparkSession
  ) = {

    //hardcoded variables
    val nDays = 40
    val since = 2

    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(since.toInt)
    val days =
      (0 until nDays.toInt).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val path = "/data/geo/safegraph/"

    // Now we obtain the list of hdfs folders to be read

    val hdfs_files = days
      .map(day => path + "%s/".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      .map(day => day + "*.gz")

    val df_safegraph = spark.read
      .option("header", "true")
      .csv(hdfs_files: _*)

    //number of users by country
    val user_country = df_safegraph
      .select(col("ad_id"), col("id_type"), col("country"))
      .groupBy("country", "id_type")
      .agg(countDistinct("ad_id").alias("unique_users"))

    user_country.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/safegraph_user_by_country_01_05")

  }

  /**
    *
    *
    *
    *
    *
    *
    *                    TELECENTRO
    *
    *
    *
    *
    *
    */
  def segments_for_telecentro(
      spark: SparkSession
  ) = {

    val telecentro = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .load(
        "/datascience/devicer/processed/AR_119211921192_2019-07-23T17-41-38-644053"
      )
      .toDF("device_type", "device_id", "segment")
      .drop("segment")

    val segments = spark.read
      .format("parquet")
      .option("delimiter", "\t")
      .load("/datascience/data_demo/triplets_segments/country=AR/")

    val segments_for_telecentro = telecentro.join(segments, Seq("device_id"))

    val druid = spark.read
      .format("csv")
      .option("header", true)
      .load("hdfs://rely-hdfs//datascience/geo/audiences/taxonomy_druid.csv")
      .withColumnRenamed("id", "feature")

    val telecentro_relevant =
      segments_for_telecentro.join(druid, Seq("feature"))

    telecentro_relevant.write
      .format("csv")
      .option("delimiter", "\t")
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .save("/datascience/audiences/crossdeviced/Telecentro_w_relevance")
  }

  def get_pii_AXIOM(spark: SparkSession) {

    val piis_ar = spark.read
      .format("parquet")
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country='AR'")
      .select("device_id", "nid_sh2")
      .filter(col("nid_sh2").isNotNull)
      .dropDuplicates()

    piis_ar.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/misc/_axiom_pii_AR_20190806")

    val piis_br = spark.read
      .format("parquet")
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country='BR'")
      .select("device_id", "nid_sh2")
      .filter(col("nid_sh2").isNotNull)
      .dropDuplicates()

    piis_br.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/misc/_axiom_pii_BR_20190806")

    //tiramos metricas
    println("Argentina")
    println(piis_ar.count())
    println
    println("Brasil")
    println(piis_br.count())

  }

  def get_pii_AR_seba(spark: SparkSession) {

    val piis_ar = spark.read
      .format("parquet")
      .load("/datascience/pii_matching/pii_tuples/day=20190731/")
      .filter("country == 'AR'")
      .select("device_id", "ml_sh2", "mb_sh2", "nid_sh2")
      .filter(
        (col("ml_sh2").isNotNull) or (col("mb_sh2").isNotNull) or (col(
          "nid_sh2"
        ).isNotNull)
      )
      .dropDuplicates()

    piis_ar.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/misc/ar_pii_seba")
    piis_ar.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/misc/ar_pii_seba")
    piis_ar.describe().filter(col("summary") === "count").show()
  }

  /**
    *
    *
    *
    *
    *
    *
    *                   Votantes_PII
    *
    *
    *
    *
    *
    */
  def PII_device_votantes(
      spark: SparkSession
  ) = {

    val pii = spark.read
      .format("parquet")
      .load("/datascience/pii_matching/pii_table/")
      .filter("country == 'AR'")

    val dir_gcba = spark.read
      .format("csv")
      .option("delimiter", ";")
      .option("header", true)
      .load("hdfs://rely-hdfs//datascience/geo/audiences/ids_dir.csv")
      .withColumnRenamed("documento", "pii")

    dir_gcba
      .join(pii, Seq("pii"))
      .write
      .format("csv")
      .option("header", true)
      .option("delimiter", "\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/audiences/ids_dir_pii")

  }

  
def get_untagged(spark: SparkSession,
                 nDays: Integer,
                 since: Integer) = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format)) //lista con formato de format
    val path = "/data/eventqueue"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/%s/".format(day)) //para cada dia de la lista day devuelve el path del día
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //es como if os.exists

    val df = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .option("basePath", path)
      .csv(hdfs_files: _*) 
      .filter("category != 'null' and tagged != '1' and event_type = 'batch'")
      .select("category")
      .dropDuplicates()

    df.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/untagged_categories")

    }

  /**
    *
    *
    *
    *
    *
    *
    *                    JOIN CON DATA DE KEYWORDS
    *
    *
    *
    *
    *
    */
  def join_data_kw(spark: SparkSession) = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(0)
    val days = (0 until 30).map(end.minusDays(_)).map(_.toString(format)) //lista con formato de format
    val path = "/datascience/data_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=PE".format(day)) //para cada dia de la lista day devuelve el path del día
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //es como if os.exists

    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*) //lee todo de una
    val content_keys_PE = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/content_keys_PE_all.csv")
    val joint = df.join(broadcast(content_keys_PE), Seq("content_keys"))
    joint.write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/PE_keys_vol")
  }

  /**
    *
    *
    *
    *
    *
    *
    *                    PEDIDO TELEFONICA
    *
    *
    *
    *
    *
    */
  def sampleTelefonica(spark: SparkSession) = {
    val file = "/data/eventqueue/2019/04/2*" // cada archivo contiene info de 5 minutos de data
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load(file)
    val df_sel = df.select(
      "device_id",
      "segments",
      "user_agent",
      "url",
      "ml_sh2",
      "mb_sh2",
      "nid_sh2",
      "time",
      "share_data",
      "country"
    )
    val df_filtered = df_sel
      .withColumn("segments", split(col("segments"), "\u0001"))
      .filter(
        "array_contains(segments, '1194') and share_data = '1' and country = 'AR'"
      )
      .drop("segments", "share_data", "country")
    df_filtered.write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/sample_telefonica_20+/")
  }

  /**
    *
    *
    *
    *
    *           Descarga de Usuarios con user agent y segmentos equifax.
    *
    *
    *
    *
    *
    */
  def user_segments(spark: SparkSession) {
    import org.apache.spark.sql.expressions.Window

    val data = getDataAudiences(spark, nDays = 2, since = 8)

    //val equi_segment = List(20107, 20108, 20109, 20110, 20117, 20118, 20121, 20122, 20123, 20125, 20126, 35360, 35361, 35362, 35363)

    //Filtro de segmentos equifax
    //val array_equifax_filter = equi_segment
    //  .map(segment => "array_contains(all_segments, '%s')".format(segment))
    //  .mkString(" OR ")

    val data_segments = data
      .filter("country = 'AR' OR country = 'MX' OR country = 'CL'") // AND (%s).format(array_equifax_filter))
      .select("device_id", "all_segments", "timestamp")

    val w = Window.partitionBy(col("device_id")).orderBy(col("timestamp").desc)

    val dfTop = data_segments
      .withColumn("rn", row_number.over(w))
      .where(col("rn") === 1)
      .drop("rn")

    dfTop.write
      .mode(SaveMode.Overwrite)
      .save("/datascience/audiences/output/user_segments_equifax_03_06_temp")
  }

  //*******
  /**
  def user_agent_parsing(spark: SparkSession) {

    //creamos la funcion para parsear el user agent.
    import org.uaparser.scala.Parser
    import org.apache.spark.sql.functions.udf

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val path_UA = "/datascience/user_agents"
    val df = spark.read
      .format("csv")
      .load(path_UA)
      .toDF("device_id", "UserAgent", "day")
      .filter(col("UserAgent").isNotNull)
      .dropDuplicates("device_id")

    val dfParsedUA = df
      .select("device_id", "UserAgent")
      .rdd
      .map(row => (row(0), Parser.default.parse(row(1).toString)))
      .map(
        row =>
          List(
            row._1,
            row._2.device.brand.getOrElse(""),
            row._2.device.model.getOrElse(""),
            row._2.userAgent.family,
            row._2.os.family,
            row._2.os.major.getOrElse(""),
            row._2.os.minor.getOrElse("")
          ).mkString(",")
      )

    //establecemos dónde guardar la data
    val output =
      "/datascience/audiences/output/celulares_user_agent_ua_parsed_temp/"

    //chequeamos si se puede borrar en caso de que exista
    try {
      fs.delete(new org.apache.hadoop.fs.Path(output), true)
    } catch { case _: Throwable => {} }

    //guardamos el dataset
    dfParsedUA.saveAsTextFile(output)
  }
    */
  //*******
  //Part 3. Join
  def ua_segment_join(spark: SparkSession) {

    val dfParsedRecover = spark.read
      .format("parquet")
      //.load("/datascience/data_useragents/day=20190520/country=AR/")
      .load("/datascience/data_useragents/")
      .dropDuplicates("device_id")

    val dfSegmentRecover = spark.read
      .format("parquet")
      .load("/datascience/audiences/output/user_segments_equifax_03_06_temp")

    val final_df = dfSegmentRecover
      .join(dfParsedRecover, Seq("device_id"))
      .withColumn("all_segments", concat_ws(",", col("all_segments")))

    final_df.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save(
        "/datascience/audiences/output/celulares_user_agent_segmentos_03_06_all_country"
      )
  }

  /**
    *
    *
    *
    *
    *           Descarga de User agents.
    *
    *
    *
    *
    *

  def user_agents(spark: SparkSession) {
    def parse_day(day: String) {
      spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", "\t")
        .load("/data/eventqueue/%s/".format(day))
        .select("device_id", "user_agent", "country")
        .filter("country IN ('AR', 'CL', 'CO', 'BR', 'MX', 'US', 'PE')")
        .select("device_id", "user_agent", "country")
        .withColumn("day", day.replace("""/""", ""))
        .dropDuplicates("device_id")
        .write
        .format("parquet")
        .partitionBy("day", "country")
        .mode("append")
        .save(
          "/datascience/data_useragents/"
        )
      println("Day %s processed!".format(day))
    }
    val day = DateTime.now.minusDays(1).toString("yyyy/MM/dd")
    parse_day("AR", day)
  }
    */

    def user_agents_1day(spark: SparkSession) {

    
    def parse_day(day: String) {
      spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", "\t")
        .load("/data/eventqueue/%s/".format(day))
        .select("device_id", "user_agent", "country")
        .filter("country IN ('AR')")
        .select("device_id", "user_agent", "country")
        .withColumn("day", lit(day.replace("""/""", "")))
        .dropDuplicates("device_id")
        .write
        .format("csv")
        .partitionBy("day", "country")
        .mode("append")
        .save(
          "/datascience/misc/data_useragents/20190808AR"//.format(day)
        )
      println("Day %s processed!".format(day))
    }
     val day = "2019/08/08"
    //val day = DateTime.now.minusDays(1).toString("yyyy/MM/dd")
    parse_day(day)
  }
  /**
    *
    *
    *
    *              DESCARGA DE USER AGENT PARA USERS CON EDAD DE EQUIFAX
    *
    *
    *
    *
    */
  def getUserAgentForAgeUsers(spark: SparkSession) {
    val data = spark.read
      .format("csv")
      .option("sep", "\t")
      .load(
        "/datascience/devicer/processed/AR_4_2019-04-11T21-22-46-395560_grouped/"
      )
      .withColumnRenamed("_c1", "device_id")
      .select("device_id", "_c2")
    val userAgents = spark.read
      .format("csv")
      .load("/datascience/user_agents/AR/")
      .withColumnRenamed("_c0", "device_id")
      .withColumnRenamed("_c1", "user_agent")
      .select("device_id", "user_agent")

    data
      .join(userAgents, Seq("device_id"))
      .distinct()
      .write
      .format("csv")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_demo/name=edades/country=AR/userAgents")
  }

  /**
    *
    *
    *
    *
    *           Descarga de User agents.
    *
    *
    *
    *
    *

  def user_agents(spark: SparkSession) {
    def parse_day(day: String) {
      spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", "\t")
        .load("/data/eventqueue/%s/".format(day))
        .select("device_id", "user_agent", "country")
        .filter("country IN ('AR', 'CL', 'CO', 'BR', 'MX', 'US', 'PE')")
        .select("device_id", "user_agent", "country")
        .withColumn("day", day.replace("""/""", ""))
        .dropDuplicates("device_id")
        .write
        .format("parquet")
        .partitionBy("day", "country")
        .mode("append")
        .save(
          "/datascience/data_useragents/"
        )
      println("Day %s processed!".format(day))
    }
    val day = DateTime.now.minusDays(1).toString("yyyy/MM/dd")
    parse_day("AR", day)
  }
    */
  /**
    *
    *
    *
    *              DESCARGA DE USER AGENT PARA USERS CON EDAD DE EQUIFAX
    *
    *
    *
    *
    */
  def getUserAgentForAgeUsers_1(spark: SparkSession) {
    val data = spark.read
      .format("csv")
      .option("sep", "\t")
      .load(
        "/datascience/devicer/processed/AR_4_2019-04-11T21-22-46-395560_grouped/"
      )
      .withColumnRenamed("_c1", "device_id")
      .select("device_id", "_c2")
    val userAgents = spark.read
      .format("csv")
      .load("/datascience/user_agents/AR/")
      .withColumnRenamed("_c0", "device_id")
      .withColumnRenamed("_c1", "user_agent")
      .select("device_id", "user_agent")

    data
      .join(userAgents, Seq("device_id"))
      .distinct()
      .write
      .format("csv")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_demo/name=edades/country=AR/userAgents")
  }

  /**
    *
    *
    *
    *
    *
    *            NARANJA
    *
    *
    *
    *
    *
    *
    */
  def naranjasaves(spark: SparkSession) {

    val data = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", true)
      .load(
        "/datascience/geo/todo_naranja_update-26-06-19_200d_argentina_26-6-2019-13h"
      )

//Retargetly > Custom GEO > Terceros Físicos Buenos Aires Provincia 14-3-2019
//Retargetly > Custom GEO > Locales Naranja
//Retargetly > Custom GEO > Terceros Físicos Ciudad de Córdoba y Rio Cuarto 14-3-2019
//Retargetly > Custom GEO > Terceros Físicos Salta Capital
//Retargetly > Custom GEO > Terceros Físicos San Salvador Jujuy

// 3erofisba
    data
      .select("device_id", "device_type", "audience", "brand")
      .filter("audience == '3erofisba' AND distance < 51 ")
      .select("device_id", "device_type", "audience")
      .distinct()
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/naranja_3erofisba_27-06_less_50")

//3erofiscordoba
    data
      .select("device_id", "device_type", "audience", "brand")
      .filter("audience == '3erofiscordoba' AND distance < 51")
      .select("device_id", "device_type", "audience")
      .distinct()
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/naranja_3erofiscordoba_27-06_less_50")

// naranja
    data
      .select("device_id", "device_type", "audience", "brand")
      .filter("audience == 'naranja' AND distance < 51")
      .select("device_id", "device_type", "audience")
      .distinct()
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/naranja_naranja_27-06_less_50")

//Retargetly > Custom GEO > Terceros Físicos Salta Capital
    data
      .select("device_id", "device_type", "audience", "brand")
      .filter("audience == '3erofissata' AND distance < 51 ")
      .select("device_id", "device_type", "audience")
      .distinct()
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/naranja_3erofissata_27-06_less_50")

//Retargetly > Custom GEO > Terceros Físicos San Salvador Jujuy
    data
      .select("device_id", "device_type", "audience", "brand")
      .filter("audience == '3erofisjujuy' AND distance < 51 ")
      .select("device_id", "device_type", "audience")
      .distinct()
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/naranja_3erofisjujuy_27-06_less_50")

//pagofacil&rapipago
    data
      .select("device_id", "device_type", "audience", "brand")
      .filter("brand == 'pagofacil' OR brand == 'rapipago' AND distance < 51")
      .select("device_id", "device_type", "audience")
      .distinct()
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/naranja_pagofacil&rapipago_27-06_less_50")

  }

  /**
    *
    *
    *
    *
    *
    *            TEST PARQUET
    *
    *
    *
    *
    *
    *
    */
  def testParquet(spark: SparkSession) {
    import spark.implicits._

    val ints =
      "created id_partner id_segment_source share_data tagged click_count conversion_count impression_count activable job_id"
        .split(" ")
        .toSeq
    val doubles =
      "latitude longitude accuracy altitude altaccuracy".split(" ").toSeq
    val array_strings = "tags app_data app_installed".split(" ").toSeq
    val array_ints =
      "segments clusters first_party second_party third_party all_clusters all_segments all_segments_xd gt removed_segments platforms"
        .split(" ")
    val longs = "ip".split(" ").toSeq

    val data = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/data/eventqueue/2019/05/23/21*")

    val withArrayStrings = array_strings.foldLeft(data)(
      (df, c) => df.withColumn(c, split(col(c), "\u0001"))
    )
    val withInts = ints.foldLeft(withArrayStrings)(
      (df, c) => df.withColumn(c, col(c).cast("int"))
    )
    val withDoubles = doubles.foldLeft(withInts)(
      (df, c) => df.withColumn(c, col(c).cast("double"))
    )
    val withLongs = longs.foldLeft(withDoubles)(
      (df, c) => df.withColumn(c, col(c).cast("long"))
    )
    val df = array_ints.foldLeft(withLongs)(
      (df, c) =>
        df.withColumn(c, split(col(c), "\u0001"))
          .withColumn(c, col(c).cast("array<int>"))
    )
    df.coalesce(12)
      .write
      .option("spark.sql.parquet.compression.codec", "gzip")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/testParquetGzip")
  }

  /**
    *
    *
    *
    *                        PIPELINE DATA PARTNER FROM STREAMING
    *
    *
    *
    */
  def process_pipeline_partner(spark: SparkSession) = {
    val nDays = 20
    val since = 10
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    days.map(
      day =>
        spark.read
          .option("basePath", "/datascience/data_audiences_streaming/")
          .load("/datascience/data_audiences_streaming/hour=%s*".format(day))
          .write
          .mode("append")
          .partitionBy("hour", "id_partner")
          .save("/datascience/data_partner_streaming2/")
    )
  }

  /**
    *
    *
    *
    *
    *
    *
    *                      PEDIDO GCBA - RMOBILE
    *
    *
    *
    *
    *
    */
  def gcba_rmobile(spark: SparkSession) = {
    def get_data(url: String): String = {
      var result = ""
      if (url.contains("?")) {
        val params = url.split("\\?")(1).split("&")
        val paramsMap =
          params
            .filter(param => param.contains("="))
            .map(param => param.split("=", -1))
            .map(l => (l(0), l(1)))
            .toMap

        if (paramsMap.contains("r_mobile") && paramsMap("r_mobile").length > 0 && !paramsMap(
              "r_mobile"
            ).contains("]")) {
          result = paramsMap("r_mobile")
        }
      }
      result
    }

    val dataUDF = udf(get_data _, StringType)
    val query =
      "country = 'AR' AND event_type = 'tk' AND id_partner = '688' AND (array_contains(all_segments, '76522') OR array_contains(all_segments, '76536') OR array_contains(all_segments, '76543')) AND url LIKE '%r_mobile=%'"

    val mobile_count = getDataAudiences(spark, 14, 6)
      .filter(query)
      .withColumn("r_mobile", dataUDF(col("url")))
      .select("r_mobile")
      .distinct()
      .count()

    println("LOGGER: %s".format(mobile_count))
  }

  /**
    *
    *
    *
    *
    *        DATA DE ENCUESTAS DE LAS VOTACIONES
    *
    *
    *
    *

  def getDataVotaciones(spark: SparkSession) = {
    val data_audience =
      getDataAudiences(spark, nDays = 30, since = 1)
        .filter(
          "country = 'AR' and event_type IN ('tk', 'batch', 'data', 'pv')"
        )
        .select("device_id", "url", "time", "all_segments")
    val data_votaciones =
      spark.read
        .format("csv")
        .option("sep", "\t")
        .option("header", "true")
        .load("/datascience/custom/approvable_pgp_employed.csv")
        .withColumnRenamed("_c0", "device_id")
    // .withColumnRenamed("_c1", "cluster")

    val joint = data_audience
      .join(broadcast(data_votaciones), Seq("device_id"))
      .withColumn("all_segments", concat_ws(",", col("all_segments")))

    joint.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/amex_con_data_all")
  }

  
  */
  
  /**
    *
    *
    *
    *
    *
    *              SCOPESI ENRICHMENT
    *
    *
    *
    *
    *
    */
  def scopesi_enrichment(spark: SparkSession) = {
    val original = spark.read
      .format("csv")
      .option("sep", "\t")
      .load("/datascience/custom/scopesi_july_original.csv")
    val xd = spark.read
      .format("csv")
      .load("/datascience/audiences/crossdeviced/scopesi_july_original.csv_xd")
    val taxo = "224 165 302 3055 36 61 26 32 230 264".split(" ").toSeq
    val triplets = spark.read
      .load(
        "/datascience/data_demo/triplets_segments/country=AR/"
      )
      .filter(col("feature").isin(taxo: _*))

    val devices = xd
      .select("_c1")
      .unionAll(original)
      .distinct()
      .withColumnRenamed("_c1", "device_id")

    val joint = triplets.join(devices, Seq("device_id"))

    joint.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/scopesi_enrichment_july")
  }

  /**
    *
    *
    *
    *          STREAMING MINUTES TO PROCESS
    *
    *
    *
    *
    */
  def getMinutesToProcess(spark: SparkSession) = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val format = "yyyyMMddHH"
    val end = DateTime.now.minusHours(1)
    val hours = (1 until 24 * 20)
      .map(end.minusHours(_))
      .map(_.toString(format))
      .map("/datascience/data_partner_streaming/hour=%s".format(_))

    val exist =
      hours.map(path => (path, fs.exists(new org.apache.hadoop.fs.Path(path))))

    val missing_files = exist.filter(t => t._2 == false).map(t => t._1)
    val existing_hours = exist.filter(t => t._2).map(t => t._1)
    val should = Seq(0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55)
    println("MISSING FILES")
    missing_files.foreach(println)

    println("")
    println("MISSING MINUTES")
    for (hour <- existing_hours) {
      try {
        val mins = spark.read
          .load(hour)
          .select("time")
          .withColumn("file", split(col("time"), ":"))
          .withColumn("file", col("file").getItem(1))
          .select("file")
          .distinct()
          .collect()
        val r =
          mins
            .map(_(0))
            .map(_.toString.toInt)
            .map(v => v / 5 * 5)
            .toList
            .distinct
        val missing_minutes = should.filter(!r.contains(_))
        val pathToHour = "/data/eventqueue/2019/%s/%s/%s%02d.tsv.gz"
        val date = hour.split("=")(1)
        missing_minutes.foreach(
          m =>
            println(
              pathToHour.format(
                date.slice(4, 6),
                date.slice(6, 8),
                date.slice(8, 10),
                m
              )
            )
        )
      } catch {
        case unknown => println("Exception at %s: %s".format(hour, unknown))
      }
    }
  }

  /**
    *
    *
    *
    *
    *
    *                      PROCESSING MISSING MINUTES
    *
    *
    *
    *
    *
    *
    */
  def processMissingMinutes(spark: SparkSession) = {
    val missingFiles = List(
      "/data/eventqueue/2019/08/04/2050.tsv.gz",
      "/data/eventqueue/2019/08/04/2055.tsv.gz",
      "/data/eventqueue/2019/08/04/2100.tsv.gz",
      "/data/eventqueue/2019/08/04/2105.tsv.gz",
      "/data/eventqueue/2019/08/04/2110.tsv.gz",
      "/data/eventqueue/2019/08/04/2115.tsv.gz",
      "/data/eventqueue/2019/08/04/2120.tsv.gz",
      "/data/eventqueue/2019/08/04/2125.tsv.gz",
      "/data/eventqueue/2019/08/04/2130.tsv.gz",
      "/data/eventqueue/2019/08/04/2135.tsv.gz",
      "/data/eventqueue/2019/08/04/2140.tsv.gz"
    )

    val processType = "batch"
    val partition = "id_partner"
    val parallel = 0
    val from = 1

    // This is the list of all the columns that each CSV file has.
    val all_columns =
      """timestamp,time,user,device_id,device_type,web_id,android_id,ios_id,event_type,data_type,nav_type,
                        version,country,user_country,ip,created,id_partner,id_partner_user,id_segment_source,share_data,
                        segments,clusters,first_party,second_party,third_party,all_clusters,all_segments,all_segments_xd,gt,
                        user_agent,browser,url,secondary_url,referer,sec,tagged,tags,title,category,sub_category,search_keyword,
                        vertical,mb_sh2,mb_sh5,ml_sh2,ml_sh5,nid_sh2,nid_sh5,track_code,track_type,advertiser_id,advertiser_name,
                        campaign_id,campaign_name,placement_id,site_id,click_count,conversion_count,impression_count,app_data,
                        app_installed,app_name,wifi_name,latitude,longitude,accuracy,altitude,altaccuracy,p223,p240,d2,d9,d10,
                        d11,d13,d14,d17,d18,d19,d20,d21,d22,url_subdomain,url_domain,url_path,referer_subdomain,referer_domain,
                        referer_path,d23,removed_segments,activable,platforms,job_id"""
        .replace("\n", "")
        .replace(" ", "")
        .split(",")
        .toList

    // This is the list of selected columns.
    val columns =
      """device_id, id_partner, event_type, device_type, segments, first_party, all_segments, url, referer, 
                    search_keyword, tags, track_code, campaign_name, campaign_id, site_id, time,
                    placement_id, advertiser_name, advertiser_id, app_name, app_installed, 
                    version, country, activable"""
        .replace("\n", "")
        .replace(" ", "")
        .split(",")
        .toList

    // This is the list of countries that will be considered.
    val countries =
      List("AR", "MX", "CL", "CO", "PE", "US", "BR", "UY", "EC", "BO")

    // This is the list of event types that will be considered.
    val event_types = List(
      "tk",
      "pv",
      "data",
      "batch",
      "sync",
      "xp",
      "retroactive",
      "xd",
      "xd_xp"
    )

    // This is the schema that will be used for the set of all columns
    var finalSchema = all_columns.foldLeft(new StructType())(
      (schema, col) => schema.add(col, "string")
    )

    // This is the list of columns that are integers
    val ints =
      "id_partner activable"
        .split(" ")
        .toSeq

    // List of columns that are arrays of strings
    val array_strings = "tags app_installed".split(" ").toSeq

    // List of columns that are arrays of integers
    val array_ints =
      "segments first_party all_segments"
        .split(" ")

    // Current day
    val day = DateTime.now.minusDays(from).toString("yyyy/MM/dd/")
    println(
      "STREAMING LOGGER:\n\tDay: %s\n\tProcessType: %s\n\tPartition"
        .format(day, processType, partition)
    )

    // Here we read the pipeline

    val data = if (processType == "stream") {
      spark.readStream
        .format("csv")
        .option("sep", "\t")
        .option("header", "true")
        // .option("maxFileAge", "0")
        .option("maxFilesPerTrigger", 4) // Maximum number of files to work on per batch
        .schema(finalSchema) // Defining the schema
        .load("/data/eventqueue/%s*.tsv.gz".format(day)) //"/datascience/streaming/")
        .select(columns.head, columns.tail: _*) // Here we select the columns to work with
        // Now we change the type of the column time to timestamp
        .withColumn(
          "datetime",
          to_utc_timestamp(regexp_replace(col("time"), "T", " "), "utc")
        )
        // Calculate the hour
        .withColumn("hour", date_format(col("datetime"), "yyyyMMddHH"))
    } else {
      spark.read
        .option("sep", "\t")
        .option("header", "true")
        .schema(finalSchema) // Defining the schema
        .format("csv")
        .load(missingFiles: _*)
        .select(columns.head, columns.tail: _*) // Here we select the columns to work with
        // Now we change the type of the column time to timestamp
        .withColumn(
          "datetime",
          to_utc_timestamp(regexp_replace(col("time"), "T", " "), "utc")
        )
        // Calculate the hour
        .withColumn("hour", date_format(col("datetime"), "yyyyMMddHH"))
    }

    println("STREAMING LOGGER:\n\tData: %s".format(data))

    // Now we transform the columns that are array of strings
    val withArrayStrings = array_strings.foldLeft(data)(
      (df, c) => df.withColumn(c, split(col(c), "\u0001"))
    )

    // We do the same with the columns that are integers
    val withInts = ints.foldLeft(withArrayStrings)(
      (df, c) => df.withColumn(c, col(c).cast("int"))
    )

    // Finally, we repeat the process with the columns that are array of integers
    val finalDF = array_ints
      .foldLeft(withInts)(
        (df, c) =>
          df.withColumn(c, split(col(c), "\u0001"))
            .withColumn(c, col(c).cast("array<int>"))
      )

    // Here we do the filtering, where we keep the event types previously specified
    val filtered =
      if (partition == "country")
        finalDF
          .filter(
            length(col("device_id")) > 0 && col("event_type")
              .isin(event_types: _*) && col("id_partner")
              .cast(IntegerType) < 5000 && col("country")
              .isin(countries: _*)
          )
      else
        finalDF
          .filter(
            length(col("device_id")) > 0 && col("event_type")
              .isin(event_types: _*) && col("id_partner")
              .cast(IntegerType) < 5000
          )

    println("STREAMING LOGGER:\n\tFinal DF: %s".format(finalDF))

    // In the last step we write the batch that has been read into /datascience/data_audiences_streaming/ or /datascience/data_partner_streaming/
    val outputPath =
      if (partition == "country")
        "/datascience/data_audiences_streaming" + (if (parallel > 0)
                                                     "_%s/".format(parallel)
                                                   else "/")
      else
        "/datascience/data_partner_streaming" + (if (parallel > 0)
                                                   "_%s".format(parallel)
                                                 else "")
    val checkpointLocation =
      if (partition == "country")
        "/datascience/checkpoint_audiences" + (if (parallel > 0)
                                                 "_%s".format(parallel)
                                               else "")
      else
        "/datascience/checkpoint_partner" + (if (parallel > 0)
                                               "_%s".format(parallel)
                                             else "")
    if (processType == "stream") {
      println("STREAMING LOGGER: Storing the streaming")
      finalDF.writeStream
        .outputMode("append")
        .format("parquet")
        .option("checkpointLocation", checkpointLocation)
        .partitionBy("hour", partition)
        .option("path", outputPath)
        // .trigger(ProcessingTime("1260 seconds"))
        .start()
        .awaitTermination()
    } else {
      finalDF.write
        .mode("append")
        .format("parquet")
        .partitionBy("hour", partition)
        .save(outputPath)
    }
  }

  /**
    *
    *
    *
    *                CROSS DEVICE DE AUDIENCIAS DE LA TAXO GENERAL
    *
    *
    *
    */
  def getCrossForFace(spark: SparkSession) = {
    val segments_nacho =
      """131,103973,477,6115,103971,103968,103970,103972,103969,230,92,104615,446,99638,451,250,105331,5295,447,105332,105334,99639,3565,457,105338,456,105333,3572,3597,105337,450,453,3051,454,3578,1160,1159,3571,105336,48465,459,458"""
        .split(",")
        .toSet
    val arrIntersect = udf(
      (segments: Seq[String]) =>
        segments.exists(s => segments_nacho.contains(s))
    )
    spark.read
      .format("csv")
      .option("sep", "\t")
      .load(
        "/datascience/audiences/crossdeviced/taxo_gral_joint/"
      )
      .withColumn("_c2", split(col("_c2"), ","))
      .filter(arrIntersect(col("_c2")))
      .filter("_c0 != 'web'")
      .withColumn("_c2", concat_ws(",", col("_c2")))
      .write
      .format("csv")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/pedidoNachoFace")
  }

  /**
    *
    *
    *
    *                CROSS DEVICE DE AUDIENCIAS DE LA TAXO GENERAL - Guardado de usuarios
    *
    *
    *
    */
  def saveCrossForFace(spark: SparkSession) = {

    val df = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .load("/datascience/custom/pedidoNachoFace")

    df.cache()

    val ids = List(131, 103973, 477, 6115, 103971, 103968, 103970, 103972,
      103969, 230, 92, 104615, 446, 99638, 451, 250, 105331, 5295, 447, 105332,
      105334, 99639, 3565, 457, 105338, 456, 105333, 3572, 3597, 105337, 450,
      453, 3051, 454, 3578, 1160, 1159, 3571, 105336, 48465, 459, 458)

    for (id <- ids)(df
      .withColumn("_c2", split(col("_c2"), ","))
      .filter("array_contains(_c2, '%s')".format(id))
      .select(col("_c1"))
      .write
      .format("csv")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/pedidoNachoFace_%s".format(id)))
  }

  /**
    *
    *
    *
    *                CUSTOM PE cel gama baja
    *
    *
    *
    */
  def ingest_via_kw(spark: SparkSession) = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(0)
    val days = (0 until 30).map(end.minusDays(_)).map(_.toString(format)) //lista con formato de format
    val path = "/datascience/data_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=PE".format(day)) //para cada dia de la lista day devuelve el path del día
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //es como if os.exists

    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*) //lee todo de una
    val content_keys_PE = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/content_keys_PE.csv")
    val query =
      ("((array_contains(kws, 'smartphone') and array_contains(kws, 'dns')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'barnes')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'sky')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'washion')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'leapfrog')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'celmi')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'digijet')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'gionee')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'bird')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'trinity')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'clickn')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'lanix')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'garmin-asus')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'sonyericsson')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'verizon')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'domo')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'celkon')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'zonda')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'ybamzq')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'inew')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'voyo')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'caterpillar')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'cellallure')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'izoom')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'hkc')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'sendtel')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'kocaso')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'digiland')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'micromax')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'x-bo')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'f2')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'ampe')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'g53')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'zeki')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'ginovo')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'infotmic')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'aimko')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'teclast')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'highscreen')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'i-mobile')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'inovacel')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'otium')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'super')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'alldaymall')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'itel')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'int')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'epik')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'craig')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'sharp')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'yes')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'vtelca')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'bouygues')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'starmobile')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'hishimoto')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'infinix')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'sxtd')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'truemove')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'm4')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'cmx')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'orange')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'verykool')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'techpad')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'doppio')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'cube')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'utok')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'elephone')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'yota')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'yezz')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 't-mobile')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'wileyfox')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'tcl')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'vulcan')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'china')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'vodafone')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'bangho')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'at&t')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'skytex')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'swift')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'direkt-tek')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'vivo')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'yuntab')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'woo')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'acteck')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'diyomate')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'nabi')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'beelink')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'avvio')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'hcl')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'zuum')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'solone')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'irulu')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'kurio')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'posh')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'a1cs')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'unitech')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'energy')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'enge')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'infinity')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'venstar')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'nextbook')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'zallars')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'acer')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'ghia')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'cellacom')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'vorago')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'gearbest')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'tabeo')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'coolpad')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'polaroid')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'coby')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'lava')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'doogee')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'keneksi')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'hisense')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'iocean')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'ekt')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'umi')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'c&s')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'haier')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'inco')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'kingsing')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'fuhu')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'viewsonic')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'kyocera')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'ulefone')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'toshiba')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'tiger')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'casio')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'lgg')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'aikun')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'smartisan')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'bluboo')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'trevi')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'alps')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'supersonic'))" +
        " or ((array_contains(kws, 'smartphone') and array_contains(kws, 'ehear')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'intel')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'star')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'zoostorm')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'trio')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'dragon')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'cx')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'siswoo')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'lazer')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'afunta')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'doro')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'k-touch')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'xolo')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'supersonic')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'indigi')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'oneplus')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'trekstor')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'unimax')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'konka')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'tesla')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'unnecto')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'conquest')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'titan')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'tecno')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'zopo')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'docomo')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'connect')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'jinga')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'explay')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'gomobile')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'ematic')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'zeepad')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'iview')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'fly')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'chromo')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'carrefour')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'apex')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'cherry')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'vida')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'karbonn')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'ais')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'cat')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'airis')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'symphony')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'allwinner')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'colorfly')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'admiral')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'tagital')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'hasee')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'zoomtak')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'figo')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'zowee')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'dell')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'ipro')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'thl')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'antel')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'ione')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'timing')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'alpha')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'xgody')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'ib')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'infocus')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'g-five')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'ghost')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'qmobile')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'kolke')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'wintouch')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'alcatel')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'infotouch')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'element')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'evercoss')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'irola')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'jxd')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'zenek')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'ippo')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'eurocase')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'u')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'joinet')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'versus')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'insignia')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'vizio')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'kingo')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'wiko')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'visual')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'aarp')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'vox')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'tmax')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'leagoo')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'actions')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'jiayu')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'eken')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'xpx')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'ainol')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'solotab')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'gigaset')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'tengda')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'sprint')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'xplay')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'gateway')) or ((array_contains(kws, 'smartphone') and array_contains(kws, 'utime')) or ((array_contains(kws, 'celular') and array_contains(kws, 'landvo')) or ((array_contains(kws, 'celular') and array_contains(kws, 'dns')) or ((array_contains(kws, 'celular') and array_contains(kws, 'barnes')) or ((array_contains(kws, 'celular') and array_contains(kws, 'sky')) or ((array_contains(kws, 'celular') and array_contains(kws, 'washion')) or ((array_contains(kws, 'celular') and array_contains(kws, 'leapfrog')) or ((array_contains(kws, 'celular') and array_contains(kws, 'celmi')) or ((array_contains(kws, 'celular') and array_contains(kws, 'digijet')) or ((array_contains(kws, 'celular') and array_contains(kws, 'gionee')) or ((array_contains(kws, 'celular') and array_contains(kws, 'bird')) or ((array_contains(kws, 'celular') and array_contains(kws, 'trinity')) or ((array_contains(kws, 'celular') and array_contains(kws, 'clickn')) or ((array_contains(kws, 'celular') and array_contains(kws, 'lanix')) or ((array_contains(kws, 'celular') and array_contains(kws, 'garmin-asus')) or ((array_contains(kws, 'celular') and array_contains(kws, 'sonyericsson')) or ((array_contains(kws, 'celular') and array_contains(kws, 'verizon')) or ((array_contains(kws, 'celular') and array_contains(kws, 'domo')) or ((array_contains(kws, 'celular') and array_contains(kws, 'celkon')) or ((array_contains(kws, 'celular') and array_contains(kws, 'zonda')) or ((array_contains(kws, 'celular') and array_contains(kws, 'ybamzq')) or ((array_contains(kws, 'celular') and array_contains(kws, 'inew')) or ((array_contains(kws, 'celular') and array_contains(kws, 'voyo')) or ((array_contains(kws, 'celular') and array_contains(kws, 'caterpillar')) or ((array_contains(kws, 'celular') and array_contains(kws, 'cellallure')) or ((array_contains(kws, 'celular') and array_contains(kws, 'izoom')) or ((array_contains(kws, 'celular') and array_contains(kws, 'hkc')) or ((array_contains(kws, 'celular') and array_contains(kws, 'sendtel')) or ((array_contains(kws, 'celular') and array_contains(kws, 'kocaso')) or ((array_contains(kws, 'celular') and array_contains(kws, 'digiland')) or ((array_contains(kws, 'celular') and array_contains(kws, 'micromax')) or ((array_contains(kws, 'celular') and array_contains(kws, 'x-bo')) or ((array_contains(kws, 'celular') and array_contains(kws, 'f2')) or ((array_contains(kws, 'celular') and array_contains(kws, 'ampe')) or ((array_contains(kws, 'celular') and array_contains(kws, 'g53')) or ((array_contains(kws, 'celular') and array_contains(kws, 'zeki')) or ((array_contains(kws, 'celular') and array_contains(kws, 'ginovo')) or ((array_contains(kws, 'celular') and array_contains(kws, 'infotmic'))" +
        " or ((array_contains(kws, 'celular') and array_contains(kws, 'aimko')) or ((array_contains(kws, 'celular') and array_contains(kws, 'teclast')) or ((array_contains(kws, 'celular') and array_contains(kws, 'highscreen')) or ((array_contains(kws, 'celular') and array_contains(kws, 'i-mobile')) or ((array_contains(kws, 'celular') and array_contains(kws, 'inovacel')) or ((array_contains(kws, 'celular') and array_contains(kws, 'otium')) or ((array_contains(kws, 'celular') and array_contains(kws, 'super')) or ((array_contains(kws, 'celular') and array_contains(kws, 'alldaymall')) or ((array_contains(kws, 'celular') and array_contains(kws, 'itel')) or ((array_contains(kws, 'celular') and array_contains(kws, 'int')) or ((array_contains(kws, 'celular') and array_contains(kws, 'epik')) or ((array_contains(kws, 'celular') and array_contains(kws, 'craig')) or ((array_contains(kws, 'celular') and array_contains(kws, 'sharp')) or ((array_contains(kws, 'celular') and array_contains(kws, 'yes')) or ((array_contains(kws, 'celular') and array_contains(kws, 'vtelca')) or ((array_contains(kws, 'celular') and array_contains(kws, 'bouygues')) or ((array_contains(kws, 'celular') and array_contains(kws, 'starmobile')) or ((array_contains(kws, 'celular') and array_contains(kws, 'hishimoto')) or ((array_contains(kws, 'celular') and array_contains(kws, 'infinix')) or ((array_contains(kws, 'celular') and array_contains(kws, 'sxtd')) or ((array_contains(kws, 'celular') and array_contains(kws, 'truemove')) or ((array_contains(kws, 'celular') and array_contains(kws, 'm4')) or ((array_contains(kws, 'celular') and array_contains(kws, 'cmx')) or ((array_contains(kws, 'celular') and array_contains(kws, 'orange')) or ((array_contains(kws, 'celular') and array_contains(kws, 'verykool')) or ((array_contains(kws, 'celular') and array_contains(kws, 'techpad')) or ((array_contains(kws, 'celular') and array_contains(kws, 'doppio')) or ((array_contains(kws, 'celular') and array_contains(kws, 'cube')) or ((array_contains(kws, 'celular') and array_contains(kws, 'utok')) or ((array_contains(kws, 'celular') and array_contains(kws, 'elephone')) or ((array_contains(kws, 'celular') and array_contains(kws, 'yota')) or ((array_contains(kws, 'celular') and array_contains(kws, 'yezz')) or ((array_contains(kws, 'celular') and array_contains(kws, 't-mobile')) or ((array_contains(kws, 'celular') and array_contains(kws, 'wileyfox')) or ((array_contains(kws, 'celular') and array_contains(kws, 'tcl')) or ((array_contains(kws, 'celular') and array_contains(kws, 'vulcan')) or ((array_contains(kws, 'celular') and array_contains(kws, 'china')) or ((array_contains(kws, 'celular') and array_contains(kws, 'vodafone')) or ((array_contains(kws, 'celular') and array_contains(kws, 'bangho')) or ((array_contains(kws, 'celular') and array_contains(kws, 'at&t')) or ((array_contains(kws, 'celular') and array_contains(kws, 'skytex')) or ((array_contains(kws, 'celular') and array_contains(kws, 'swift')) or ((array_contains(kws, 'celular') and array_contains(kws, 'direkt-tek')) or ((array_contains(kws, 'celular') and array_contains(kws, 'vivo')) or ((array_contains(kws, 'celular') and array_contains(kws, 'yuntab')) or ((array_contains(kws, 'celular') and array_contains(kws, 'woo')) or ((array_contains(kws, 'celular') and array_contains(kws, 'acteck')) or ((array_contains(kws, 'celular') and array_contains(kws, 'diyomate')) or ((array_contains(kws, 'celular') and array_contains(kws, 'nabi')) or ((array_contains(kws, 'celular') and array_contains(kws, 'beelink')) or ((array_contains(kws, 'celular') and array_contains(kws, 'avvio')) or ((array_contains(kws, 'celular') and array_contains(kws, 'hcl')) or ((array_contains(kws, 'celular') and array_contains(kws, 'zuum')) or ((array_contains(kws, 'celular') and array_contains(kws, 'solone')) or ((array_contains(kws, 'celular') and array_contains(kws, 'irulu')) or ((array_contains(kws, 'celular') and array_contains(kws, 'kurio')) or ((array_contains(kws, 'celular') and array_contains(kws, 'posh')) or ((array_contains(kws, 'celular') and array_contains(kws, 'a1cs')) or ((array_contains(kws, 'celular') and array_contains(kws, 'unitech')) or ((array_contains(kws, 'celular') and array_contains(kws, 'energy')) or ((array_contains(kws, 'celular') and array_contains(kws, 'enge')) or ((array_contains(kws, 'celular') and array_contains(kws, 'infinity')) or ((array_contains(kws, 'celular') and array_contains(kws, 'venstar')) or ((array_contains(kws, 'celular') and array_contains(kws, 'nextbook')) or ((array_contains(kws, 'celular') and array_contains(kws, 'zallars')) or ((array_contains(kws, 'celular') and array_contains(kws, 'acer')) or ((array_contains(kws, 'celular') and array_contains(kws, 'ghia')) or ((array_contains(kws, 'celular') and array_contains(kws, 'cellacom')) or ((array_contains(kws, 'celular') and array_contains(kws, 'vorago')) or ((array_contains(kws, 'celular') and array_contains(kws, 'gearbest')) or ((array_contains(kws, 'celular') and array_contains(kws, 'tabeo')) or ((array_contains(kws, 'celular') and array_contains(kws, 'coolpad')) or ((array_contains(kws, 'celular') and array_contains(kws, 'polaroid')) or ((array_contains(kws, 'celular') and array_contains(kws, 'coby')) or ((array_contains(kws, 'celular') and array_contains(kws, 'lava')) or ((array_contains(kws, 'celular') and array_contains(kws, 'doogee')) or ((array_contains(kws, 'celular') and array_contains(kws, 'keneksi')) or ((array_contains(kws, 'celular') and array_contains(kws, 'hisense')) or ((array_contains(kws, 'celular') and array_contains(kws, 'iocean')) or ((array_contains(kws, 'celular') and array_contains(kws, 'ekt')) or ((array_contains(kws, 'celular') and array_contains(kws, 'umi')) or ((array_contains(kws, 'celular') and array_contains(kws, 'c&s')) or ((array_contains(kws, 'celular') and array_contains(kws, 'haier')) or ((array_contains(kws, 'celular') and array_contains(kws, 'inco')) or ((array_contains(kws, 'celular') and array_contains(kws, 'kingsing')) or ((array_contains(kws, 'celular') and array_contains(kws, 'fuhu')) or ((array_contains(kws, 'celular') and array_contains(kws, 'viewsonic')) or ((array_contains(kws, 'celular') and array_contains(kws, 'kyocera')) or ((array_contains(kws, 'celular') and array_contains(kws, 'ulefone')) or ((array_contains(kws, 'celular') and array_contains(kws, 'toshiba')) or ((array_contains(kws, 'celular') and array_contains(kws, 'tiger')) or ((array_contains(kws, 'celular') and array_contains(kws, 'casio')) or ((array_contains(kws, 'celular') and array_contains(kws, 'lgg')) or ((array_contains(kws, 'celular') and array_contains(kws, 'aikun')) or ((array_contains(kws, 'celular') and array_contains(kws, 'smartisan')) or ((array_contains(kws, 'celular') and array_contains(kws, 'bluboo')) or ((array_contains(kws, 'celular') and array_contains(kws, 'trevi')) or ((array_contains(kws, 'celular') and array_contains(kws, 'alps')) or ((array_contains(kws, 'celular') and array_contains(kws, 'supersonic')) or ((array_contains(kws, 'celular') and array_contains(kws, 'ehear')) or ((array_contains(kws, 'celular') and array_contains(kws, 'intel')) or ((array_contains(kws, 'celular') and array_contains(kws, 'star')) or ((array_contains(kws, 'celular') and array_contains(kws, 'zoostorm')) or ((array_contains(kws, 'celular') and array_contains(kws, 'trio')) or ((array_contains(kws, 'celular') and array_contains(kws, 'dragon')) or ((array_contains(kws, 'celular') and array_contains(kws, 'cx')) or ((array_contains(kws, 'celular') and array_contains(kws, 'siswoo')) or ((array_contains(kws, 'celular') and array_contains(kws, 'lazer')) or ((array_contains(kws, 'celular') and array_contains(kws, 'afunta')) or ((array_contains(kws, 'celular') and array_contains(kws, 'doro')) or ((array_contains(kws, 'celular') and array_contains(kws, 'k-touch')) or ((array_contains(kws, 'celular') and array_contains(kws, 'xolo')) or ((array_contains(kws, 'celular') and array_contains(kws, 'supersonic')) or ((array_contains(kws, 'celular') and array_contains(kws, 'indigi')) or ((array_contains(kws, 'celular') and array_contains(kws, 'oneplus')) or ((array_contains(kws, 'celular') and array_contains(kws, 'trekstor')) or ((array_contains(kws, 'celular') and array_contains(kws, 'unimax')) or ((array_contains(kws, 'celular') and array_contains(kws, 'konka')) or ((array_contains(kws, 'celular') and array_contains(kws, 'tesla')) or ((array_contains(kws, 'celular') and array_contains(kws, 'unnecto')) or ((array_contains(kws, 'celular') and array_contains(kws, 'conquest')) or ((array_contains(kws, 'celular') and array_contains(kws, 'titan')) or ((array_contains(kws, 'celular') and array_contains(kws, 'tecno')) or ((array_contains(kws, 'celular') and array_contains(kws, 'zopo')) or ((array_contains(kws, 'celular') and array_contains(kws, 'docomo')) or ((array_contains(kws, 'celular') and array_contains(kws, 'connect')) or ((array_contains(kws, 'celular') and array_contains(kws, 'jinga')) or ((array_contains(kws, 'celular') and array_contains(kws, 'explay')) or ((array_contains(kws, 'celular') and array_contains(kws, 'gomobile')) or ((array_contains(kws, 'celular') and array_contains(kws, 'ematic')) or ((array_contains(kws, 'celular') and array_contains(kws, 'zeepad')) or ((array_contains(kws, 'celular') and array_contains(kws, 'iview')) or ((array_contains(kws, 'celular') and array_contains(kws, 'fly')) or ((array_contains(kws, 'celular') and array_contains(kws, 'chromo')) or ((array_contains(kws, 'celular') and array_contains(kws, 'carrefour')) or ((array_contains(kws, 'celular') and array_contains(kws, 'apex')) or ((array_contains(kws, 'celular') and array_contains(kws, 'cherry')) or ((array_contains(kws, 'celular') and array_contains(kws, 'vida')) or ((array_contains(kws, 'celular') and array_contains(kws, 'karbonn')) or ((array_contains(kws, 'celular') and array_contains(kws, 'ais')) or ((array_contains(kws, 'celular') and array_contains(kws, 'cat'))" +
        " or ((array_contains(kws, 'celular') and array_contains(kws, 'airis')) or ((array_contains(kws, 'celular') and array_contains(kws, 'symphony')) or ((array_contains(kws, 'celular') and array_contains(kws, 'allwinner')) or ((array_contains(kws, 'celular') and array_contains(kws, 'colorfly')) or ((array_contains(kws, 'celular') and array_contains(kws, 'admiral')) or ((array_contains(kws, 'celular') and array_contains(kws, 'tagital')) or ((array_contains(kws, 'celular') and array_contains(kws, 'hasee')) or ((array_contains(kws, 'celular') and array_contains(kws, 'zoomtak')) or ((array_contains(kws, 'celular') and array_contains(kws, 'figo')) or ((array_contains(kws, 'celular') and array_contains(kws, 'zowee')) or ((array_contains(kws, 'celular') and array_contains(kws, 'dell')) or ((array_contains(kws, 'celular') and array_contains(kws, 'ipro')) or ((array_contains(kws, 'celular') and array_contains(kws, 'thl')) or ((array_contains(kws, 'celular') and array_contains(kws, 'antel')) or ((array_contains(kws, 'celular') and array_contains(kws, 'ione')) or ((array_contains(kws, 'celular') and array_contains(kws, 'timing')) or ((array_contains(kws, 'celular') and array_contains(kws, 'alpha')) or ((array_contains(kws, 'celular') and array_contains(kws, 'xgody')) or ((array_contains(kws, 'celular') and array_contains(kws, 'ib')) or ((array_contains(kws, 'celular') and array_contains(kws, 'infocus')) or ((array_contains(kws, 'celular') and array_contains(kws, 'g-five')) or ((array_contains(kws, 'celular') and array_contains(kws, 'ghost')) or ((array_contains(kws, 'celular') and array_contains(kws, 'qmobile')) or ((array_contains(kws, 'celular') and array_contains(kws, 'kolke')) or ((array_contains(kws, 'celular') and array_contains(kws, 'wintouch')) or ((array_contains(kws, 'celular') and array_contains(kws, 'alcatel')) or ((array_contains(kws, 'celular') and array_contains(kws, 'infotouch')) or ((array_contains(kws, 'celular') and array_contains(kws, 'element')) or ((array_contains(kws, 'celular') and array_contains(kws, 'evercoss')) or ((array_contains(kws, 'celular') and array_contains(kws, 'irola')) or ((array_contains(kws, 'celular') and array_contains(kws, 'jxd')) or ((array_contains(kws, 'celular') and array_contains(kws, 'zenek')) or ((array_contains(kws, 'celular') and array_contains(kws, 'ippo')) or ((array_contains(kws, 'celular') and array_contains(kws, 'eurocase')) or ((array_contains(kws, 'celular') and array_contains(kws, 'u')) or ((array_contains(kws, 'celular') and array_contains(kws, 'joinet')) or ((array_contains(kws, 'celular') and array_contains(kws, 'versus')) or ((array_contains(kws, 'celular') and array_contains(kws, 'insignia')) or ((array_contains(kws, 'celular') and array_contains(kws, 'vizio')) or ((array_contains(kws, 'celular') and array_contains(kws, 'kingo')) or ((array_contains(kws, 'celular') and array_contains(kws, 'wiko')) or ((array_contains(kws, 'celular') and array_contains(kws, 'visual')) or ((array_contains(kws, 'celular') and array_contains(kws, 'aarp')) or ((array_contains(kws, 'celular') and array_contains(kws, 'vox')) or ((array_contains(kws, 'celular') and array_contains(kws, 'tmax')) or ((array_contains(kws, 'celular') and array_contains(kws, 'leagoo')) or ((array_contains(kws, 'celular') and array_contains(kws, 'actions')) or ((array_contains(kws, 'celular') and array_contains(kws, 'jiayu')) or ((array_contains(kws, 'celular') and array_contains(kws, 'eken')) or ((array_contains(kws, 'celular') and array_contains(kws, 'xpx')) or ((array_contains(kws, 'celular') and array_contains(kws, 'ainol')) or ((array_contains(kws, 'celular') and array_contains(kws, 'solotab')) or ((array_contains(kws, 'celular') and array_contains(kws, 'gigaset')) or ((array_contains(kws, 'celular') and array_contains(kws, 'tengda')) or ((array_contains(kws, 'celular') and array_contains(kws, 'sprint')) or ((array_contains(kws, 'celular') and array_contains(kws, 'xplay')) or ((array_contains(kws, 'celular') and array_contains(kws, 'gateway')) or (array_contains(kws, 'celular') and array_contains(kws, 'utime'))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))")
    val joint = df.join(broadcast(content_keys_PE), Seq("content_keys"))
    joint
      .drop("country_t")
      .drop("_c0")
      .drop("count")
      .dropDuplicates()
      .groupBy("device_id")
      .agg(collect_list("content_keys").as("kws"))
      .withColumn("device_type", lit("web"))
      .filter(query)
      .withColumn("seg_id", lit("104419"))
      .select("device_type", "device_id", "seg_id")
      .write
      .format("csv")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/devicer/processed/PE_104419_users")
    val os = fs.create(
      new Path("/datascience/ingester/ready/PE_104419_users")
    )
    val content =
      """{"filePath":"/datascience/devicer/processed/PE_104419_users", "priority": 20, "partnerId": 0, "queue":"highload", "jobid": 0, "description":"taxo nueva"}"""

    println(content)
    os.write(content.getBytes)
    os.close()

  }

  /**
    *
    *
    *
    *                POPULAR TAXO NUEVA
    *
    *
    *
    */
  def get_volumes_new_taxo(spark: SparkSession) = {

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/vol_taxo_nueva/")
      .drop("country_t")
      .drop("_c0")
      .drop("count")
      .dropDuplicates()

    df.groupBy("device_id")
      .agg(collect_list("content_keys").as("kws"))
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/new_taxo_grouped")
  }

  def get_sample_mx_mediabrands(spark: SparkSession) = {

    val day_user = spark.read
      .format("parquet")
      .load("/datascience/data_audiences/day=20190731/country=MX/")
      .select("device_id", "all_segments")

    val taxo = spark.read
      .format("csv")
      .option("header", true)
      .load("/datascience/geo/RetargetlyTAXOMediaBrands.csv")

    val taxo_list = taxo.select("Segment ID").rdd.map(r => r(0)).collect()

    val array_equifax_filter = taxo_list
      .map(
        segment =>
          "array_contains(all_segments, '%s')"
            .format(segment)
      )
      .mkString(" OR ")

    val selected_users =
      day_user.filter(array_equifax_filter).select("device_id")

    selected_users.write
      .format("csv")
      .option("header", true)
      .option("delimiter", "\t")
      .mode(SaveMode.Overwrite)
      .save(
        "/datascience/audiences/crossdeviced/MX_Mediabrands_Sample_01_08_19"
      )

  }

  def populateTaxoNueva(spark: SparkSession) = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://rely-hdfs")

    val fs = FileSystem.get(conf)

    val data = spark.read
      .load("/datascience/custom/new_taxo_grouped")
      .withColumn("device_type", lit("web"))
    data.cache()
    val queries = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/scala_taxo_new.csv")
      .select("seg_id", "queries")
      .collect()
      .map(r => (r(0).toString, r(1).toString))
    for (t <- queries) {
      data
        .filter(t._2)
        .withColumn("seg_id", lit(t._1))
        .select("device_type", "device_id", "seg_id")
        .write
        .format("csv")
        .option("sep", "\t")
        .mode(SaveMode.Overwrite)
        .save("/datascience/devicer/processed/taxoNueva_%s".format(t._1))
      val os = fs.create(
        new Path("/datascience/ingester/ready/taxoNueva_%s".format(t._1))
      )
      val content =
        """{"filePath":"/datascience/devicer/processed/taxoNueva_%s", "priority": 20, "partnerId": 0, "queue":"highload", "jobid": 0, "description":"taxo nueva"}"""
          .format(t._1)
      println(content)
      os.write(content.getBytes)
      os.close()
    }
  }

  def getDataVotacionesExplotada(spark: SparkSession) = {
    // val data_votaciones = spark.read
    //   .format("csv")
    //   .load(
    //     "/datascience/custom/votaciones_con_data_all/"
    //   )
    //   .select("_c3", "_c0")
    //   .withColumnRenamed("_c3", "segments")
    //   .withColumn("segments", explode(split(col("segments"), ",")))
    //   .withColumn("segments", col("segments").cast("int"))
    //   .filter("segments<1500 and (segments<580 or segments>820)")
    //   .distinct()

    // data_votaciones.write
    //   .format("csv")
    //   .mode(SaveMode.Overwrite)
    //   .save("/datascience/custom/votacion_con_data_segments")
    val getDomain = udf((url: String) => (url + ".").split("\\.", -1)(1))

    val data_votaciones = spark.read
      .format("csv")
      .load(
        "/datascience/custom/votaciones_con_data_all/"
      )
      .withColumn("domain", getDomain(col("_c1")))
    data_votaciones
      .select("_c0", "domain")
      .distinct()
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/votacion_con_data_domains")
  }

  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("Spark devicer").config("spark.sql.files.ignoreCorruptFiles", "true").getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)
    
    get_device_IDS(spark = spark)
     
  }

}