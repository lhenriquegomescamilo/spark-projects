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
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
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
import java.security.MessageDigest
import java.util
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64

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
    val path = "/datascience/data_audiences"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

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

    val segments = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    // Importamos implicits para que funcione el as[String]

    import spark.implicits._

    //cargamos la data de los usuarios XD. Sólo nos quedamos con los códigos y el device_id
    val pois = spark.read
      .option("header", false)
      .option("delimiter", ",")
      .csv(
        "hdfs://rely-hdfs/datascience/geo/geo_processed/Lat_Long_Sarmiento_90d_argentina_24-4-2019-9h_aggregated"
      )
      .select("_c0", "_c1")
      .withColumnRenamed("_c0", "device_id")
      .withColumnRenamed("_c1", "Codigo")

    //hacemos el join
    val joint = pois.join(segments, Seq("device_id")) //.withColumn("segments", explode(col("segments")))

    //explotamos
    val exploded = joint.withColumn("segments", explode(col("segments")))

    //reemplazamos para filtrar
    val filtered = exploded
      .withColumn("segments", regexp_replace(col("segments"), "s_", ""))
      .withColumn("segments", regexp_replace(col("segments"), "as_", ""))

    val taxo_general = spark.read
      .format("csv")
      .option("sep", ",")
      .option("header", "True")
      .load("/datascience/data_publicis/taxonomy_publicis.csv")

    val taxo_segments = taxo_general.select("Segment Id").as[String].collect()

    filtered
      .filter(col("segments").isin(taxo_segments: _*))
      .groupBy("Codigo", "segments")
      .count()
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/AR/sarmiento_code_segment_count_filtered_24-04")

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
      .select(col("ad_id"), col("id_type"),col("country"))
      .groupBy("country","id_type")
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
      .filter("array_contains(segments, '1194') and share_data = '1' and country = 'AR'")
      .drop("segments","share_data","country")
    df_filtered.write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/sample_telefonica_20+/")
  }

  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("Run matching estid-device_id").getOrCreate()
    //sampleTelefonica(spark)
    safegraph_by_country(spark)
  }

}
