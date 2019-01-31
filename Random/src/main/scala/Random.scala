package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
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

  def getEstIdsMatching(spark: SparkSession) = {
    val format = "yyyy/MM/dd"
    val start = DateTime.now.minusDays(30)
    val end = DateTime.now.minusDays(15)

    val daysCount = Days.daysBetween(start, end).getDays()
    val days =
      (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

    val dfs = (0 until daysCount)
      .map(start.plusDays(_))
      .map(_.toString(format))
      .map(
        x =>
          spark.read
            .format("csv")
            .option("sep", "\t")
            .option("header", "true")
            .load("/data/eventqueue/%s/*.tsv.gz".format(x))
            .filter(
              "d17 is not null and country = 'US' and event_type = 'sync'"
            )
            .select("d17", "device_id")
            .dropDuplicates()
      )

    val df = dfs.reduce(_ union _)

    df.write
      .format("csv")
      .option("sep", "\t")
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .save("/datascience/matching_estid_2")
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
      .load("/data/crossdevice/tapad/Retargetly_ids_full_*")
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
    val path = "/datascience/data_audiences_p"

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
      .save("/datascience/custom/tapad_pii")
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
    val from = 31
    val start = DateTime.now.minusDays(from)
    val days = (0 until 75).map(start.minusDays(_)).map(_.toString(format))

    def parseDay(day: String) = {
      println("LOGGER: processing day %s".format(day))
      spark.read
        .format("com.databricks.spark.csv")
        .load("/datascience/sharethis/loading/%s*.json".format(day))
        .filter("_c13 = 'san francisco' AND _c8 LIKE '%att%'")
        .select("_c0", "_c1", "_c3", "_c4", "_c5", "_c6", "_c7", "_c8", "_c9")
        .withColumn("_c0", udfEncrypt(col("_c0")))
        .write
        .moded("append") //.mode(SaveMode.Overwrite)
        .format("csv")
        .option("sep", "\t")
        .save("/datascience/sharethis/sample_att")
      println("LOGGER: day %s processed successfully!".format(day))
    }

    days.map(day => parseDay(day))
  }

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("Run matching estid-device_id").getOrCreate()
    // getTapadPerformance(spark)
    // getDBPerformance(spark)
    getSampleATT(spark)
  }

}
