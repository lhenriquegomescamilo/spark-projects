package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}

object GenerateFeaturesUrls {

  /**
    * Este metodo se encarga de generar una lista de triplets de la pinta <device_id, segment, count>
    * utilizando la data ubicada en data_keywords_p. Una vez generado el dataframe se lo guarda en formato
    * parquet dentro de /datascience/data_demo/triplets_segments
    * Los parametros que recibe son:
    *
    * @param spark: Spark session object que sera utilizado para cargar los DataFrames.
    * @param ndays: cantidad de dias que se utilizaran para generar los triplets.
    *
    */
  def get_datasets_gt(spark: SparkSession, ndays: Int, since: Int) {

    val interest_filter =
      "country = 'AR' and ( array_contains(segments, '36') or array_contains(segments, '59') or array_contains(segments, '61') or array_contains(segments, '129') or array_contains(segments, '144') or array_contains(segments, '145') or array_contains(segments, '165') or array_contains(segments, '224') or array_contains(segments, '247') )"

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until ndays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_audiences"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    val df_interest = df
      .filter(interest_filter)
      .select("url")
      .withColumn("label", lit(0))

    val df_intent = df
      .filter("(country = 'AR' and (array_contains(segments, '352')))")
      .select("url")
      .withColumn("label", lit(1))

    df_interest
      .unionAll(df_intent)
      .distinct()
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_urls/gt")

  }

  def get_dataset_timestamps(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      name: String,
      country: String
  ) {
    val myUDF = udf(
      (weekday: String, hour: String) =>
        if (weekday == "Sunday" || weekday == "Saturday") "%s1".format(hour)
        else "%s0".format(hour)
    )

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until ndays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_audiences"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select("url", "timestamp")

    df.withColumn(
        "Time",
        to_timestamp(
          from_unixtime(
            col("timestamp") - (if (country == "AR") 3 else 5) * 3600
          )
        )
      ) // AR time transformation
      .withColumn("Hour", date_format(col("Time"), "HH"))
      .withColumn("Weekday", date_format(col("Time"), "EEEE"))
      .withColumn("wd", myUDF(col("Weekday"), col("Hour")))
      .groupBy("url", "wd")
      .count()
      .groupBy("url")
      .pivot("wd")
      .agg(sum("count"))
      .write
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save(
        "/datascience/data_urls/name=%s/country=%s/features_timestamp"
          .format(name, country)
      )
  }

  def get_dataset_keywords(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      name: String,
      country: String
  ) {

    // Getting data from selected keywords
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

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
      .map(
        day =>
          spark.read
            .format("csv")
            .option("header", "true")
            .load("/datascience/selected_keywords/%s.csv".format(day))
      )

    // Union of all dataframes
    val df = dfs.reduce(_ union _).dropDuplicates()

    // Filtering urls from country
    val df_filtered = df
      .filter("country = '%s'".format(country))
      .select("url_raw", "hits", "kw")
      .withColumnRenamed("url_raw", "url")

    // Saving Features
    df_filtered.write
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save(
        "/datascience/data_urls/name=%s/country=%s/features_keywords"
          .format(name, country)
      )
  }

  def get_dataset_devices(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      name: String,
      country: String
  ) {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until ndays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_audiences"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    df.groupBy("url", "device_type")
      .agg(count("device_id").as("count"))
      .groupBy("url")
      .pivot("device_type")
      .agg(sum("count"))
      .na
      .fill(0)
      .write
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save(
        "/datascience/data_urls/name=%s/country=%s/features_devices"
          .format(name, country)
      )
  }

  def get_dataset_user_agent(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      name: String,
      country: String
  ) {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until ndays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_useragents"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    val triplets_brand = df
      .groupBy("url", "brand")
      .agg(count("device_id").as("count"))
      .withColumnRenamed("brand", "feature")
    val triplets_model = df
      .groupBy("url", "model")
      .agg(count("device_id").as("count"))
      .withColumnRenamed("model", "feature")
    val triplets_browser = df
      .groupBy("url", "browser")
      .agg(count("device_id").as("count"))
      .withColumnRenamed("browser", "feature")
    val triplets_os = df
      .groupBy("url", "os")
      .agg(count("device_id").as("count"))
      .withColumnRenamed("os", "feature")

    val features_ua = triplets_brand
      .union(triplets_model)
      .union(triplets_browser)
      .union(triplets_os)

    features_ua.write
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save(
        "/datascience/data_urls/name=%s/country=%s/features_user_agent"
          .format(name, country)
      )
  }

  def get_datasets_training(spark: SparkSession) {

    val gt = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/data_urls/gt")

    /**
        val features_timestamp = spark.read.format("csv")
                                    .option("header","true")
                                    .option("sep", "\t")
                                    .load("/datascience/data_urls/name=training_AR/country=AR/features_timestamp")

        val features_devices = spark.read.format("csv")
                            .option("header","true")
                            .option("sep", "\t")
                            .load("/datascience/data_urls/name=training_AR/country=AR/features_devices")
**/
    val features_user_agent = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load(
        "/datascience/data_urls/name=training_AR/country=AR/features_user_agent"
      )

    val features_keywords = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load(
        "/datascience/data_urls/name=training_AR/country=AR/features_keywords"
      )

    /**
        gt.join(features_timestamp,Seq("url")).na.fill(0)
                .write
                .format("csv")
                .option("header", "true")
                .option("sep", "\t")
                .mode(SaveMode.Overwrite)
                .save("/datascience/data_urls/name=training_AR/country=AR/dataset_timestamp")

        gt.join(features_devices,Seq("url")).na.fill(0)
                .write
                .format("csv")
                .option("header", "true")
                .option("sep", "\t")
                .mode(SaveMode.Overwrite)
                .save("/datascience/data_urls/name=training_AR/country=AR/dataset_devices")
**/
    gt.join(features_user_agent, Seq("url"))
      .write
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save(
        "/datascience/data_urls/name=training_AR/country=AR/dataset_user_agent"
      )

    gt.join(features_keywords, Seq("url"))
      .write
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save(
        "/datascience/data_urls/name=training_AR/country=AR/dataset_keywords"
      )

  }

/////////////////////////////////////////////////////////////////////////////////////////////////////////

  def getKeywordsByURL(
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
      .withColumnRenamed("_c3", "content_keys")
      .withColumnRenamed("_c4", "scores")
      .withColumn("content_keys", split(col("content_keys"), " "))
      .withColumn(
        "url",
        regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
      )
      .drop("count", "scores")
      .dropDuplicates("url")

    df
  }

  def get_data_urls(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      country: String
  ): DataFrame = {
    /// Configuraciones de spark
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    /// Obtenemos la data de los ultimos ndays
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(since)

    val days =
      (0 until ndays).map(start.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_demo/data_urls/"
    val dfs = days
      .map(day => path + "/day=%s/country=%s".format(day, country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      .map(
        x =>
          spark.read
            .option("basePath", path)
            .parquet(x)
            .withColumn("day", lit(x.split("/").last.slice(4, 13)))
      )

    val urls = dfs
      .reduce((df1, df2) => df1.union(df2))
      .withColumn(
        "url",
        regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
      )

    urls
  }

  def get_url_content(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      country: String,
      replicationFactor: Int = 4
  ) {

    val urls = get_url_gt(spark, ndays, since, country).withColumn(
      "composite_key",
      concat(
        col("url"),
        lit("@"),
        // This last part is a random integer ranging from 0 to replicationFactor
        least(
          floor(rand() * replicationFactor),
          lit(replicationFactor - 1) // just to avoid unlikely edge case
        )
      )
    )

    val URLkeys = (0 until replicationFactor)
      .map(
        i =>
          getKeywordsByURL(spark, 10, since)
            .withColumn("composite_key", concat(col("url"), lit("@"), lit(i)))
      )
      .reduce((df1, df2) => df1.unionAll(df2))
      .drop("url")

    // Hacemos el join entre nuestra data y la data de las urls con keywords.
    val joint = urls
      .join(URLkeys, Seq("composite_key"))
      .drop("composite_key")
      .withColumn("content_keys", explode(col("content_keys")))
      .groupBy("url", "content_keys")
      .count()

    joint.write
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_url_classifier/dataset_keywords")

  }

  def get_url_gt(spark: SparkSession, ndays: Int, since: Int, country: String): DataFrame = {
    val data_urls = get_data_urls(spark, ndays, since, country)
    val segments = List(129, 59, 61, 250, 396, 150, 26, 32, 247, 3013, 3017)

    val filtered = data_urls
      .select("url", "segments")
      .withColumn("segments", explode(col("segments")))
      .filter(
        col("segments")
          .isin(segments: _*)
      )

    filtered.write
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_url_classifier/gt_multi_class")

    filtered
  }

  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder
      .appName("Get Features: Url Classifier")
      .getOrCreate()

    // Parseo de parametros
    val ndays = if (args.length > 0) args(0).toInt else 10
    val since = if (args.length > 1) args(1).toInt else 1
    val country = if (args.length > 2) args(2).toString else ""

    get_url_content(spark, country = country, since = since, ndays = ndays)
  }
}
