package main.scala
import main.scala.geodevicer.Geodevicer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import scala.util.parsing.json._
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
  length,
  to_timestamp,
  from_unixtime,
  date_format,
  sum
}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{
  StructType,
  StructField,
  StringType,
  IntegerType
}
import org.apache.spark.sql.{Column, Row}
import scala.util.Random.shuffle
import org.apache.spark.sql.expressions.Window

object Coronavirus {

  def get_data_pois(spark: SparkSession) {

    // Path fijos con los pois de los distintos paises
    val files = List(
      ("tablero_25-03-20_5d_argentina_26-3-2020-16h", "AR"),
      ("Critical_Places_MX_30d_mexico_27-3-2020-11h", "MX")
    )

    val path_geo_jsons = "/datascience/geo/geo_json/"

    val timezone = Map(
      "AR" -> "GMT-3",
      "MX" -> "GMT-5",
      "CL" -> "GMT-3",
      "CO" -> "GMT-5",
      "PE" -> "GMT-5"
    )

    // iterate over pois and generate files
    for ((filename, country) <- files) {
      //setting timezone depending on country
      spark.conf.set("spark.sql.session.timeZone", timezone(country))

      // Run geo
      Geodevicer.run_geodevicer(spark, path_geo_jsons + filename + ".json")
      // Process results and save
      spark.read
        .format("csv")
        .option("header", true)
        .option("delimiter", "\t")
        .load("/datascience/geo/raw_output/%s".format(filename))
        .withColumn("Time", to_timestamp(from_unixtime(col("timestamp"))))
        .withColumn("day", date_format(col("Time"), "dd-MM-YY"))
        .groupBy("day", "audience")
        .agg(
          countDistinct("device_id") as "devices",
          count("timestamp") as "detections"
        )
        .orderBy(asc("Day"))
        .withColumn("country", lit(country))
        .write
        .format("parquet")
        .partitionBy("day", "country")
        .mode(SaveMode.Overwrite)
        .save("/datascience/coronavirus/data_pois/")
    }

  }

  def get_coronavirus(spark: SparkSession, country: String, day: String) {

    val raw = spark.read
      .format("parquet")
      .option("basePath", "/datascience/geo/safegraph/")
      .load(
        "/datascience/geo/safegraph/day=%s/country=%s/".format(day, country)
      )
      .withColumnRenamed("ad_id", "device_id")
      .withColumn("device_id", lower(col("device_id")))
      .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
      .withColumn("Hour", date_format(col("Time"), "YYYYMMddHH"))
      .withColumn("window", date_format(col("Time"), "mm"))
      .withColumn(
        "window",
        when(col("window") > 40, 3)
          .otherwise(when(col("window") > 20, 2).otherwise(1))
      )
      .withColumn("window", concat(col("Hour"), col("window")))
      .drop("Time")

    // Select sample of 1000 users
    val initial_seed = spark.read
      .load(
        "/datascience/coronavirus/coronavirus_seed/day=%s/country=%s"
          .format(day, country)
      )
      .select("device_id", "geo_hash", "window")

    // Get the distinct moments to filter the raw data
    val moments = initial_seed.select("geo_hash", "window").distinct()

    // Join raw data with the moments and store
    raw
      .join(moments, Seq("geo_hash", "window"))
      .withColumn("day", lit(day))
      .withColumn("country", lit(country))
      .write
      .format("parquet")
      .partitionBy("day", "country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/coronavirus/coronavirus_contacts/")

    val joint = spark.read.load(
      "/datascience/coronavirus/coronavirus_contacts/day=%s/country=%s"
        .format(day, country)
    )

    // Calculate it by day
    val udfDay = udf((d: String) => d.substring(0, 8))

    joint
      .join(
        initial_seed.withColumnRenamed("device_id", "original_id"),
        Seq("geo_hash", "window")
      )
      .withColumn("day", udfDay(col("window")))
      .groupBy("original_id", "day")
      .agg(collect_set(col("device_id")).as("devices"))
      .withColumn("contacts", size(col("devices")) - 1) // Calculate number of contacts for each device
      .groupBy("day")
      .agg(mean("contacts") as "contacts")
      .withColumn("country", lit(country))
      .write
      .format("parquet")
      .partitionBy("day", "country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/coronavirus/coronavirus_contacts_per_day/")

  }
  def generate_seed(spark: SparkSession, country: String, day: String) {
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(0)

    // Get sample of 10K seed users per day
    val days = (0 until 10).map(start.minusDays(_)).map(_.toString(format))
    //val days = List("20200323","20200322","20200321","20200320","20200319","20200318","20200317","20200316","20200315","20200314","20200313","20200312","20200311","20200310",
    //"20200309","20200308","20200307","20200306","20200305","20200304","20200303","20200302","20200301")
    val path = "/datascience/geo/safegraph/"
    val dfs = days
      .map(day => path + "day=%s/".format(day) + "country=%s".format(country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      .map(
        x =>
          spark.read
            .option("basePath", path)
            .parquet(x)
            .withColumnRenamed("ad_id", "device_id")
            .select("device_id")
            .distinct
            .limit(20000)
      )

    val users =
      dfs.reduce((df1, df2) => df1.union(df2)).select("device_id").distinct

    // Now calculate all the moments for the seed users
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .withColumnRenamed("ad_id", "device_id")
      .withColumn("device_id", lower(col("device_id")))
      .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
      .withColumn("Hour", date_format(col("Time"), "YYYYMMddHH"))
      .withColumn("window", date_format(col("Time"), "mm"))
      .withColumn(
        "window",
        when(col("window") > 40, 3)
          .otherwise(when(col("window") > 20, 2).otherwise(1))
      )
      .withColumn("window", concat(col("Hour"), col("window")))
      .drop("Time")
      .join(users, Seq("device_id"), "inner")
      .select("device_id", "geo_hash", "window")
      .withColumn("day", lit(day))
      .withColumn("country", lit(country))
      .distinct
      .write
      .format("parquet")
      .partitionBy("day", "country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/coronavirus/coronavirus_seed/")

  }

  def coronavirus_barrios(
      spark: SparkSession,
      country: String,
      barrios: DataFrame,
      name: String,
      day: String
  ) {
    val udfGeo = udf((d: String) => d.substring(0, 7))

    val initial_seed = spark.read
      .load(
        "/datascience/coronavirus/coronavirus_seed/day=%s/country=%s"
          .format(day, country)
      )
      .select("device_id", "geo_hash", "window")

    val contacts = spark.read
      .load(
        "/datascience/coronavirus/coronavirus_contacts/day=%s/country=%s"
          .format(day, country)
      )
      .withColumn("geo_hash_join", udfGeo(col("geo_hash")))

    val joint = contacts.join(broadcast(barrios), Seq("geo_hash_join"), "inner")

    // Calculate it by day
    val udfDay = udf((d: String) => d.substring(0, 8))

    joint
      .join(
        initial_seed.withColumnRenamed("device_id", "original_id"),
        Seq("geo_hash", "window")
      )
      .withColumn("day", udfDay(col("window")))
      .groupBy("original_id", "day", name)
      .agg(collect_set(col("device_id")).as("devices"))
      .withColumn("contacts", size(col("devices")) - 1) // Calculate number of contacts for each device
      .groupBy(name)
      .agg(mean("contacts") as "contacts")
      .withColumn("day", lit(day))
      .withColumn("country", lit(country))
      .write
      .format("parquet")
      .partitionBy("day", "country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/coronavirus/coronavirus_contacts_barrios")

  }

  def get_safegraph_data(
      spark: SparkSession,
      since: Int,
      nDays:Int,
      country: String
  ) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays)
      .map(end.minusDays(_))
      .map(_.toString(format))

    //Now we obtain the list of hdfs files to be read
    val path = "/datascience/geo/safegraph/"
    val hdfs_files = days
      .map(day => path +  "day=%s/country=%s/".format(day,country))
      .filter(
        path => fs.exists(new org.apache.hadoop.fs.Path(path))
      )

    // Finally we read, filter by country, rename the columns and return the data
    val df_safegraph = spark.read
                            .option("header", "true")
                            .parquet(hdfs_files: _*)
                            .dropDuplicates("ad_id", "latitude", "longitude")

    df_safegraph
  }

  def distance_traveled_ar(spark: SparkSession, since:Int) {
    val country = "argentina"
    val format = "dd-MM-YY"
    val day = DateTime.now.minusDays(since+2).toString(format)

    val raw = get_safegraph_data(spark,since,4, country)
      .withColumnRenamed("ad_id", "device_id")
      .withColumn("device_id", lower(col("device_id")))
      .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
      .withColumn("Day", date_format(col("Time"), "dd-MM-YY"))
      .drop("Time")
      .withColumn("geo_hash_7", substring(col("geo_hash"), 0, 7))
      .withColumn("day", lit(day))
      .filter(col("day") === col("Day"))

    //Vamos a usarlo para calcular velocidad y distancia al hogar
    raw.persist()

    val geo_hash_visits = raw
      .groupBy("device_id", "day", "geo_hash_7")
      .agg(count("utc_timestamp") as "detections")
      .withColumn("country", lit(country))

    ///val output_file = "/datascience/coronavirus/geohashes_by_user/"

    geo_hash_visits.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .partitionBy("day", "country")
      .save("/datascience/coronavirus/geohashes_by_user")

    //Queremos un cálculo general por país
    val hash_user = spark.read
      .format("parquet")
      .load(
        "/datascience/coronavirus/geohashes_by_user/day=%s/country=%s"
          .format(day, country)
      )
      .withColumn("device_id", lower(col("device_id")))
      .withColumn("day", lit(day))

    hash_user
      .groupBy("day", "device_id")
      .agg(countDistinct("geo_hash_7") as "geo_hash_7")
      .groupBy("day")
      .agg(
        avg("geo_hash_7") as "geo_hash_7_avg",
        stddev_pop("geo_hash_7") as "geo_hash_7_std",
        count("device_id") as "devices"
      )
      .withColumn("country", lit(country))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .partitionBy("day", "country")
      .save("/datascience/coronavirus/geohashes_by_country")

    ///////////Partidos
    //QUeremos un cálculo por municipio:
    //Desagregado por entidad y municipio
    val entidad = spark.read
      .format("csv")
      .option("header", true)
      .option("delimiter", "\t")
      .load(
        "/datascience/geo/geo_processed/AR_departamentos_barrios_mexico_sjoin_polygon"
      )
      .withColumnRenamed("geo_hashote", "geo_hash_7")

    //Acá por provincia
    val output_file_provincia =
      "/datascience/coronavirus/geohashes_by_provincia"
    val provincia = spark.read
      .format("parquet")
      .load(
        "/datascience/coronavirus/geohashes_by_user/day=%s/country=%s".format(
          day,
          country
        )
      )
      .withColumn("day", lit(day))
      .join(entidad, Seq("geo_hash_7"))
      .groupBy("PROVCODE", "PROVINCIA", "day", "device_id")
      .agg(countDistinct("geo_hash_7") as "geo_hash_7")
      .groupBy("PROVCODE", "PROVINCIA", "day")
      .agg(
        count("device_id") as "devices",
        avg("geo_hash_7") as "geo_hash_7_avg",
        stddev_pop("geo_hash_7") as "geo_hash_7_std"
      )
      .withColumn("country", lit(country))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", true)
      .partitionBy("day", "country")
      .save("/datascience/coronavirus/geohashes_by_provincia")

    //Acá por partido
    val output_file_partido = "/datascience/coronavirus/geohashes_by_partido"
    val partido = spark.read
      .format("parquet")
      .load(
        "/datascience/coronavirus/geohashes_by_user/day=%s/country=%s".format(
          day,
          country
        )
      )
      .withColumn("day", lit(day))
      .join(entidad, Seq("geo_hash_7"))
      .groupBy("PROVCODE", "IN1", "PROVINCIA", "NAM", "FNA", "day", "device_id")
      .agg(countDistinct("geo_hash_7") as "geo_hash_7")
      .groupBy("PROVCODE", "IN1", "PROVINCIA", "NAM", "FNA", "day")
      .agg(
        count("device_id") as "devices",
        avg("geo_hash_7") as "geo_hash_7_avg",
        stddev_pop("geo_hash_7") as "geo_hash_7_std"
      )
      .withColumn("country", lit(country))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", true)
      .partitionBy("day", "country")
      .save("/datascience/coronavirus/geohashes_by_partido")

    ///////////////barrios
    //Con esto de abajo calculamos para barrios, por ahroa sólo funciona para Argentina
    val barrios = spark.read
      .format("csv")
      .option("header", true)
      .option("delimiter", ",")
      .load("/datascience/geo/Reports/GCBA/Coronavirus/")
      .withColumnRenamed("geo_hashote", "geo_hash_7")

    //Alternativa 1
    //Path home ARG

    //Nos quedamos con los usuarios de los homes que viven en caba
    val homes = spark.read
      .format("parquet")
      .load("/datascience/data_insights/homes/day=2020-03/country=AR")
    val geocode_barrios = spark.read
      .format("csv")
      .option("header", true)
      .load(
        "/datascience/geo/Reports/GCBA/Coronavirus/Geocode_Barrios_CABA.csv"
      )

    val homes_barrio = homes
      .select("device_id", "GEOID")
      .join(geocode_barrios, Seq("GEOID"))
      .drop("GEOID")

    val output_file_tipo_1 =
      "/datascience/coronavirus/geohash_travel_barrio_CLASE1"

    spark.read
      .format("parquet")
      .load(
        "/datascience/coronavirus/geohashes_by_user/day=%s/country=%s".format(
          day,
          country
        )
      )
      .withColumn("day", lit(day))
      .withColumn("device_id", lower(col("device_id")))
      .groupBy("device_id", "day")
      .agg(countDistinct("geo_hash_7") as "geo_hash_7")
      .join(homes_barrio, Seq("device_id"))
      .groupBy("BARRIO", "day")
      .agg(
        avg("geo_hash_7") as "geo_hash_7_avg",
        stddev_pop("geo_hash_7") as "geo_hash_7_std",
        count("device_id") as "devices"
      )
      .withColumn("country", lit(country))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", true)
      .partitionBy("day", "country")
      .save("/datascience/coronavirus/geohash_travel_barrio_CLASE1")

    //Alternativa 2
    val output_file_tipo_2 =
      "/datascience/coronavirus/geohash_travel_barrio_CLASE2"
    val tipo2 = spark.read
      .format("parquet")
      .load(
        "/datascience/coronavirus/geohashes_by_user/day=%s/country=%s".format(
          day,
          country
        )
      )
      .withColumn("day", lit(day))
      .join(barrios, Seq("geo_hash_7"))
      .groupBy("COMUNA", "BARRIO", "day", "device_id")
      .agg(countDistinct("geo_hash_7") as "geo_hash_7")
      .groupBy("COMUNA", "BARRIO", "day")
      .agg(
        count("device_id") as "devices",
        avg("geo_hash_7") as "geo_hash_7_avg",
        stddev_pop("geo_hash_7") as "geo_hash_7_std"
      )
      .withColumn("country", lit(country))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", true)
      .partitionBy("day", "country")
      .save("/datascience/coronavirus/geohash_travel_barrio_CLASE2")
  }

  def distance_traveled_mx(spark: SparkSession, since: Int) {
    
    val country = "mexico"
    val format = "dd-MM-YY"
    val day = DateTime.now.minusDays(since+2).toString(format)
    println("Working on day %s".format(day))
    val raw = get_safegraph_data(spark,since,4, country)
      .withColumnRenamed("ad_id", "device_id")
      .withColumn("device_id", lower(col("device_id")))
      .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
      .withColumn("Day", date_format(col("Time"), "dd-MM-YY"))
      .drop("Time")
      .withColumn("geo_hash_7", substring(col("geo_hash"), 0, 7))
      .withColumn("day", lit(day))
      .filter(col("day") === col("Day"))

    //Vamos a usarlo para calcular velocidad y distancia al hogar
    raw.persist()

    val geo_hash_visits = raw
      .groupBy("device_id", "day", "geo_hash_7")
      .agg(count("utc_timestamp") as "detections")
      .withColumn("country", lit(country))
      .withColumn("day", lit(day))

    geo_hash_visits.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .option("header", "true")
      .partitionBy("day", "country")
      .save("/datascience/coronavirus/geohashes_by_user")

    //Queremos un cálculo general por país
    val hash_user = spark.read
      .format("parquet")
      .load(
        "/datascience/coronavirus/geohashes_by_user/day=%s/country=%s"
          .format(day, country)
      )
      .withColumn("device_id", lower(col("device_id")))
      .withColumn("day", lit(day))

    hash_user
      .groupBy("day", "device_id")
      .agg(countDistinct("geo_hash_7") as "geo_hash_7")
      .groupBy("day")
      .agg(
        avg("geo_hash_7") as "geo_hash_7_avg",
        stddev_pop("geo_hash_7") as "geo_hash_7_std",
        count("device_id") as "devices"
      )
      .withColumn("country", lit(country))
      .withColumn("day", lit(day))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .partitionBy("day", "country")
      .save("/datascience/coronavirus/geohashes_by_country")

    //Desagregado por entidad y municipio
    val entidad = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .load(
        "/datascience/geo/geo_processed/MX_municipal_Updated_mexico_sjoin_polygon"
      )

    //Acá lo agregamos por estado
    spark.read
      .format("parquet")
      .load(
        "/datascience/coronavirus/geohashes_by_user/day=%s/country=%s".format(
          day,
          country
        )
      )
      .withColumn("day", lit(day))
      .join(entidad, Seq("geo_hash_7"))
      .groupBy("NOM_ENT", "day", "device_id")
      .agg(countDistinct("geo_hash_7") as "geo_hash_7")
      .groupBy("NOM_ENT", "day")
      .agg(
        count("device_id") as "devices",
        avg("geo_hash_7") as "geo_hash_7_avg",
        stddev_pop("geo_hash_7") as "geo_hash_7_std"
      )
      .withColumn("country", lit(country))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .partitionBy("day", "country")
      .save("/datascience/coronavirus/geohashes_by_estado")

    //Acá lo agregamos por municipio
    spark.read
      .format("parquet")
      .load(
        "/datascience/coronavirus/geohashes_by_user/day=%s/country=%s".format(
          day,
          country
        )
      )
      .withColumn("day", lit(day))
      .join(entidad, Seq("geo_hash_7"))
      .groupBy("NOM_ENT", "CVEGEO", "NOM_MUN", "day", "device_id")
      .agg(countDistinct("geo_hash_7") as "geo_hash_7")
      .groupBy("NOM_ENT", "CVEGEO", "NOM_MUN", "day")
      .agg(
        count("device_id") as "devices",
        avg("geo_hash_7") as "geo_hash_7_avg",
        stddev_pop("geo_hash_7") as "geo_hash_7_std"
      )
      .withColumn("country", lit(country))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .partitionBy("day", "country")
      .save("/datascience/coronavirus/geohashes_by_municipio")

  }

  def distance_traveled_rest(
      spark: SparkSession,
      since: Int,
      country: String
  ) {
    val format = "dd-MM-YY"
    val day = DateTime.now.minusDays(since+1).toString(format)

    val raw = get_safegraph_data(spark,since,4, country)
      .withColumnRenamed("ad_id", "device_id")
      .withColumn("device_id", lower(col("device_id")))
      .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
      .withColumn("Day", date_format(col("Time"), "dd-MM-YY"))
      .drop("Time")
      .withColumn("country", lit(country))
      .withColumn(
        "geo_hash_7",
        ((abs(col("latitude").cast("float")) * 1000)
          .cast("long") * 100000) + (abs(
          col("longitude").cast("float") * 1000
        ).cast("long"))
      )
      .withColumn("day", lit(day))
      .filter(col("day") === col("Day"))

    //Vamos a usarlo para calcular velocidad y distancia al hogar
    raw.persist()

    val geo_hash_visits = raw
      .groupBy("device_id", "day", "geo_hash_7")
      .agg(count("utc_timestamp") as "detections")
      .withColumn("country", lit(country))
      .withColumn("day", lit(day))

    geo_hash_visits.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .option("header", "true")
      .partitionBy("day", "country")
      .save("/datascience/coronavirus/geohashes_by_user")

    ///////////Agregación Nivel 0
    //Queremos un cálculo general por país
    val hash_user = spark.read
      .format("parquet")
      .load(
        "/datascience/coronavirus/geohashes_by_user/day=%s/country=%s"
          .format(day, country)
      )
      .withColumn("device_id", lower(col("device_id")))
      .withColumn("day", lit(day))

    hash_user
      .groupBy("day", "device_id")
      .agg(countDistinct("geo_hash_7") as "geo_hash_7")
      .groupBy("day")
      .agg(
        avg("geo_hash_7") as "geo_hash_7_avg",
        stddev_pop("geo_hash_7") as "geo_hash_7_std",
        count("device_id") as "devices"
      )
      .withColumn("country", lit(country))
      .withColumn("day", lit(day))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .partitionBy("day", "country")
      .save("/datascience/coronavirus/geohashes_by_country")

    // Agregaciones geogŕaficas

    //Levantamos la tabla de equivalencias
    val geo_hash_table = spark.read
      .format("csv")
      .option("header", "true")
      .load(
        "/datascience/geo/geohashes_tables/%s_GeoHash_to_Entity.csv"
          .format(country)
      )

    //Levantamos la data
    val geo_labeled_users = spark.read
      .format("parquet")
      .load(
        "/datascience/coronavirus/geohashes_by_user/day=%s/country=%s"
          .format(day, country)
      )
      .withColumn("day",lit(day))
      .join(geo_hash_table, Seq("geo_hash_7"))

    geo_labeled_users.persist()

    ///////////Agregación Nivel 1
    geo_labeled_users
      .groupBy("Level1_Code", "Level1_Name", "day", "device_id")
      .agg(countDistinct("geo_hash_7") as "geo_hash_7")
      .groupBy("Level1_Code", "Level1_Name", "day")
      .agg(
        count("device_id") as "devices",
        avg("geo_hash_7") as "geo_hash_7_avg",
        stddev_pop("geo_hash_7") as "geo_hash_7_std"
      )
      .withColumn("country", lit(country))
      .withColumn("day", lit(day))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .partitionBy("day", "country")
      .save("/datascience/coronavirus/geohashes_by_level_1")

    ///////////Agregación Nivel 2
    geo_labeled_users
      .groupBy(
        "Level1_Code",
        "Level1_Name",
        "Level2_Code",
        "Level2_Name",
        "day",
        "device_id"
      )
      .agg(countDistinct("geo_hash_7") as "geo_hash_7")
      .groupBy(
        "Level1_Code",
        "Level1_Name",
        "Level2_Code",
        "Level2_Name",
        "day"
      )
      .agg(
        count("device_id") as "devices",
        avg("geo_hash_7") as "geo_hash_7_avg",
        stddev_pop("geo_hash_7") as "geo_hash_7_std"
      )
      .withColumn("country", lit(country))
      .withColumn("day", lit(day))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .partitionBy("day", "country")
      .save("/datascience/coronavirus/geohashes_by_level_2")

  }

  def main(args: Array[String]) {

    Logger.getRootLogger.setLevel(Level.WARN)

    var spark = SparkSession.builder
      .appName("Coronavirus Daily Data")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    var since = if (args.length > 0) args(0).toInt else 1
    
    //distance_traveled_ar(spark,since)
    //distance_traveled_mx(spark,since)
    distance_traveled_rest(spark, since, "PE")
    distance_traveled_rest(spark, since, "CL")
    distance_traveled_rest(spark, since, "CO")

    // try {
    //   distance_traveled_rest(spark, since, "PE")
    // } catch {
    //   case e: Throwable => {
    //     println("Error: PE - %s".format(since))
    //   }
    // }
    // try {
    //   distance_traveled_rest(spark, since, "CO")
    // } catch {
    //   case e: Throwable => {
    //     println("Error: CO - %s".format(since))
    //   }
    // }
    // try {
    //   distance_traveled_rest(spark, since, "CL")
    // } catch {
    //   case e: Throwable => {
    //     println("Error: CL - %s".format(since))
    //   }
    // }
  }
}