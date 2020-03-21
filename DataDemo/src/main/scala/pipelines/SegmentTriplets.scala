package main.scala.pipelines
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{
  upper,
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
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}

object SegmentTriplets {

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
  def generate_triplets_segments(
      spark: SparkSession,
      ndays: Int,
      from: Int = 1
  ) {
    /// Configuraciones de spark
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    /// Obtenemos la data de los ultimos ndays
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(from)

    val days =
      (0 until ndays).map(start.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_audiences_streaming"
    val files = days
      .flatMap(
        day =>
          (0 until 24).map(
            hour =>
              path + "/hour=%s%02d/"
                .format(day, hour)
          )
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    files.foreach(println)
    println(start)
    // val dfs = files
    //   .map(
    //     x =>
    //       spark.read
    //         .option("basePath", "/datascience/data_audiences_streaming/")
    //         .parquet(x)
    //         .filter(
    //           "event_type IN ('batch', 'data', 'tk', 'pv', 'retroactive')"
    //         )
    //         .select(
    //           "device_id",
    //           "segments",
    //           "country",
    //           "event_type",
    //           "id_partner"
    //         )
    //         .withColumn("segments", explode(col("segments")))
    //         .withColumn("day", lit(x.split("/").last.slice(5, 13)))
    //         .withColumnRenamed("segments", "feature")
    //         .withColumn("count", lit(1))
    //   )

    // val df = dfs.reduce((df1, df2) => df1.union(df2))

    val df = spark.read
      .option("basePath", "/datascience/data_audiences_streaming/")
      .parquet(files: _*)
      .filter(
        "event_type IN ('batch', 'data', 'tk', 'pv', 'retroactive')"
      )
      .select(
        "device_id",
        "segments",
        "country",
        "event_type",
        "id_partner",
        "datetime",
        "activable",
        "device_type"
      )
      .withColumn("segments", explode(col("segments")))
      .withColumnRenamed("segments", "feature")
      .withColumn("count", lit(1))
      .withColumn("day", date_format(col("datetime"), "yyyyMMdd"))
      .drop("datetime")

    /**
      Update: 20200318
      - Se agregan event_types para pipeline de Taxonomy/TaxoInsights
      - Se mapean a ints con la intuicion de reducir espacio en disco. Debe evaluarse la eficiencia de esto.
      */
    val map_events = Map(
          "batch" -> 0,
          "data" -> 1,
          "tk" -> 2,
          "pv" -> 3,
          "retroactive" -> 4)
    val mapUDF_events = udf((event_type: String) => map_events(event_type))

    val grouped_data = df
      .select("device_id", "feature", "country", "day", "id_partner", "device_type", "activable", "event_type")
      .withColumn("event_type",mapUDF_events(col("event_type")))  //mapeo de event_types
      .distinct()
      .withColumn("count", lit(1))
    // .groupBy("device_id", "feature", "country", "day", "id_partner")
    // .agg(sum("count").as("count"))

    grouped_data
      .orderBy("device_id")
      .write
      .format("parquet")
      .mode("overwrite")
      .partitionBy("day", "country")
      .save("/datascience/data_triplets/segments")
  }

  /**
    * Este metodo se encarga de generar una lista de triplets de la pinta <device_id, keyword, count>
    * utilizando la data ubicada en data_keywords_p. Para generar los triplets se utilizaran tanto las keywords
    * provenientes de las urls visitadas por los usuarios como las keywords provienentes del contenido de las urls (scrapping)-
    * Una vez generado el dataframe se lo guarda en formato parquet dentro de /datascience/data_demo/triplets_keywords
    * Los parametros que recibe son:
    *
    * @param spark: Spark session object que sera utilizado para cargar los DataFrames.
    * @param ndays: cantidad de dias que se utilizaran para generar los triplets.
    *
    */
  def generate_triplets_keywords(spark: SparkSession, ndays: Int) {
    /// Configuraciones de spark
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    /// Obtenemos la data de los ultimos ndays
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(ndays)
    val end = DateTime.now.minusDays(0)

    val daysCount = Days.daysBetween(start, end).getDays()
    val days =
      (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

    val dfs = days
      .filter(
        day =>
          fs.exists(
            new org.apache.hadoop.fs.Path(
              "/datascience/data_keywords/day=%s".format(day)
            )
          )
      )
      .map(
        x =>
          spark.read
            .parquet("/datascience/data_keywords/day=%s".format(x))
            .select("device_id", "content_keys", "country")
      )

    val df = dfs.reduce((df1, df2) => df1.union(df2))

    /// Obtenemos las keywords del contenido de la url
    val df_content_keys = df
      .select("device_id", "content_keys", "country")
      //.withColumn("content_keys",split(col("content_keys"),","))
      .withColumnRenamed("content_keys", "feature")
      .withColumn("count", lit(1))
      .withColumn("feature", explode(col("feature")))

    val grouped_data = df_content_keys
      .groupBy("device_id", "feature", "country")
      .agg(sum("count").as("count"))

    /// Filtramos las palabras que tiene longitud menor a 3 y guardamos
    grouped_data
      .where(length(col("feature")) > 3)
      .write
      .format("parquet")
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .partitionBy("country")
      .save("/datascience/data_demo/triplets_keywords")
  }

  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder
      .appName("Get triplets: keywords and segments")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    // Parseo de parametros
    val ndays = if (args.length > 0) args(0).toInt else 20
    val from = if (args.length > 1) args(1).toInt else 1

    for (i <- 0 until ndays) {
      generate_triplets_segments(spark, 1, from + i)
    }
  }
}
