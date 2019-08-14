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
    val dfs = days
      .flatMap(
        day =>
          (0 until 24).map(
            hour =>
              path + "/hour=%s%02d/"
                .format(day, hour)
          )
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      .map(
        x =>
          spark.read
            .option("basePath", "/datascience/data_audiences_streaming/")
            .parquet(x)
            .filter("event_type IN ('batch', 'data', 'tk', 'pv')")
            .select("device_id", "segments", "country")
            .withColumn("segments", explode(col("segments")))
            .withColumn("day", lit(x.split("/").last.slice(0, 8)))
            .withColumnRenamed("segments", "feature")
            .withColumn("count", lit(1))
      )

    val df = dfs.reduce((df1, df2) => df1.union(df2))

    val grouped_data = df
      .groupBy("device_id", "feature", "country")
      .agg(sum("count").as("count"))

    grouped_data.write
      .format("parquet")
      .mode("append")
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
      .getOrCreate()

    // Parseo de parametros
    val ndays = if (args.length > 0) args(0).toInt else 20
    val from = if (args.length > 1) args(1).toInt else 1

    for (i <- 0 until ndays){
        generate_triplets_segments(spark, 1, from+i)
    }
  }
}
