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

object DataGoogleAnalytics {

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
  def generate_google_analytics_domain(
      spark: SparkSession,
      ndays: Int,
      from: Int = 1,
      filename:String
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
            .filter("country = 'AR' or country = 'MX'") // We only get AR and MX users because we only have GA data for those countries
            .withColumn("day", lit(x.split("/").last.slice(5, 13)))
            .withColumnRenamed("time", "timestamp")
            .select("device_id", "url", "day", "country", "timestamp")
      )

    /// Concatenamos los dataframes y nos quedamos solamente con el dominio de la url
    val df = dfs
      .reduce((df1, df2) => df1.union(df2))
      .withColumn(
        "url",
        regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
      )
      .withColumn("url", regexp_replace(col("url"), "/.*", ""))

    /// Leemos el archivo que tiene las distribuciones para cada URL
    val distributions = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load(filename)
      .withColumnRenamed("DOMAIN", "url")
      .withColumn(
        "url",
        regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
      )
      .withColumn("url", regexp_replace(col("url"), "/.*", ""))

    /// Hacemos el join de ambos dataframes para obtener la distribucion de cada device_id
    val joint = df.join(broadcast(distributions), Seq("url"))

    joint.write
      .format("parquet")
      .partitionBy("day","country")
      .mode("append")
      .save("/datascience/data_demo/google_analytics_domain")

  }
  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder
      .appName("Generate Google analytics data")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .getOrCreate()

    val ndays = if (args.length > 0) args(0).toInt else 1
    val from = if (args.length > 1) args(1).toInt else 1

    // Path con data del devicer
    val filename_domain = "/data/metadata/20190316-domains-counts.tsv"

    val ga_domain = generate_google_analytics_domain(spark, ndays, from, filename_domain);
  }
}
