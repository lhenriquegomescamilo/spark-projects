package main.scala
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
  count,
  mean,
  stddev
}
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path

object DataGoogleAnalytics {

  /**
    * Este metodo se encarga de generar un dataframe de la pinta < device_id, mean(segment 2), mean(segment 3), ..., std(2), std(3), ..., count(2)>
    * utilizando la data ubicada en data_audiences_p y la distribucion de google analytics.
    * Una vez generado el dataframe se lo guarda en formato
    * parquet dentro de /datascience/data_demo/data_google_analytics
    * Los parametros que recibe son:
    *
    * @param spark: Spark session object que sera utilizado para cargar los DataFrames.
    * @param ndays: cantidad de dias que se utilizaran para generar los triplets.
    *
    */
  def get_data_google_analytics(spark: SparkSession, ndays: Int) {
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
              "/datascience/data_audiences/day=%s".format(day)
            )
          )
      )
      .map(
        x =>
          spark.read
            .format("parquet")
            .load("/datascience/data_audiences/day=%s".format(x))
            .withColumn("day", lit(x))
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
    val filename = "/data/metadata/20190316-domains-counts.tsv"
  
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
      .partitionBy("country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_demo/data_google_analytics_domain")
  }

  /**
    * Este metodo se encarga de generar un dataframe de la pinta < device_id, mean(segment 2), mean(segment 3), ..., std(2), std(3), ..., count(2)>
    * utilizando la data ubicada en data_audiences_p y la distribucion de google analytics.
    * Una vez generado el dataframe se lo guarda en formato
    * parquet dentro de /datascience/data_demo/data_google_analytics
    * Los parametros que recibe son:
    *
    * @param spark: Spark session object que sera utilizado para cargar los DataFrames.
    * @param ndays: cantidad de dias que se utilizaran para generar los triplets.
    *
    */
    def get_data_google_analytics_path(spark: SparkSession, ndays: Int) {
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
                "/datascience/data_audiences/day=%s".format(day)
              )
            )
        )
        .map(
          x =>
            spark.read
              .format("parquet")
              .load("/datascience/data_audiences/day=%s".format(x))
              .withColumn("day", lit(x))
              .select("device_id", "url", "day", "country", "timestamp")
        )
  
      /// Concatenamos los dataframes y nos quedamos solamente con el dominio de la url
      val df = dfs
        .reduce((df1, df2) => df1.union(df2))
        .withColumn(
          "url",
          regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
        )
  
      /// Leemos el archivo que tiene las distribuciones para cada URL
      val filename = "/data/metadata/20190316-paths-counts.tsv"

      val distributions = spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", "\t")
        .load(filename)
        .withColumnRenamed("PATH", "url")
        .withColumn(
          "url",
          regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
        )
  
      /// Hacemos el join de ambos dataframes para obtener la distribucion de cada device_id
      val joint = df.join(broadcast(distributions), Seq("url"))
  
      joint.write
        .format("parquet")
        .partitionBy("country")
        .mode(SaveMode.Overwrite)
        .save("/datascience/data_demo/data_google_analytics_path")
    }


  def main(args: Array[String]) {
    /// Configuracion spark
    val spark =
      SparkSession.builder.appName("Data Demo: Google Analytics").getOrCreate()

    // Parseo de parametros
    val ndays = if (args.length > 0) args(0).toInt else 30

    get_data_google_analytics(spark, ndays)
    get_data_google_analytics_path(spark, ndays)
  }
}
