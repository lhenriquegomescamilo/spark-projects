package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.hadoop.fs.Path


object keywordIngestion {
  /**
   * This method reads the data from the selected keywords, processes them and returns them as a DataFrame. 
   * This data contains the following columns:
   *    - URL
   *    - Count
   *    - Country
   *    - Keywords
   *    - TF/IDF Scores
   *    - Domain
   *    - Stemmed keywords
   * 
   * The URL is parsed in such a way that it is easier to join with data_audiences.
  */
  def getKeywordsByURL(
      spark: SparkSession,
      ndays: Int,
      today: String,
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

    val stem_column = if (df.columns.contains("_c6")) "_c6" else "_c3"

    val processed = df
      .withColumnRenamed("_c0", "url")
      .withColumnRenamed("_c1", "count")
      .withColumnRenamed("_c2", "country_web")
      .withColumnRenamed("_c3", "content_keys")
      .withColumnRenamed("_c4", "scores")
      .withColumnRenamed("_c5", "domain")
      .withColumnRenamed(stem_column, "stemmed_keys")
      .withColumn("content_keys", split(col("content_keys"), " "))
      .withColumn("stemmed_keys", split(col("stemmed_keys"), " "))
      .withColumn(
        "url",
        regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
      )
      .drop("count", "scores")
      .dropDuplicates("url")

    df
  }

  /**
    * Esta funcion retorna un DataFrame con toda la data proveniente del pipeline de data_audiences.
    * Ademas, esta función se encarga de preprocesar las URLs de manera que se obtengan todas las
    * keywords de dicha data.
    */
  def getAudienceData(spark: SparkSession, today: String): DataFrame = {
    spark.read
      .option("basePath", "/datascience/data_audiences_streaming/")
      .parquet("/datascience/data_audiences_streaming/hour=%s*".format(today)) // We read the data
      .withColumn(
        "url",
        regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
      ) // Standarize the URL
      .select(
        "device_id",
        "device_type",
        "url",
        "country"
      )
  }

  /**
    * Este metodo se encarga de generar un archivo de la pinta <device_id, url_keywords, content_keys, all_segments, country>.
    * Para armar este dataframe se utiliza la data de las keywords subidas diariamente (se utilizan los ultimos ndays y el parametro since para levantar la data)
    * la cual tiene la pinta < url, keywords >. Luego se lee la data del dia actual desde data_audience_p
    * < device_id","event_type","all_segments","url","device_type","country>  y se hace un join entre ambos dataframes para quedarse con las keywords.
    * Finalmente se procesan y filtran las keywords. Una vez generado el dataframe se lo guarda en formato parquet dentro de
    * /datascience/data_keywords_p/.

    * Los parametros que recibe son:
    *
    * @param spark: Spark session object que sera utilizado para cargar los DataFrames.
    * @param ndays: Un entero que representa la cantidad de dias a utilizar para leer la data de keywords.
    * @param today: Es un string con el dia actual en formato yyyyMMdd (se utilizara el dia como nombre de archivo al guardar).
    * @param since: Un entero que representa desde cuantos dias hacia atras comenzara a leerse la data de kywords.
    */
  def get_data_for_queries(
      spark: SparkSession,
      ndays: Int,
      today: String,
      since: Int,
      replicationFactor: Int
  ) {
    // This function takes a list of lists and returns only a list with all the values.
    val flatten = udf((xs: Seq[Seq[String]]) => xs.flatten)

    // Primero levantamos el dataframe que tiene toda la data de los usuarios con sus urls
    val URLkeys = (0 until replicationFactor)
      .map(
        i =>
          getKeywordsByURL(spark, ndays, today, since)
            .withColumn("composite_key", concat(col("url"), lit("@"), lit(i)))
      )
      .reduce((df1, df2) => df1.unionAll(df2))
      .drop("url")

    // Ahora levantamos la data de las audiencias
    val df_audiences = getAudienceData(spark, today).withColumn(
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

    // This function appends two columns
    val zip = udf((xs: Seq[Long], ys: Seq[Long]) => xs.zip(ys))

    // Hacemos el join entre nuestra data y la data de las urls con keywords.
    val joint = df_audiences
      .join(URLkeys, Seq("composite_key"))
      .drop("composite_key")
      .withColumn("keys", explode(zip(col("content_keys"), col("stemmed_keys"))))
      .select(col("device_id"), col("device_type"), col("country"), col("keys._1").alias("content_keys"), col("keys._2").alias("stemmed_keys"), col("domain"))
      .groupBy("device_id", "device_type", "country", "content_keys", "stemmed_keys", "domain")
      .count()
      .withColumn("day", lit(today)) // Agregamos el dia

    // Guardamos la data en formato parquet
    joint.write
      .format("parquet")
      .mode("append")
      .partitionBy("day", "country")
      .save("/datascience/data_keywords/")
  }

  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder
          .appName("keyword ingestion")
          .config("spark.sql.files.ignoreCorruptFiles", "true")
          .getOrCreate()
    val ndays = if (args.length > 0) args(0).toInt else 10
    val since = if (args.length > 1) args(1).toInt else 1
    val actual_day = if (args.length > 2) args(2).toInt else 1

    val today = DateTime.now().minusDays(actual_day).toString("yyyyMMdd")

    get_data_for_queries(spark, ndays, today, since, replicationFactor = 4)
  }
}
