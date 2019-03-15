package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{
  upper,
  col,
  abs,
  udf,
  regexp_replace,
  collect_list,
  split,
  size,
  lit,
  concat_ws
}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path

object keywordIngestion {

  /**
    * Este metodo se encarga de generar un archivo de la pinta <device_id, url_keywords, content_keys, all_segments, country>
    * utilizando la data ubicada en data_keywords_p. Siendo url_keywords y content_keywords listas de keywords separadas por ','
    * y all_segments una lista de segmentos tambien separada por ','.
    * La diferencia principal con el metodo get_data_for_queries es que este
    * metodo formatea los datos para que puedan ser ingestados en elastic (se agrega c_ al country, as_ a cada segmento)
    * Una vez generado el dataframe se lo guarda en formato tsv dentro de /datascience/data_keywords_elastic/
    * Los parametros que recibe son:
    *
    * @param spark: Spark session object que sera utilizado para cargar los DataFrames.
    * @param today: Es un string con el dia actual (se utilizara el dia como nombre de archivo al guardar).
    *
    */
  def get_data_for_elastic(spark: SparkSession, today: String) {
    // Armamos el archivo que se utilizara para ingestar en elastic
    val udfAs = udf(
      (segments: Seq[String]) => segments.map(token => "as_" + token)
    )
    val udfCountry = udf((country: String) => "c_" + country)
    val udfXp = udf(
      (segments: Seq[String], et: String) =>
        if (et == "xp") segments :+ "xp"
        else segments
    )
    val udfJoin = udf(
      (lista: Seq[String]) =>
        if (lista.length > 0) lista.reduce((seg1, seg2) => seg1 + "," + seg2)
        else ""
    )
    /// Leemos la data del dia actual previamente generada
    val joint = spark.read
      .format("parquet")
      .load("/datascience/data_keywords_p/day=%s".format(today))

    /// Formateamos la data en formato elastic
    val to_csv = joint
      .select(
        "device_id",
        "url_keys",
        "content_keys",
        "all_segments",
        "event_type",
        "country"
      )
      .withColumn("all_segments", udfAs(col("all_segments")))
      .withColumn("all_segments", udfXp(col("all_segments"), col("event_type")))
      .withColumn("country", udfCountry(col("country")))
      .withColumn("all_segments", udfJoin(col("all_segments")))
      .withColumn("url_keys", udfJoin(col("url_keys")))
      .select(
        "device_id",
        "url_keys",
        "content_keys",
        "all_segments",
        "country"
      )

    to_csv
      .repartition(100)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("sep", "\t")
      .save("/datascience/data_keywords_elastic/%s.csv".format(today))

  }

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
            new Path("/datascience/data_keyword_ingestion/%s.csv".format(day))
          )
      )
      .map(day => "/datascience/data_keyword_ingestion/%s.csv".format(day))

    val df = spark.read
      .format("csv")
      .load(dfs: _*)
      .withColumnRenamed("_c0", "url")
      .withColumnRenamed("_c1", "content_keys")
      .withColumn("content_keys", split(col("content_keys"), "\\|"))
      .withColumnRenamed("_c2", "count")
      .withColumnRenamed("_c3", "country_web")
      .withColumn(
          "url",
          regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
        )
      .drop("count")
      .dropDuplicates("url")

    df
  }

  /**
    * Esta funcion retorna un DataFrame con toda la data proveniente del pipeline de data_audiences.
    * Ademas, esta función se encarga de preprocesar las URLs de manera que se obtengan todas las
    * keywords de dicha data.
    */
  def getAudienceData(spark: SparkSession, today: String): DataFrame = {
    // En primer lugar definimos un par de funciones que seran de utilidad
    // La primer funcion sirve para eliminar todos los tokens que tienen digitos
    val udfFilter = udf(
      (segments: Seq[String]) =>
        segments.filter(token => !(token.matches("\\d*")))
    )

    // Esta funcion toma el campo segments y all_segments y los pone todos en un mismo listado
    val udfGetSegments = udf(
      (segments: Seq[String], all_segments: Seq[String], event_type: String) =>
        (segments
          .map(s => "s_%s".format(s))
          .toList ::: all_segments.map(s => "as_%s".format(s)).toList)
          .map(s => "%s%s".format((if (event_type == "xp") "xp" else ""), s))
          .toSeq
    )

    // Finalme
    spark.read
      .parquet("/datascience/data_audiences_p/day=%s".format(today)) // Leemos la data
      .repartition(500)
      .withColumn(
        "segments",
        udfGetSegments(col("segments"), col("all_segments"), col("event_type"))
      ) // En este punto juntamos todos los segmentos en una misma columna
      .withColumn(
          "url",
          regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
        )
      .select(
        "device_id",
        "device_type",
        "url",
        "country",
        "segments"
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
      since: Int
  ) {
    // This function takes a list of lists and returns only a list with all the values.
    val flatten = udf((xs: Seq[Seq[String]]) => xs.flatten)

    // Primero levantamos el dataframe que tiene toda la data de los usuarios con sus urls
    val URLkeys = getKeywordsByURL(spark, ndays, today, since).repartition(100)

    // Ahora levantamos la data de las audiencias
    val df_audiences = getAudienceData(spark, today)

    // Hacemos el join entre nuestra data y la data de las urls con keywords.
    //val df_b = spark.sparkContext.broadcast(df)
    val joint = df_audiences
      .join(broadcast(URLkeys), Seq("url"), "left_outer")
      .na
      .fill("")
      .groupBy("device_id", "device_type", "country")
      .agg(
        collect_list("segments").as("segments"),
        collect_list("content_keys").as("content_keys"),
        collect_list("url").as("url")
      )
      .withColumn("content_keys", flatten(col("content_keys")))
      .withColumn("segments", flatten(col("segments")))
      .withColumn("url", concat_ws("|", col("url")))
      .withColumn("day", lit(today)) // Agregamos el dia

    // Guardamos la data en formato parquet
    joint
      .write
      .format("parquet")
      .mode("append")
      .partitionBy("day", "country")
      .save("/datascience/data_keywords/")
    // df_b.unpersist()
    // df_b.destroy()
  }

  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder.appName("keyword ingestion").getOrCreate()
    val ndays = if (args.length > 0) args(0).toInt else 10
    val since = if (args.length > 1) args(1).toInt else 1
    val actual_day = if (args.length > 2) args(2).toInt else 1

    val today = DateTime.now().minusDays(actual_day).toString("yyyyMMdd")

    get_data_for_queries(spark, ndays, today, since)

    // val today = DateTime.now().minusDays(1)
    // val days = (0 until 20).map(
    //   since =>
    //     get_data_for_queries(
    //       spark,
    //       ndays,
    //       today.minusDays(since).toString("yyyyMMdd"),
    //       since
    //     )
    // )

    //get_data_for_elastic(spark,today)
  }
}

