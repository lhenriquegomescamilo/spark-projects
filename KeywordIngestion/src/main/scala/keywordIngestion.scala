package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{
  upper,
  col,
  abs,
  udf,
  regexp_replace,
  split,
  lit
}
import org.apache.spark.sql.SaveMode
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
      .map(
        x =>
          spark.read
            .format("csv")
            .load("/datascience/data_keyword_ingestion/%s.csv".format(x))
      )

    val df = dfs
      .reduce(_ union _)
      .withColumnRenamed("_c0", "url")
      .withColumnRenamed("_c1", "content_keys")
      .withColumnRenamed("_c2", "count")
      .withColumnRenamed("_c3", "country")
      .drop("country")

    // Levantamos el dataframe que tiene toda la data de los usuarios con sus urls
    val udfFilter = udf(
      (segments: Seq[String]) =>
        segments.filter(token => !(token.matches("\\d*")))
    )

    val df_audiences = spark.read
      .parquet("/datascience/data_audiences_p/day=%s".format(today))
      .select(
        "device_id",
        "event_type",
        "all_segments",
        "url",
        "device_type",
        "country"
      )
      .withColumn("url_keys", regexp_replace(col("url"), """https*://""", ""))
      .withColumn(
        "url_keys",
        regexp_replace(col("url_keys"), """[/,=&\.\(\) \|]""", " , ")
      )
      .withColumn("url_keys", regexp_replace(col("url_keys"), """%..""", " , "))
      .withColumn("url_keys", split(col("url_keys"), " , "))
      .withColumn("url_keys", udfFilter(col("url_keys")))
      .withColumn("day", lit(today))

    // Hacemos el join entre nuestra data y la data de las urls con keywords.
    val joint = df_audiences.join(broadcast(df), Seq("url"), "left_outer").na.fill("")
    // Guardamos la data en formato parquet
    joint.write
      .format("parquet")
      .mode("append")
      .partitionBy("day")
      .save("/datascience/data_keywords/")
    df.unpersist()
    df.destroy()
  }

  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder.appName("keyword ingestion").getOrCreate()
    val ndays = if (args.length > 0) args(0).toInt else 10
    //val since = if (args.length > 1) args(1).toInt else 1
    val actual_day = if (args.length > 2) args(2).toInt else 1

    //val today = DateTime.now().minusDays(actual_day)

    //get_data_for_queries(spark,ndays,today,since)
    val today = DateTime.now().minusDays(1)
    val days = (0 until 20).map(
      since =>
        get_data_for_queries(
          spark,
          ndays,
          today.minusDays(since).toString("yyyyMMdd"),
          since
        )
    )
    //get_data_for_elastic(spark,today)
  }
}
