package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, DataFrame, Row}
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}


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
      .na
      .drop()
      .dropDuplicates("url")

    val processed =
      if (df.columns.contains("_c6"))
        df.withColumnRenamed("_c5", "domain")
          .withColumnRenamed("_c6", "stemmed_keys")
          .withColumn("stemmed_keys", split(col("stemmed_keys"), " "))
      else if (df.columns.contains("_c5"))
        df.withColumnRenamed("_c5", "domain")
          .withColumn("stemmed_keys", col("content_keys"))
      else
        df.withColumn("domain", lit(""))
          .withColumn("stemmed_keys", col("content_keys"))

    processed
  }

  /**
    * Esta funcion retorna un DataFrame con toda la data proveniente del pipeline de data_audiences.
    * Ademas, esta función se encarga de preprocesar las URLs de manera que se obtengan todas las
    * keywords de dicha data.
    */
  def getAudienceData(spark: SparkSession, today: String): DataFrame = {
    val df = spark.read
              .option("basePath", "/datascience/data_audiences_streaming/")
              .parquet("/datascience/data_audiences_streaming/hour=%s*".format(today)) // We read the data
              .select(
                "device_id",
                "device_type",
                "url",
                "country"
              )

    processURLHTTP(df)
        .withColumn("url",regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", ""))
        .withColumn("url",regexp_replace(col("url"), "'", ""))
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
    val zip = udf((xs: Seq[String], ys: Seq[String]) => xs.zip(ys))

    println("DF AUDIENCES:")
    df_audiences.show()

    println("DF URL KEYS:")
    URLkeys.show()

    // Hacemos el join entre nuestra data y la data de las urls con keywords.
    val joint = df_audiences
      .join(URLkeys, Seq("composite_key"))
      .drop("composite_key")
      .withColumn(
        "keys",
        explode(zip(col("content_keys"), col("stemmed_keys")))
      )
      .select(
        col("device_id"),
        col("device_type"),
        col("country"),
        col("keys._1").alias("content_keys"),
        col("keys._2").alias("stemmed_keys"),
        col("domain")
      )
      .groupBy(
        "device_id",
        "device_type",
        "country",
        "content_keys",
        "stemmed_keys",
        "domain"
      )
      .count()
      .withColumn("day", lit(today)) // Agregamos el dia


    // Guardamos la data en formato parquet
    joint.write
      .format("parquet")
      .mode("overwrite")
      .partitionBy("day", "country")
      .save("/datascience/data_keywords/")
  }

 def processURLHTTP(dfURL: DataFrame, field: String = "url"): DataFrame = {
    // First of all, we get the domains, and filter out those ones that are very generic
    
    val generic_domains = List(
      "google",
      "doubleclick",
      "facebook",
      "messenger",
      "yahoo",
      "android",
      "android-app",
      "bing",
      "instagram",
      "cxpublic",
      "content",
      "cxense",
      "criteo",
      "outbrain",
      "flipboard",
      "googleapis",
      "googlequicksearchbox",
      "0_media",
      "provider",
      "parser",
      "downloads",
      "xlxx",
      "xvideo2",
      "coffetube"
    )
    val query_generic_domains = generic_domains
      .map(dom => "domain NOT LIKE '%" + dom + "%'")
      .mkString(" AND ")
    val filtered_domains = dfURL
      .selectExpr("*", "parse_url(%s, 'HOST') as domain".format(field))
      .filter(query_generic_domains)
    // Now we filter out the domains that are IPs
    val filtered_IPs = filtered_domains
      .withColumn(
        "domain",
        regexp_replace(col("domain"), "^([0-9]+\\.){3}[0-9]+$", "IP")
      )
      .filter("domain != 'IP'")
    // Now if the host belongs to Retargetly, then we will take the r_url field from the QS
    val retargetly_domains = filtered_IPs
      .filter("domain LIKE '%retargetly%'")
      .selectExpr(
        "*",
        "parse_url(%s, 'QUERY', 'r_url') as new_url".format(field)
      )
      .filter("new_url IS NOT NULL")
      .withColumn(field, col("new_url"))
      .drop("new_url")
    // Then we process the domains that come from ampprojects
    val pattern =
      """^([a-zA-Z0-9_\-]+).cdn.ampproject.org/?([a-z]/)*([a-zA-Z0-9_\-\/\.]+)?""".r
    def ampPatternReplace(url: String): String = {
      var result = ""
      if (url != null) {
        val matches = pattern.findAllIn(url).matchData.toList
        if (matches.length > 0) {
          val list = matches
            .map(
              m =>
                if (m.groupCount > 2) m.group(3)
                else if (m.groupCount > 0) m.group(1).replace("-", ".")
                else "a"
            )
            .toList
          result = list(0).toString
        }
      }
      result
    }
    val ampUDF = udf(ampPatternReplace _, StringType)
    val ampproject_domains = filtered_IPs
      .filter("domain LIKE '%ampproject%'")
      .withColumn(field, ampUDF(col(field)))
      .filter("length(%s)>0".format(field))
    // Now we union the filtered dfs with the rest of domains
    val non_filtered_domains = filtered_IPs.filter(
      "domain NOT LIKE '%retargetly%' AND domain NOT LIKE '%ampproject%'"
    )
    val filtered_retargetly = non_filtered_domains
      .unionAll(retargetly_domains)
      .unionAll(ampproject_domains)
    // Finally, we remove the querystring and protocol
    filtered_retargetly
      .withColumn(
        field,
        regexp_replace(col(field), "://(.\\.)*", "://")
      )
      .withColumn(
        field,
        regexp_replace(col(field), "(\\?|#).*", "")
      )
      .drop("domain")
      .withColumn(field,lower(col(field)))
        .withColumn(
        field,
        regexp_replace(col(field), "@", "_")
      )    
  }  

  def main(args: Array[String]) {
    /// Configuracion spark
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder
      .appName("keyword ingestion")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()
    val ndays = if (args.length > 0) args(0).toInt else 10
    val since = if (args.length > 1) args(1).toInt else 1
    val actual_day = if (args.length > 2) args(2).toInt else 1

    val today = DateTime.now().minusDays(actual_day).toString("yyyyMMdd")

    get_data_for_queries(spark, ndays, today, since, replicationFactor = 4)
  }
}
