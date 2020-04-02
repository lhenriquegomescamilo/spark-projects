package main.scala
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

object RandomTincho {

  def get_selected_keywords(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      country: String
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
      .option("header", "true")
      .load(dfs: _*)
      .filter("country = '%s'".format(country))
      .withColumnRenamed("kw", "kws")
      .withColumnRenamed("url_raw", "url")
      .withColumn("kws", split(col("kws"), " "))
      .select("url", "kws")
      .dropDuplicates("url")

    df
  }

  def get_gt_new_taxo(
      spark: SparkSession,
      ndays: Int,
      since: Int,
      country: String
  ) = {

    // Get selected keywords <url, [kws]>
    val selected_keywords = get_selected_keywords(
      spark,
      ndays = ndays,
      since = since,
      country = country
    )
    selected_keywords.cache()

    // Get Data urls <url>
    val data_urls =
      get_data_urls(spark, ndays = ndays, since = since, country = country)
    data_urls.cache()

    // Get queries <seg_id, query (array contains), query (url like)>
    var queries = spark.read.load("/datascience/custom/new_taxo_queries_join")

    var dfs: DataFrame = null
    var first = true
    // Iterate queries and get urls
    for (row <- queries.rdd.collect) {
      var segment = row(0).toString
      var query = row(1).toString
      var query_url_like = row(2).toString

      // Filter selected keywords dataframe using query with array contains
      selected_keywords
        .filter(query)
        .withColumn("segment", lit(segment))
        .select("url", "segment")
        .write
        .format("parquet")
        .mode("append")
        .save("/datascience/data_url_classifier/GT_new_taxo_queries")

      // Filter data_urls dataframe using query with array url LIKE
      data_urls
        .filter(query_url_like)
        .withColumn("segment", lit(segment))
        .select("url", "segment")
        .write
        .format("parquet")
        .mode("append")
        .save("/datascience/data_url_classifier/GT_new_taxo_queries")
    }

    // Groupby by url and concatenating segments with ;
    spark.read
      .load("/datascience/data_url_classifier/GT_new_taxo_queries")
      .groupBy("url")
      .agg(collect_list(col("segment")).as("segment"))
      .withColumn("segment", concat_ws(";", col("segment")))
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_url_classifier/GT_new_taxo")
  }

  def get_segments_pmi(spark: SparkSession, ndays: Int, since: Int) {

    val files = List(
      "/datascience/misc/cookies_chesterfield.csv",
      "/datascience/misc/cookies_marlboro.csv",
      "/datascience/misc/cookies_phillip_morris.csv",
      "/datascience/misc/cookies_parliament.csv"
    )

    /// Configuraciones de spark
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(since)

    val days = (0 until ndays).map(start.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_triplets/segments/"
    val dfs = days
      .map(day => path + "day=%s/".format(day) + "country=AR")
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      .map(
        x =>
          spark.read
            .option("basePath", "/datascience/data_triplets/segments/")
            .parquet(x)
            .select("device_id", "feature")
      )

    var data = dfs.reduce((df1, df2) => df1.union(df2))

    for (filename <- files) {
      var cookies = spark.read
        .format("csv")
        .load(filename)
        .withColumnRenamed("_c0", "device_id")

      data
        .join(broadcast(cookies), Seq("device_id"))
        .select("device_id", "feature")
        .dropDuplicates()
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .save(
          "/datascience/custom/segments_%s"
            .format(filename.split("/").last.split("_").last)
        )
    }
  }
  def test_tokenizer(spark: SparkSession) {

    var gtDF = spark.read
      .load("/datascience/data_url_classifier/gt_new_taxo_filtered")
      .withColumn("url", lower(col("url")))
      .withColumn("url_path", regexp_replace(col("url"), """^[^/]*/""", ""))
      .withColumn("url_keys", split(col("url_path"), "[^a-z0-9]"))
      .withColumn("keyword", explode(col("url_keys")))
      .filter(col("keyword").rlike("[a-z]{2,}"))
      .withColumn("keyword", regexp_replace(col("keyword"), " ", "_"))
      .withColumn("keyword", regexp_replace(col("keyword"), "\\(", ""))
      .withColumn("keyword", regexp_replace(col("keyword"), "\\)", ""))
      .withColumn("keyword", regexp_replace(col("keyword"), ",", ""))
      .groupBy("url", "segment")
      .agg(collect_list(col("keyword").as("url_keys")))
      .withColumn("url_keys", concat_ws(";", col("url_keys")))
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_url_classifier/gt_new_taxo_tokenized")
  }

  def get_matching_metrics(spark: SparkSession) {

    // Brazil2_101419_FINAL.csv -	dunnhumby/onboarding/GPA-BR.csv.gz
    var partner_br = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/misc/Brazil2_101419_FINAL.csv")
      .withColumnRenamed("email_sha256", "email")
      .withColumn("email", lower(col("email")))
      .select("email")
    partner_br.cache()

    var compared = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/misc/GPA-BR.csv.gz")
      .withColumnRenamed("DS_EMAIL_LOWER", "email")
      .withColumn("email", lower(col("email")))
      .select("email")

    var cant = partner_br
      .join(compared, Seq("email"), "inner")
      .select("email")
      .distinct
      .count

    println(
      "Brazil2_101419_FINAL.csv -	dunnhumby/onboarding/GPA-BR.csv.gz: %s"
        .format(cant)
    )

    // Brazil2_101419_FINAL.csv	- dunnhumby/onboarding/RD-BR.csv.gz
    compared = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/misc/RD-BR.csv.gz")
      .withColumnRenamed("ds_email_lower", "email")
      .withColumn("email", lower(col("email")))
      .select("email")
      .na
      .drop

    cant = partner_br
      .join(compared, Seq("email"), "inner")
      .select("email")
      .distinct
      .count

    println(
      "Brazil2_101419_FINAL.csv	- dunnhumby/onboarding/RD-BR.csv.gz: %s"
        .format(cant)
    )

    //Brazil2_101419_FINAL.csv	Retargetly
    compared = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'BR'")
      .select("ml_sh2")
      .withColumnRenamed("ml_sh2", "email")
      .withColumn("email", lower(col("email")))
      .select("email")

    cant = partner_br
      .join(compared, Seq("email"), "inner")
      .select("email")
      .distinct
      .count

    println("Brazil2_101419_FINAL.csv	Retargetly: %s".format(cant))

    //Brazil2_101419_FINAL.csv	acxiom/files/acxiom_BR_Partner_Universe_Extract_20190809.tsv.gz
    var df1 = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load(
        "/datascience/misc/acxiom_BR_Partner_Universe_Extract_20190809.tsv.gz"
      )
      .select("email_addr_01")
      .withColumnRenamed("email_addr_01", "email")
    var df2 = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load(
        "/datascience/misc/acxiom_BR_Partner_Universe_Extract_20190809.tsv.gz"
      )
      .select("email_addr_02")
      .withColumnRenamed("email_addr_02", "email")
    var df3 = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load(
        "/datascience/misc/acxiom_BR_Partner_Universe_Extract_20190809.tsv.gz"
      )
      .select("email_addr_03")
      .withColumnRenamed("email_addr_03", "email")
    var df4 = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load(
        "/datascience/misc/acxiom_BR_Partner_Universe_Extract_20190809.tsv.gz"
      )
      .select("email_addr_04")
      .withColumnRenamed("email_addr_04", "email")
    var df5 = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load(
        "/datascience/misc/acxiom_BR_Partner_Universe_Extract_20190809.tsv.gz"
      )
      .select("email_addr_05")
      .withColumnRenamed("email_addr_05", "email")
    var df6 = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load(
        "/datascience/misc/acxiom_BR_Partner_Universe_Extract_20190809.tsv.gz"
      )
      .select("email_addr_06")
      .withColumnRenamed("email_addr_06", "email")

    compared = df1
      .union(df2)
      .union(df3)
      .union(df4)
      .union(df5)
      .union(df6)
      .withColumn("email", lower(col("email")))

    println(
      "acxiom/files/acxiom_BR_Partner_Universe_Extract_20190809.tsv.gz: %s"
        .format(compared.select("email").distinct.count)
    )

    cant = partner_br
      .join(compared, Seq("email"), "inner")
      .select("email")
      .distinct
      .count

    println(
      "Brazil2_101419_FINAL.csv	acxiom/files/acxiom_BR_Partner_Universe_Extract_20190809.tsv.gz: %s"
        .format(cant)
    )

    // Colombia2_101419_FINAL.csv	dunnhumby/onboarding/Exito-CO.csv.gz
    val partner_co = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/misc/Colombia2_101419_FINAL.csv")
      .withColumnRenamed("email_sha256", "email")
      .withColumn("email", lower(col("email")))
      .select("email")
    partner_co.cache()

    compared = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/misc/Exito-CO.csv.gz")
      .withColumnRenamed("distinct_correo", "email")
      .withColumn("email", lower(col("email")))
      .select("email")
      .na
      .drop

    cant = partner_co
      .join(compared, Seq("email"), "inner")
      .select("email")
      .distinct
      .count

    println(
      "Colombia2_101419_FINAL.csv -	dunnhumby/onboarding/Exito-CO.csv.gz: %s"
        .format(cant)
    )

    // Colombia2_101419_FINAL.csv	Retargetly
    compared = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'CO'")
      .select("ml_sh2")
      .withColumnRenamed("ml_sh2", "email")
      .withColumn("email", lower(col("email")))
      .select("email")

    cant = partner_co
      .join(compared, Seq("email"), "inner")
      .select("email")
      .distinct
      .count

    println("Colombia2_101419_FINAL.csv -	Retargetly: %s".format(cant))

    // Argentina2_101419_FINAL.csv	Retargetly
    val partner_ar = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/misc/Argentina2_101419_FINAL.csv")
      .withColumnRenamed("email_sha256", "email")
      .withColumn("email", lower(col("email")))
      .select("email")
    partner_ar.cache()

    compared = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'AR'")
      .select("ml_sh2")
      .withColumnRenamed("ml_sh2", "email")
      .withColumn("email", lower(col("email")))

    cant = partner_ar
      .join(compared, Seq("email"), "inner")
      .select("email")
      .distinct
      .count

    println("Argentina2_101419_FINAL.csv	Retargetly: %s".format(cant))

    // Mexico2_101419_FINAL.csv -	Retargetly
    val partner_mx = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/misc/Mexico2_101419_FINAL.csv")
      .select("email_sha256")
      .withColumnRenamed("email_sha256", "email")
      .withColumn("email", lower(col("email")))
    partner_mx.cache()

    compared = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'MX'")
      .select("ml_sh2")
      .withColumnRenamed("ml_sh2", "email")
      .withColumn("email", lower(col("email")))

    cant = partner_mx
      .join(compared, Seq("email"), "inner")
      .select("email")
      .distinct
      .count

    println("Mexico2_101419_FINAL.csv -	Retargetly: %s".format(cant))

    // Mexico2_101419_FINAL.csv	acxiom/files/acxiom_MX_Partner_Universe_Extract_20190809.tsv.gz
    df1 = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load(
        "/datascience/misc/acxiom_MX_Partner_Universe_Extract_20190809.tsv.gz"
      )
      .select("email1")
      .withColumnRenamed("email1", "email")
    df2 = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load(
        "/datascience/misc/acxiom_MX_Partner_Universe_Extract_20190809.tsv.gz"
      )
      .select("email2")
      .withColumnRenamed("email2", "email")

    compared = df1
      .union(df2)
      .withColumn("email", lower(col("email")))

    println(
      "acxiom/files/acxiom_MX_Partner_Universe_Extract_20190809.tsv.gz: %s"
        .format(compared.select("email").distinct.count)
    )

    cant = partner_mx
      .join(compared, Seq("email"), "inner")
      .select("email")
      .distinct
      .count

    println(
      "Mexico2_101419_FINAL.csv	acxiom/files/acxiom_MX_Partner_Universe_Extract_20190809.tsv.gz: %s"
        .format(cant)
    )

    // Chile2_101419_FINAL.csv	Retargetly
    val partner_cl = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/misc/Chile2_101419_FINAL.csv")
      .select("email_sha256")
      .withColumnRenamed("email_sha256", "email")
      .withColumn("email", lower(col("email")))

    cant = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'CL'")
      .select("ml_sh2")
      .withColumnRenamed("ml_sh2", "email")
      .withColumn("email", lower(col("email")))
      .join(partner_cl, Seq("email"), "inner")
      .select("email")
      .distinct
      .count

    println("Chile2_101419_FINAL.csv	Retargetly: %s".format(cant))

    // Peru2_101419_FINAL.csv	Retargetly
    val partner_pe = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/misc/Peru2_101419_FINAL.csv")
      .select("email_sha256")
      .withColumnRenamed("email_sha256", "email")
      .withColumn("email", lower(col("email")))

    cant = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'PE'")
      .select("ml_sh2")
      .withColumnRenamed("ml_sh2", "email")
      .withColumn("email", lower(col("email")))
      .join(partner_pe, Seq("email"), "inner")
      .select("email")
      .distinct
      .count

    println("Peru2_101419_FINAL.csv	Retargetly: %s".format(cant))

  }

  def keywords_embeddings(
      spark: SparkSession,
      kws_path: String,
      embeddings_path: String
  ) {
    val word_embeddings = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/word_embeddings.csv")

    def isAllDigits(x: String) = x forall Character.isDigit

    val myUDF = udf(
      (keyword: String) => if (isAllDigits(keyword)) "DIGITO" else keyword
    )

    val dataset_kws = spark.read
      .load(kws_path)
      .withColumn("keywords", myUDF(col("keywords")))
      .withColumnRenamed("keywords", "word")
      .withColumn("word", lower(col("word")))

    // Checkpoint
    var join = dataset_kws
      .join(word_embeddings, Seq("word"), "inner")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_url_classifier/dataset_keyword_embedding_chkpt")

    var df = spark.read
      .format("parquet")
      .load("/datascience/data_url_classifier/dataset_keyword_embedding_chkpt")
    df.cache()

    for (i <- 1 to 300) {
      df = df.withColumn(i.toString, col(i.toString) * col("count"))
    }

    var df_preprocessed = processURL(df)
      .drop("word")
      .groupBy("url")
      .sum()

    for (i <- 1 to 300) {
      df_preprocessed = df_preprocessed.withColumn(
        "sum(%s)".format(i.toString),
        col("sum(%s)".format(i.toString)) / col("sum(count)")
      )
    }

    df_preprocessed.write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(embeddings_path)
  }

  def processURL_qs(url: String): String = {
    val columns = List(
      "r_mobile",
      "r_mobile_type",
      "r_app_name",
      "r_campaign",
      "r_lat_long",
      "id_campaign"
    )
    var res = ""

    try {
      if (url.toString.contains("?")) {
        val params = url
          .split("\\?", -1)(1)
          .split("&")
          .map(p => p.split("=", -1))
          .map(p => (p(0), p(1)))
          .toMap

        if (params.contains("r_mobile") && params("r_mobile").length > 0 && !params(
              "r_mobile"
            ).contains("[")) {
          res = columns
            .map(col => if (params.contains(col)) params(col) else "")
            .mkString(",")
        }
      }
    } catch {
      case _: Throwable => println("Error")
    }

    res
  }

  def get_report_gcba_1134(spark: SparkSession, ndays: Int, since: Int) {

    val myUDF = udf((url: String) => processURL_qs(url))

    /// Configuraciones de spark
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until ndays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_partner_streaming"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .flatMap(
        day =>
          (0 until 24).map(
            hour =>
              path + "/hour=%s%02d/id_partner=1134"
                .format(day, hour)
          )
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .filter("event_type = 'tk'")
      .select("url")

    df.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/1134_octubre")

  }

  def processURL(dfURL: DataFrame, field: String = "url"): DataFrame = {
    // First of all, we get the domains, and filter out those ones that are very generic
    val generic_domains = List(
      "google",
      "facebook",
      "yahoo",
      "android",
      "bing",
      "instagram",
      "cxpublic",
      "criteo",
      "outbrain",
      "flipboard",
      "googleapis",
      "googlequicksearchbox"
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
        regexp_replace(col(field), "http.*://(.\\.)*(www\\.){0,1}", "")
      )
      .withColumn(
        field,
        regexp_replace(col(field), "(\\?|#).*", "")
      )
      .drop("domain")
      .withColumn(field, lower(col(field)))
  }

  def processURLHTTP(dfURL: DataFrame, field: String = "url"): DataFrame = {
    // First of all, we get the domains, and filter out those ones that are very generic
    val generic_domains = List(
      "google",
      "facebook",
      "yahoo",
      "android",
      "bing",
      "instagram",
      "cxpublic",
      "criteo",
      "outbrain",
      "flipboard",
      "googleapis",
      "googlequicksearchbox"
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
      .withColumn(field, lower(col(field)))
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
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val urls = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    processURL(dfURL = urls, field = "url")
  }

  def get_gt_contextual(spark: SparkSession) {

    val segments = List(26, 32, 36, 59, 61, 82, 85, 92, 104, 118, 129, 131, 141,
      144, 145, 147, 149, 150, 152, 154, 155, 158, 160, 165, 166, 177, 178, 210,
      213, 218, 224, 225, 226, 230, 245, 247, 250, 264, 265, 270, 275, 276, 302,
      305, 311, 313, 314, 315, 316, 317, 318, 322, 323, 325, 326, 352, 353, 354,
      356, 357, 358, 359, 363, 366, 367, 374, 377, 378, 379, 380, 384, 385, 386,
      389, 395, 396, 397, 398, 399, 401, 402, 403, 404, 405, 409, 410, 411, 412,
      413, 418, 420, 421, 422, 429, 430, 432, 433, 434, 440, 441, 446, 447, 450,
      451, 453, 454, 456, 457, 458, 459, 460, 462, 463, 464, 465, 467, 895, 898,
      899, 909, 912, 914, 915, 916, 917, 919, 920, 922, 923, 928, 929, 930, 931,
      932, 933, 934, 935, 937, 938, 939, 940, 942, 947, 948, 949, 950, 951, 952,
      953, 955, 956, 957, 1005, 1116, 1159, 1160, 1166, 2623, 2635, 2636, 2660,
      2719, 2720, 2721, 2722, 2723, 2724, 2725, 2726, 2727, 2733, 2734, 2735,
      2736, 2737, 2743, 3010, 3011, 3012, 3013, 3014, 3015, 3016, 3017, 3018,
      3019, 3020, 3021, 3022, 3023, 3024, 3025, 3026, 3027, 3028, 3029, 3030,
      3031, 3032, 3033, 3034, 3035, 3036, 3037, 3038, 3039, 3040, 3041, 3055,
      3076, 3077, 3084, 3085, 3086, 3087, 3302, 3303, 3308, 3309, 3310, 3388,
      3389, 3418, 3420, 3421, 3422, 3423, 3470, 3472, 3473, 3564, 3565, 3566,
      3567, 3568, 3569, 3570, 3571, 3572, 3573, 3574, 3575, 3576, 3577, 3578,
      3579, 3580, 3581, 3582, 3583, 3584, 3585, 3586, 3587, 3588, 3589, 3590,
      3591, 3592, 3593, 3594, 3595, 3596, 3597, 3598, 3599, 3600, 3779, 3782,
      3913, 3914, 3915, 4097, 103917, 103918, 103919, 103920, 103921, 103922,
      103923, 103924, 103925, 103926, 103927, 103928, 103929, 103930, 103931,
      103932, 103933, 103934, 103935, 103936, 103937, 103938, 103939, 103940,
      103941, 103942, 103943, 103945, 103946, 103947, 103948, 103949, 103950,
      103951, 103952, 103953, 103954, 103955, 103956, 103957, 103958, 103959,
      103960, 103961, 103962, 103963, 103964, 103965, 103966, 103967, 103968,
      103969, 103970, 103971, 103972, 103973, 103974, 103975, 103976, 103977,
      103978, 103979, 103980, 103981, 103982, 103983, 103984, 103985, 103986,
      103987, 103988, 103989, 103990, 103991, 103992, 104389, 104390, 104391,
      104392, 104393, 104394, 104395, 104396, 104397, 104398, 104399, 104400,
      104401, 104402, 104403, 104404, 104405, 104406, 104407, 104408, 104409,
      104410, 104411, 104412, 104413, 104414, 104415, 104416, 104417, 104418,
      104419, 104420, 104421, 104422, 104423, 104424, 104425, 104426, 104427,
      104428, 104429, 104430, 104431, 104432, 104433, 104434, 104435, 104608,
      104609, 104610, 104611, 104612, 104613, 104614, 104615, 104616, 104617,
      104618, 104619, 104620, 104621, 104622)

    val data_urls = get_data_urls(spark, ndays = 30, since = 1, country = "AR")
      .select("url", "segments")
      .withColumn("segments", explode(col("segments")))
      .filter(
        col("segments")
          .isin(segments: _*)
      )
      .distinct()

    val urls_contextual = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/scrapped_urls.csv")
      .select("url")

    val urls_contextual_processed = processURL(urls_contextual)

    val joint = data_urls
      .join(urls_contextual_processed, Seq("url"), "inner")
      .select("url", "segments")

    joint
      .groupBy("url")
      .agg(collect_list(col("segments")).as("segments"))
      .withColumn("segments", concat_ws(";", col("segments")))
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_url_classifier/gt_contextual")

  }

  def get_dataset_contextual(spark: SparkSession, scrapped_path: String) {

    val stopwords = List(
      "a",
      "aca",
      "ahi",
      "al",
      "algo",
      "alla",
      "ante",
      "antes",
      "aquel",
      "aqui",
      "arriba",
      "asi",
      "atras",
      "aun",
      "aunque",
      "bien",
      "cada",
      "casi",
      "como",
      "con",
      "cual",
      "cuales",
      "cuan",
      "cuando",
      "de",
      "del",
      "demas",
      "desde",
      "donde",
      "en",
      "eres",
      "etc",
      "hasta",
      "me",
      "mientras",
      "muy",
      "para",
      "pero",
      "pues",
      "que",
      "si",
      "siempre",
      "siendo",
      "sin",
      "sino",
      "sobre",
      "su",
      "sus",
      "te",
      "tu",
      "tus",
      "y",
      "ya",
      "yo"
    )

    val title_kws = spark.read
      .format("csv")
      .option("header", "true")
      .load(scrapped_path)
      .filter("title is not null")
      .select("url", "title")
      .withColumn("title", split(col("title"), " "))
      .withColumn("keywords", explode(col("title")))
      .filter(!col("keywords").isin(stopwords: _*))
      .select("url", "keywords")
      .withColumn("count", lit(1))

    title_kws.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/kws_title_contextual")

    keywords_embeddings(
      spark,
      kws_path = "/datascience/custom/kws_title_contextual",
      embeddings_path =
        "/datascience/data_url_classifier/embeddings_title_contextual"
    )

    val path_kws = spark.read
      .format("csv")
      .option("header", "true")
      .load(scrapped_path)
      .select("url")
      .withColumn("url", lower(col("url")))
      .withColumn("url_path", regexp_replace(col("url"), """^[^/]*/""", ""))
      .withColumn("url_keys", split(col("url_path"), "[^a-z0-9]"))
      .withColumn("keywords", explode(col("url_keys")))
      .filter(col("keywords").rlike("[a-z]{2,}"))
      .filter(!col("keywords").isin(stopwords: _*))
      .select("url", "keywords")
      .withColumn("count", lit(1))

    path_kws.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/kws_path_contextual")

    keywords_embeddings(
      spark,
      kws_path = "/datascience/custom/kws_path_contextual",
      embeddings_path =
        "/datascience/data_url_classifier/embeddings_path_contextual"
    )

    title_kws
      .union(path_kws)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/kws_path_title_contextual")

    keywords_embeddings(
      spark,
      kws_path = "/datascience/custom/kws_path_title_contextual",
      embeddings_path =
        "/datascience/data_url_classifier/embeddings_path_title_contextual"
    )

  }

  def get_dataset_contextual_augmented(
      spark: SparkSession,
      scrapped_path: String
  ) {

    val udfLength = udf((xs: Seq[String]) => xs.toList.length * 95 / 100)
    val udfRandom = udf((xs: Seq[String], n: Int) => shuffle(xs.toList).take(n))

    val stopwords = List(
      "a",
      "aca",
      "ahi",
      "al",
      "algo",
      "alguna",
      "alguno",
      "algunos",
      "algunas",
      "alla",
      "ambos",
      "ante",
      "antes",
      "aquel",
      "aquella",
      "aquello",
      "aqui",
      "arriba",
      "asi",
      "atras",
      "aun",
      "aunque",
      "bien",
      "cada",
      "casi",
      "como",
      "con",
      "cual",
      "cuales",
      "cualquier",
      "cualquiera",
      "cuan",
      "cuando",
      "cuanto",
      "cuanta",
      "de",
      "del",
      "demas",
      "desde",
      "donde",
      "el",
      "ella",
      "ello",
      "ellos",
      "ellas",
      "en",
      "eres",
      "esa",
      "ese",
      "eso",
      "esos",
      "esas",
      "esta",
      "este",
      "etc",
      "hasta",
      "la",
      "los",
      "las",
      "me",
      "mi",
      "mia",
      "mientras",
      "muy",
      "nosotras",
      "nosotros",
      "nuestra",
      "nuestro",
      "nuestras",
      "nuestros",
      "otra",
      "otro",
      "para",
      "pero",
      "pues",
      "que",
      "si",
      "siempre",
      "siendo",
      "sin",
      "sino",
      "sobre",
      "sr",
      "sra",
      "sres",
      "sta",
      "su",
      "sus",
      "te",
      "tu",
      "tus",
      "un",
      "una",
      "usted",
      "ustedes",
      "vosotras",
      "vosotros",
      "vuestra",
      "vuestro",
      "vuestras",
      "vuestros",
      "y",
      "ya",
      "yo"
    )

    val title_kws = spark.read
      .format("csv")
      .option("header", "true")
      .load(scrapped_path)
      .filter("title is not null")
      .select("url", "title")
      .withColumn("title", split(col("title"), " "))
      .withColumn("n", udfLength(col("title")))
      .withColumn("title", udfRandom(col("title"), col("n")))
      .withColumn("title", split(col("title"), " "))
      .withColumn("keywords", explode(col("title")))
      .filter(!col("keywords").isin(stopwords: _*))
      .select("url", "keywords")
      .withColumn("count", lit(1))

    title_kws.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/kws_title_augmented_contextual")

    keywords_embeddings(
      spark,
      kws_path = "/datascience/custom/kws_title_augmented_contextual",
      embeddings_path =
        "/datascience/data_url_classifier/embeddings_title_augmented_contextual"
    )

    val path_kws = spark.read
      .format("csv")
      .option("header", "true")
      .load(scrapped_path)
      .select("url")
      .withColumn("url", lower(col("url")))
      .withColumn("url_path", regexp_replace(col("url"), """^[^/]*/""", ""))
      .withColumn("url_keys", split(col("url_path"), "[^a-z0-9]"))
      .withColumn("n", udfLength(col("url_keys")))
      .withColumn("url_keys", udfRandom(col("url_keys"), col("n")))
      .withColumn("keywords", explode(col("url_keys")))
      .filter(col("keywords").rlike("[a-z]{2,}"))
      .filter(!col("keywords").isin(stopwords: _*))
      .select("url", "keywords")
      .withColumn("count", lit(1))

    path_kws.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/kws_path_augmented_contextual")

    keywords_embeddings(
      spark,
      kws_path = "/datascience/custom/kws_path_augmented_contextual",
      embeddings_path =
        "/datascience/data_url_classifier/embeddings_path_augmented_contextual"
    )

    title_kws
      .union(path_kws)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/kws_path_title_augmented_contextual")

    keywords_embeddings(
      spark,
      kws_path = "/datascience/custom/kws_path_title_contextual",
      embeddings_path =
        "/datascience/data_url_classifier/embeddings_path_title_augmented_contextual"
    )

  }

  def get_urls_for_ingester(spark: SparkSession) {

    val replicationFactor = 8

    val df = processURLHTTP(
      spark.read
        .load("/datascience/data_demo/data_urls/day=20191110/")
        .select("url", "country")
    )

    val df_processed = df
      .withColumn(
        "composite_key",
        concat(
          col("url"),
          lit("@"),
          col("country"),
          lit("@"),
          // This last part is a random integer ranging from 0 to replicationFactor
          least(
            floor(rand() * replicationFactor),
            lit(replicationFactor - 1) // just to avoid unlikely edge case
          )
        )
      )
      .groupBy("composite_key")
      .count
      .withColumn("split", split(col("composite_key"), "@"))
      .withColumn("url", col("split")(0))
      .withColumn("country", col("split")(1))
      .groupBy("url", "country")
      .agg(sum(col("count")).as("count"))
      .sort(desc("count"))
      .limit(500000)

    df_processed
      .select("url", "country", "count")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .partitionBy("country")
      .save("/datascience/url_ingester/to_scrap")

  }

  def get_keywords_for_equifax(spark: SparkSession) {

    val replicationFactor = 8

    val nids = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'AR' and nid_sh2 is not null")
      .select("device_id", "nid_sh2")
      .distinct()
    nids.cache()

    val keywords_diciembre = spark.read
      .load("/datascience/data_keywords/day=201912*/country=AR/")
      .select("device_id", "content_keys")
      .distinct()

    val keywords_enero = spark.read
      .load("/datascience/data_keywords/day=202001*/country=AR/")
      .select("device_id", "content_keys")
      .distinct()

    nids
      .join(keywords_enero, Seq("device_id"), "inner")
      .select("nid_sh2", "content_keys")
      .groupBy("nid_sh2")
      .agg(collect_list(col("content_keys")).as("keywords"))
      .withColumn("keywords", concat_ws(";", col("keywords")))
      .select("nid_sh2", "keywords")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/kws_equifax_enero")

  }

  def get_mails_mobs_equifax(spark: SparkSession) {
    //  val mails = spark.read.load("/datascience/pii_matching/pii_tuples/")
    //                       .filter("country = 'AR' and ml_sh2 is not null")
    //                       .select("device_id","ml_sh2")
    //                       .withColumnRenamed("ml_sh2","pii")
    //                       .withColumn("type",lit("ml"))
    //                       .distinct()

    // val mobiles = spark.read.load("/datascience/pii_matching/pii_tuples/")
    //                       .filter("country = 'AR' and mb_sh2 is not null")
    //                       .select("device_id","mb_sh2")
    //                       .withColumnRenamed("mb_sh2","pii")
    //                       .withColumn("type",lit("mb"))
    //                       .distinct()

    //   val piis = mails.union(mobiles)

    //   val equifax_pii = spark.read.format("csv").option("header","true")
    //                               .load("/datascience/custom/match_eml_cel.csv")
    //                               .select("atributo_hash")
    //                               .withColumnRenamed("atributo_hash","pii")
    //                               .distinct()

    //   piis.join(equifax_pii,Seq("pii"),"inner").write.format("parquet").save("/datascience/custom/piis_equifax_chkpt")

    val joint = spark.read.load("/datascience/custom/piis_equifax_chkpt")
    joint.cache()

    val keywords_oct = spark.read
      .load("/datascience/data_keywords/day=201910*/country=AR/")
      .select("device_id", "content_keys")
      .distinct()

    joint
      .join(keywords_oct, Seq("device_id"), "inner")
      .select("pii", "content_keys")
      .groupBy("pii")
      .agg(collect_list(col("content_keys")).as("keywords"))
      .withColumn("keywords", concat_ws(";", col("keywords")))
      .select("pii", "keywords")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/ml_mb_kws_equifax_octubre")

    val keywords_nov = spark.read
      .load("/datascience/data_keywords/day=201911*/country=AR/")
      .select("device_id", "content_keys")
      .distinct()

    joint
      .join(keywords_nov, Seq("device_id"), "inner")
      .select("pii", "content_keys")
      .groupBy("pii")
      .agg(collect_list(col("content_keys")).as("keywords"))
      .withColumn("keywords", concat_ws(";", col("keywords")))
      .select("pii", "keywords")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/ml_mb_kws_equifax_noviembre")
  }

  def get_data_BR_matching(spark: SparkSession) {
    spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'BR' and nid_sh2 is not null")
      .select("nid_sh2")
      .distinct()
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/nids_BR")

    spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'BR' and ml_sh2 is not null")
      .select("ml_sh2")
      .distinct()
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/ml_BR")

  }

  def get_data_dani(spark: SparkSession) {

    val days = List("01", "02", "03", "04", "05")
    for (day <- days) {
      spark.read
        .format("csv")
        .option("sep", "\t")
        .option("header", "true")
        .load("/data/eventqueue/%s/*.tsv.gz".format("2019/12/%s".format(day)))
        .filter("id_partner = 879 and device_id is not null")
        .select(
          "time",
          "id_partner",
          "device_id",
          "campaign_id",
          "campaign_name",
          "segments",
          "device_type",
          "country",
          "data_type",
          "nid_sh2"
        )
        .write
        .format("parquet")
        .save("/datascience/custom/sample_dani/day=%s".format(day))

    }

  }

  def process_day_sync(spark: SparkSession, day: String) {
    spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/data/eventqueue/%s/*.tsv.gz".format(day))
      .filter(
        "(event_type = 'sync' or event_type = 'ltm_sync') and (d11 is not null or d13 is not null or d10 is not null or d2 is not null)"
      )
      .select("device_id", "d11", "d2", "d13", "d10", "id_partner")
      .withColumn("day", lit(day))
      .withColumn("day", regexp_replace(col("day"), "/", ""))
      .write
      .format("parquet")
      .partitionBy("day")
      .mode("append")
      .save("/datascience/custom/data_sync")

  }

  def report_dada_sync_v2(spark: SparkSession) {

    val devices = spark.read.load("/datascience/custom/data_sync")

    // Mediamath
    val devices_mm_across = devices
      .filter("d10 is not null and id_partner = 47")
      .select("d10")
      .distinct()
    devices_mm_across.cache()
    println(
      "D10 aportados por across (Total): %s ".format(devices_mm_across.count)
    )

    val count_mm = devices_mm_across
      .join(
        devices.filter("id_partner != 47 and d10 is not null"),
        Seq("d10"),
        "inner"
      )
      .select("d10")
      .distinct()
      .count
    println("D10 aportados por across que ya teniamos: %s ".format(count_mm))

    // DBM
    val devices_dbm_across = devices
      .filter("d11 is not null and id_partner = 47")
      .select("d11")
      .distinct()
    devices_dbm_across.cache()
    println(
      "d11 aportados por across (Total): %s ".format(devices_dbm_across.count)
    )

    val count_dbm = devices_dbm_across
      .join(
        devices.filter("id_partner != 47 and d11 is not null"),
        Seq("d11"),
        "inner"
      )
      .select("d11")
      .distinct()
      .count
    println("d11 aportados por across que ya teniamos: %s ".format(count_dbm))

    // TTD
    val devices_ttd_across = devices
      .filter("d13 is not null and id_partner = 47")
      .select("d13")
      .distinct()
    devices_ttd_across.cache()
    println(
      "d13 aportados por across (Total): %s ".format(devices_ttd_across.count)
    )

    val count_ttd = devices_ttd_across
      .join(
        devices.filter("id_partner != 47 and d13 is not null"),
        Seq("d13"),
        "inner"
      )
      .select("d13")
      .distinct()
      .count
    println("d13 aportados por across que ya teniamos: %s ".format(count_ttd))

    // APPNXS
    val devices_apn_across = devices
      .filter("d2 is not null and id_partner = 47")
      .select("d2")
      .distinct()
    devices_apn_across.cache()
    println(
      "d2 aportados por across (Total): %s ".format(devices_apn_across.count)
    )

    val count_apn = devices_ttd_across
      .join(
        devices.filter("id_partner != 47 and d2 is not null"),
        Seq("d2"),
        "inner"
      )
      .select("d2")
      .distinct()
      .count
    println("d2 aportados por across que ya teniamos: %s ".format(count_apn))

  }

  def report_dada_sync(spark: SparkSession) {

    val devices = spark.read.load("/datascience/custom/data_sync")

    // Mediamath
    val devices_mm_across = devices
      .filter("d10 is not null and id_partner = 47")
      .select("device_id")
      .distinct()
    devices_mm_across.cache()
    println(
      "Devices de MM aportados por across (Total): %s "
        .format(devices_mm_across.count)
    )

    val count_mm = devices_mm_across
      .join(
        devices.filter("id_partner != 47 and d10 is not null"),
        Seq("device_id"),
        "inner"
      )
      .select("device_id")
      .distinct()
      .count
    println(
      "Devices de MM aportados por across que ya teniamos: %s ".format(count_mm)
    )

    // DBM
    val devices_dbm_across = devices
      .filter("d11 is not null and id_partner = 47")
      .select("device_id")
      .distinct()
    devices_dbm_across.cache()
    println(
      "Devices de DBM aportados por across (Total): %s "
        .format(devices_dbm_across.count)
    )

    val count_dbm = devices_dbm_across
      .join(
        devices.filter("id_partner != 47 and d11 is not null"),
        Seq("device_id"),
        "inner"
      )
      .select("device_id")
      .distinct()
      .count
    println(
      "Devices de DBM aportados por across que ya teniamos: %s "
        .format(count_dbm)
    )

    // TTD
    val devices_ttd_across = devices
      .filter("d13 is not null and id_partner = 47")
      .select("device_id")
      .distinct()
    devices_ttd_across.cache()
    println(
      "Devices de TTD aportados por across (Total): %s "
        .format(devices_ttd_across.count)
    )

    val count_ttd = devices_ttd_across
      .join(
        devices.filter("id_partner != 47 and d13 is not null"),
        Seq("device_id"),
        "inner"
      )
      .select("device_id")
      .distinct()
      .count
    println(
      "Devices de TTD aportados por across que ya teniamos: %s "
        .format(count_ttd)
    )

    // APPNXS
    val devices_apn_across = devices
      .filter("d2 is not null and id_partner = 47")
      .select("device_id")
      .distinct()
    devices_apn_across.cache()
    println(
      "Devices de Appnexus aportados por across (Total): %s "
        .format(devices_apn_across.count)
    )

    val count_apn = devices_ttd_across
      .join(
        devices.filter("id_partner != 47 and d2 is not null"),
        Seq("device_id"),
        "inner"
      )
      .select("device_id")
      .distinct()
      .count
    println(
      "Devices de Appnexus aportados por across que ya teniamos: %s "
        .format(count_apn)
    )

  }

  def get_join_kws(spark: SparkSession) {

    val encodeUdf = udf(
      (s: String) => scala.io.Source.fromBytes(s.getBytes(), "UTF-8").mkString
    )

    val df = spark.read
      .option("basePath", "/datascience/data_audiences_streaming/")
      .parquet(
        "/datascience/data_audiences_streaming/hour=%s*".format(20200121)
      ) // We read the data
      .withColumn(
        "url",
        regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
      )
      .withColumn("url", encodeUdf(col("url")))
      .withColumn("url", regexp_replace(col("url"), "'", ""))
      .select("url")
      .distinct()

    val kws = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/selected_keywords/2020-01-21.csv")
      .withColumnRenamed("url_raw", "url")
      .withColumn(
        "url",
        regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
      )
      .select("url")
      .distinct()

    df.join(kws, Seq("url"), "inner")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/join_kws")

  }

  def get_kws_sharethis(spark: SparkSession) {
    spark.read
      .json("/data/providers/sharethis/keywords/")
      .select("url")
      .write
      .format("csv")
      .option("delimiter", "\t")
      .mode("overwrite")
      .save("/datascience/custom/kws_sharethis/")

  }

  def get_piis_bridge(spark: SparkSession) {
    val pii_1 = spark.read
      .format("csv")
      .option("header", "true")
      .load("/data/providers/Bridge/Bridge_Linkage_File_Retargetly_LATAM.csv")
      .withColumnRenamed("SHA256_Email_Hash", "pii")
      .select("pii")
    val pii_2 = spark.read
      .format("csv")
      .option("header", "true")
      .load(
        "/data/providers/Bridge/Bridge_Linkage_File_Retargetly_LATAM_Historical.csv"
      )
      .withColumnRenamed("SHA256_Email_Hash", "pii")
      .select("pii")
    val pii_3 = spark.read
      .format("csv")
      .option("header", "true")
      .load(
        "/data/providers/Bridge/Retargetly_Bridge_Linkage_LATAM_01_2020.csv"
      )
      .withColumnRenamed("SHA256_Email_Hash", "pii")
      .select("pii")
    val pii_4 = spark.read
      .format("csv")
      .option("header", "true")
      .load(
        "/data/providers/Bridge/Retargetly_Bridge_Linkage_LATAM_11_2019.csv"
      )
      .withColumnRenamed("SHA256_Email_Hash", "pii")
      .select("pii")
    val pii_5 = spark.read
      .format("csv")
      .option("header", "true")
      .load(
        "/data/providers/Bridge/Retargetly_Bridge_Linkage_LATAM_12_2019.csv"
      )
      .withColumnRenamed("SHA256_Email_Hash", "pii")
      .select("pii")

    pii_1
      .unionAll(pii_2)
      .unionAll(pii_3)
      .unionAll(pii_4)
      .unionAll(pii_5)
      .select("pii")
      .distinct()
      .write
      .format("csv")
      .save("/datascience/custom/piis_bridge")

  }

  val udfGet = udf(
    (segments: Seq[Row], pos: Int) =>
      segments.map(record => record(pos).toString)
  )

  def get_dataset_sharethis_kws(spark: SparkSession) {
    val kws_scrapper = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/kws_sharethis_scrapper_20200130.csv")
      .withColumnRenamed("url_raw", "url")
      .withColumnRenamed("kw", "kw_scrapper")
      .select("url", "kw_scrapper")

    val kws_sharethis = spark.read
      .json("/data/providers/sharethis/keywords/")
      .na
      .drop()
      .withColumnRenamed("keywords", "kws_sharethis")
      .withColumn("kws_sharethis", udfGet(col("kws_sharethis"), lit(2)))
      .withColumn("concepts", udfGet(col("concepts"), lit(1)))
      .withColumn("entities", udfGet(col("entities"), lit(3)))
      .withColumn("concepts", concat_ws(";", col("concepts")))
      .withColumn("kws_sharethis", concat_ws(";", col("kws_sharethis")))
      .withColumn("entities", concat_ws(";", col("entities")))
      .select("url", "kws_sharethis", "entities", "concepts")

    kws_sharethis
      .join(kws_scrapper, Seq("url"), "inner")
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/dataset_kws_sharethis")
  }

  def email_to_madid(spark: SparkSession) {
    val piis = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'CL' and ml_sh2 is not null")
      .select("device_id", "ml_sh2")
      .distinct()

    val crossdevice = spark.read
      .format("csv")
      .load("/datascience/audiences/crossdeviced/cookies_cl_xd")
      .withColumnRenamed("_c0", "device_id")

    val count = piis
      .join(crossdevice, Seq("device_id"), "inner")
      .select("ml_sh2")
      .distinct
      .count()

    println("Count ml unique: %s".format(count))
  }

  def report_33across(spark: SparkSession) {

    spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/datascience/33accross/20200*/*/*views-ar-*.gz")
      .groupBy("PAGE_URL")
      .agg(approx_count_distinct(col("COOKIE"), 0.01).as("devices"))
      .sort(desc("devices"))
      .write
      .format("csv")
      .save("/datascience/custom/urls_ar_across")

    val grouped_domain_ar = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/datascience/33accross/20200*/*/*views-ar-*.gz")
      .selectExpr("*", "parse_url(%s, 'HOST') as domain".format("PAGE_URL"))
      .groupBy("domain")
      .agg(approx_count_distinct(col("COOKIE"), 0.01).as("devices"))
      .sort(desc("devices"))

    println("Top Domains AR")
    grouped_domain_ar.show(15)

    val grouped_category_ar = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/datascience/33accross/20200*/*/*views-ar-*.gz")
      .groupBy("PAGE_CATEGORY")
      .agg(approx_count_distinct(col("COOKIE"), 0.01).as("devices"))
      .sort(desc("devices"))

    println("Top Categories AR")
    grouped_category_ar.show(15)

    spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/datascience/33accross/20200*/*/*views-mx-*.gz")
      .groupBy("PAGE_URL")
      .agg(approx_count_distinct(col("COOKIE"), 0.01).as("devices"))
      .sort(desc("devices"))
      .write
      .format("csv")
      .save("/datascience/custom/urls_mx_across")

    val grouped_domain_mx = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/datascience/33accross/20200*/*/*views-mx-*.gz")
      .selectExpr("*", "parse_url(%s, 'HOST') as domain".format("PAGE_URL"))
      .groupBy("domain")
      .agg(approx_count_distinct(col("COOKIE"), 0.01).as("devices"))
      .sort(desc("devices"))

    println("Top Domains MX")
    grouped_domain_mx.show(15)

    val grouped_category_mx = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/datascience/33accross/20200*/*/*views-mx-*.gz")
      .groupBy("PAGE_CATEGORY")
      .agg(approx_count_distinct(col("COOKIE"), 0.01).as("devices"))
      .sort(desc("devices"))

    println("Top Categories MX")
    grouped_category_mx.show(15)
  }

  def report_sharethis(spark: SparkSession) {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(0)
    val days = (0 until 15).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_partner_streaming/"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .flatMap(
        day =>
          (0 until 24).map(
            hour => path + "hour=%s%02d/id_partner=%s".format(day, hour, 411)
          )
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select("url", "device_id", "country")

    df.filter("country = 'AR'")
      .groupBy("url")
      .agg(approx_count_distinct(col("device_id"), 0.01).as("devices"))
      .sort(desc("devices"))
      .write
      .format("csv")
      .save("/datascience/custom/urls_ar_st")

    val grouped_domain_ar = df
      .filter("country = 'AR'")
      .selectExpr("*", "parse_url(%s, 'HOST') as domain".format("url"))
      .groupBy("domain")
      .agg(approx_count_distinct(col("device_id"), 0.01).as("devices"))
      .sort(desc("devices"))

    println("Top Domains AR")
    grouped_domain_ar.show(15)

    // MX
    df.filter("country = 'MX'")
      .groupBy("url")
      .agg(approx_count_distinct(col("device_id"), 0.01).as("devices"))
      .sort(desc("devices"))
      .write
      .format("csv")
      .save("/datascience/custom/urls_mx_st")

    val grouped_domain_mx = df
      .filter("country = 'MX'")
      .selectExpr("*", "parse_url(%s, 'HOST') as domain".format("url"))
      .groupBy("domain")
      .agg(approx_count_distinct(col("device_id"), 0.01).as("devices"))
      .sort(desc("devices"))

    println("Top Domains MX")
    grouped_domain_mx.show(15)

  }

  def report_bridge(spark: SparkSession) {

    val bridge = spark.read
      .format("csv")
      .option("header", "true")
      .load("/data/tmp/Bridge_Linkage_File_Retargetly_LATAM_ALL.csv")
      .filter("country = 'ar'")
      .withColumnRenamed("advertising_id", "device_id")
      .withColumn("device_id", lower(col("device_id")))

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(0)
    val days = (0 until 30).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_partner_streaming/"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files_factual = days
      .flatMap(
        day =>
          (0 until 24).map(
            hour => path + "hour=%s%02d/id_partner=%s".format(day, hour, 1008)
          )
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val factual = spark.read
      .option("basePath", path)
      .parquet(hdfs_files_factual: _*)
      .filter("country = 'AR'")
      .withColumn("device_id", lower(col("device_id")))
      .select("device_id")

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files_startapp = days
      .flatMap(
        day =>
          (0 until 24).map(
            hour => path + "hour=%s%02d/id_partner=%s".format(day, hour, 1139)
          )
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val startapp = spark.read
      .option("basePath", path)
      .parquet(hdfs_files_startapp: _*)
      .filter("country = 'AR'")
      .withColumn("device_id", lower(col("device_id")))
      .select("device_id")

    val geo = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load(
        "/datascience/geo/NSEHomes/argentina_365d_home_21-1-2020-12h_xd_push"
      )
      .withColumn("device_id", lower(col("device_id")))

    // val join_factual = bridge.join(factual,Seq("device_id"),"inner")
    // println("Factual Devices:")
    // println(join_factual.select("device_id").distinct().count())
    // println("Factual Emails:")
    // println(join_factual.select("email_sha256").distinct().count())
    // bridge.join(factual,Seq("device_id"),"inner")
    //       .write
    //       .format("parquet")
    //       .mode(SaveMode.Overwrite)
    //       .save("/datascience/custom/join_factual")

    // val join_startapp = bridge.join(startapp,Seq("device_id"),"inner")
    // println("Startapp Devices:")
    // println(join_startapp.select("device_id").distinct().count())
    // println("Startapp Emails:")
    // println(join_startapp.select("email_sha256").distinct().count())
    bridge
      .join(startapp, Seq("device_id"), "inner")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/join_startapp")

    bridge
      .join(geo, Seq("device_id"), "inner")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/join_geo")

  }

  def report_gcba(spark: SparkSession) {

    val gcba = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/devices_gcba.csv")
      .withColumnRenamed("Device Id", "device_id")
      .withColumn("device_id", lower(col("device_id")))
      .select("device_id")
      .distinct

    val bridge = spark.read
      .format("csv")
      .option("header", "true")
      .load("/data/tmp/Bridge_Linkage_File_Retargetly_LATAM_ALL.csv")
      .filter("country = 'ar'")
      .withColumnRenamed("advertising_id", "device_id")
      .withColumn("device_id", lower(col("device_id")))

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(0)
    val days = (0 until 30).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_partner_streaming/"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files_factual = days
      .flatMap(
        day =>
          (0 until 24).map(
            hour => path + "hour=%s%02d/id_partner=%s".format(day, hour, 1008)
          )
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val factual = spark.read
      .option("basePath", path)
      .parquet(hdfs_files_factual: _*)
      .filter("country = 'AR'")
      .withColumn("device_id", lower(col("device_id")))
      .select("device_id")

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files_startapp = days
      .flatMap(
        day =>
          (0 until 24).map(
            hour => path + "hour=%s%02d/id_partner=%s".format(day, hour, 1139)
          )
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val startapp = spark.read
      .option("basePath", path)
      .parquet(hdfs_files_startapp: _*)
      .filter("country = 'AR'")
      .withColumn("device_id", lower(col("device_id")))
      .select("device_id")

    val geo = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load(
        "/datascience/geo/NSEHomes/argentina_365d_home_21-1-2020-12h_xd_push"
      )
      .withColumn("device_id", lower(col("device_id")))

    // val join_bridge = gcba.join(bridge,Seq("device_id"),"inner")

    // val join_factual = gcba.join(factual,Seq("device_id"),"inner")
    // val join_startapp = gcba.join(startapp,Seq("device_id"),"inner")
    // val join_geo = gcba.join(geo,Seq("device_id"),"inner")

    val hdfs_files_gcba = days
      .flatMap(
        day =>
          (0 until 24).map(
            hour => path + "hour=%s%02d/id_partner=%s".format(day, hour, 349)
          )
      )
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val gcba_first = spark.read
      .option("basePath", path)
      .parquet(hdfs_files_factual: _*)
      .filter("country = 'AR'")
      .withColumn("device_id", lower(col("device_id")))
      .select("device_id")

    println("Join Final Devices:")
    //println(gcba.join(factual,Seq("device_id"),"inner").join(startapp,Seq("device_id"),"inner").join(geo,Seq("device_id"),"inner").join(bridge,Seq("device_id"),"inner").join(gcba_first,Seq("device_id"),"inner").select("device_id").distinct().count())
    println("GCBA First Devices:")
    println(
      gcba
        .join(gcba_first, Seq("device_id"), "inner")
        .select("device_id")
        .distinct()
        .count()
    )

  }

  def analisis_domains(spark: SparkSession) {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    var start = DateTime.now.minusDays(11 + 15)
    var end = DateTime.now.minusDays(11)
    var daysCount = Days.daysBetween(start, end).getDays()
    var days =
      (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))
    val path = "/datascience/data_keywords"

    // Now we obtain the list of hdfs folders to be read
    var hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, "AR")) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val data_new = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .groupBy("content_keys") //groupBy("domain")
      .agg(approx_count_distinct(col("device_id"), 0.03).as("devices_new"))

    start = DateTime.now.minusDays(31 + 15)
    end = DateTime.now.minusDays(31)
    daysCount = Days.daysBetween(start, end).getDays()
    days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

    hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, "AR")) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val data_old = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .groupBy("content_keys") //groupBy("domain")
      .agg(approx_count_distinct(col("device_id"), 0.03).as("devices_old"))

    //data_old.join(data_new,Seq("domain"),"inner")
    data_old
      .join(data_new, Seq("content_keys"), "outer")
      .na
      .fill(0)
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/domains_scrapper_2102_all_kws")

  }

  def matching_detergentes(spark: SparkSession) {
    val detergentes_nid = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/limpiadores_detergentes.csv")
      .filter("device_type = 'nid'")
      .select("device_id")
      .withColumnRenamed("device_id", "nid_sh2")
      .distinct()

    val detergentes_ml = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/limpiadores_detergentes.csv")
      .filter("device_type = 'email'")
      .select("device_id")
      .withColumnRenamed("device_id", "ml_sh2")
      .distinct()

    val detergentes_mob = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/limpiadores_detergentes.csv")
      .filter("device_type = 'phone'")
      .select("device_id")
      .withColumnRenamed("device_id", "mb_sh2")
      .distinct()

    val nids = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'AR' and nid_sh2 is not null")
      .select("nid_sh2")
      .distinct()

    val mob = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'AR' and mb_sh2 is not null")
      .select("mb_sh2")
      .distinct()

    val mls = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'AR' and ml_sh2 is not null")
      .select("ml_sh2")
      .distinct()

//    println("Devices Nid:")
    //   println(detergentes_nid.join(nids,Seq("nid_sh2"),"inner").select("nid_sh2").distinct().count())

    println("Devices Mail:")
    println(
      detergentes_ml
        .join(mls, Seq("ml_sh2"), "inner")
        .select("ml_sh2")
        .distinct()
        .count()
    )

    println("Devices Phone:")
    println(
      detergentes_mob
        .join(mob, Seq("mb_sh2"), "inner")
        .select("mb_sh2")
        .distinct()
        .count()
    )

  }

  def enrichment_target_data(spark: SparkSession) {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val since = 1
    val ndays = 30
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(since)
    val days = (0 until ndays).map(start.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_triplets/segments/"
    val dfs = days
      .map(day => path + "day=%s/".format(day) + "country=BR")
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      .map(
        x =>
          spark.read
            .option("basePath", "/datascience/data_triplets/segments/")
            .parquet(x)
            .select("device_id", "feature")
      )

    val segments = List(463, 105154, 105155, 105156, 32, 103977)

    val triplets = dfs
      .reduce((df1, df2) => df1.union(df2))
      .filter(col("feature").isin(segments: _*))
      .select("device_id", "feature")

    val mails = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'BR' and ml_sh2 is not  null")
      .select("device_id", "ml_sh2")
      .withColumnRenamed("ml_sh2", "pii")

    val nids = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'BR' and nid_sh2 is not null")
      .select("device_id", "nid_sh2")
      .withColumnRenamed("nid_sh2", "pii")

    val piis = mails.union(nids)

    val joint = piis
      .join(triplets, Seq("device_id"), "inner")
      .groupBy("pii")
      .agg(collect_list(col("feature")).as("feature"))
      .withColumn("feature", concat_ws(";", col("feature")))
      .select("pii", "feature")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/enrichment_target_data")

  }

  def report_havas(spark: SparkSession) {

    val detergentes_nid = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/limpiadores_detergentes.csv")
      .filter("device_type = 'nid'")
      .select("device_id")
      .withColumnRenamed("device_id", "nid_sh2")
      .distinct()

    val detergentes_ml = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/limpiadores_detergentes.csv")
      .filter("device_type = 'email'")
      .select("device_id")
      .withColumnRenamed("device_id", "ml_sh2")
      .distinct()

    val detergentes_mob = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/limpiadores_detergentes.csv")
      .filter("device_type = 'phone'")
      .select("device_id")
      .withColumnRenamed("device_id", "mb_sh2")
      .distinct()

    val nids = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'AR' and nid_sh2 is not null")
      .select("device_id", "nid_sh2")
      .distinct()

    val mob = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'AR' and mb_sh2 is not null")
      .select("device_id", "mb_sh2")
      .distinct()

    val mls = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'AR' and ml_sh2 is not null")
      .select("device_id", "ml_sh2")
      .distinct()

    // Get pii data <device_id, pii>
    val join_nids = detergentes_nid
      .join(nids, Seq("nid_sh2"), "inner")
      .select("device_id", "nid_sh2")
      .withColumnRenamed("nid_sh2", "pii")
    val join_ml = detergentes_ml
      .join(mls, Seq("ml_sh2"), "inner")
      .select("device_id", "ml_sh2")
      .withColumnRenamed("ml_sh2", "pii")
    val join_mob = detergentes_mob
      .join(mob, Seq("mb_sh2"), "inner")
      .select("device_id", "mb_sh2")
      .withColumnRenamed("mb_sh2", "pii")

    val piis = join_nids
      .union(join_ml)
      .union(join_mob)
      .select("device_id")
      .distinct()

    println(piis.count())

    // Get Triplets data
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val since = 1
    val ndays = 30
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(since)
    val days = (0 until ndays).map(start.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_triplets/segments/"
    val dfs = days
      .map(day => path + "day=%s/".format(day) + "country=AR")
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      .map(
        x =>
          spark.read
            .option("basePath", "/datascience/data_triplets/segments/")
            .parquet(x)
            .select("device_id", "feature")
      )

    val segments = List(129, 61, 141, 302, 144, 2, 3, 4, 5, 6, 7, 8, 9, 352,
      35360, 35361, 35362, 35363, 20107, 20108, 20109, 20110, 20111, 20112,
      20113, 20114, 20115, 20116, 20117, 20118, 20119, 20120, 20121, 20122,
      20123, 20124, 20125, 20126)

    val triplets = dfs
      .reduce((df1, df2) => df1.union(df2))
      .filter(col("feature").isin(segments: _*))
      .select("device_id", "feature")
      .distinct()

    triplets
      .join(piis, Seq("device_id"), "inner")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/report_havas")
  }

  def report_tapad_madids(spark: SparkSession) {

    val madids_factual = spark.read
      .format("csv")
      .option("sep", "\t")
      .load(
        "/datascience/devicer/processed/madids_factual/part-00000-3ac5df52-df3c-4bba-b64c-997007ce486d-c000.csv"
      )
      .withColumnRenamed("_c1", "madids")
      .select("madids")
    val madids_startapp = spark.read
      .format("csv")
      .option("sep", "\t")
      .load(
        "/datascience/devicer/processed/madids_startapp/part-00000-d1ed18a6-48a5-4e68-bb5a-a5c303704f45-c000.csv"
      )
      .withColumnRenamed("_c1", "madids")
      .select("madids")
    // GEO
    val madids_geo_ar = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .load("/datascience/geo/NSEHomes/argentina_365d_home_21-1-2020-12h")
      .withColumnRenamed("_c0", "madids")
      .select("madids")

    val madids_geo_mx = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .load("/datascience/geo/NSEHomes/mexico_200d_home_29-1-2020-12h")
      .withColumnRenamed("_c0", "madids")
      .select("madids")

    val madids_geo_cl = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .load("/datascience/geo/NSEHomes/CL_90d_home_29-1-2020-12h")
      .withColumnRenamed("_c0", "madids")
      .select("madids")

    val madids_geo_co = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .load("/datascience/geo/NSEHomes/CO_90d_home_18-2-2020-12h")
      .withColumnRenamed("_c0", "madids")
      .select("madids")

    madids_factual
      .union(madids_startapp)
      .union(madids_geo_ar)
      .union(madids_geo_mx)
      .union(madids_geo_cl)
      .union(madids_geo_co)
      .withColumn("madids", lower(col("madids")))
      .distinct
      .write
      .format("csv")
      .save("/datascience/custom/tapad_madids")

  }
  def report_tapad_bridge(spark: SparkSession) {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val path = "/data/providers/Bridge/"
    val dfs = fs
      .listStatus(new Path(path))
      .map(
        x =>
          spark.read
            .format("csv")
            .option("header", "true")
            .load(path + x.getPath.toString.split("/").last)
      )
      .toList

    val df_union = dfs
      .reduce((df1, df2) => df1.unionAll(df2))
      .select("Timestamp", "IP_Address", "Device_ID", "Device_Type")

    df_union.write.format("csv").save("/datascience/custom/report_tapad_bridge")

  }
  def dummy_havas(spark: SparkSession) {

    val udfFeature = udf((r: Double) => if (r > 0.5) 34316 else 34323)

    val detergentes_nid = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/limpiadores_detergentes.csv")
      .filter("device_type = 'nid'")
      .select("device_id")
      .withColumnRenamed("device_id", "nid_sh2")
      .distinct()

    val detergentes_ml = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/limpiadores_detergentes.csv")
      .filter("device_type = 'email'")
      .select("device_id")
      .withColumnRenamed("device_id", "ml_sh2")
      .distinct()

    val detergentes_mob = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/custom/limpiadores_detergentes.csv")
      .filter("device_type = 'phone'")
      .select("device_id")
      .withColumnRenamed("device_id", "mb_sh2")
      .distinct()

    val nids = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'AR' and nid_sh2 is not null")
      .select("device_id", "nid_sh2")
      .distinct()

    val mob = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'AR' and mb_sh2 is not null")
      .select("device_id", "mb_sh2")
      .distinct()

    val mls = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'AR' and ml_sh2 is not null")
      .select("device_id", "ml_sh2")
      .distinct()

    // Get pii data <device_id, pii>
    val join_nids = detergentes_nid
      .join(nids, Seq("nid_sh2"), "inner")
      .select("device_id", "nid_sh2")
      .withColumnRenamed("nid_sh2", "pii")
    val join_ml = detergentes_ml
      .join(mls, Seq("ml_sh2"), "inner")
      .select("device_id", "ml_sh2")
      .withColumnRenamed("ml_sh2", "pii")
    val join_mob = detergentes_mob
      .join(mob, Seq("mb_sh2"), "inner")
      .select("device_id", "mb_sh2")
      .withColumnRenamed("mb_sh2", "pii")

    val piis = join_nids
      .union(join_ml)
      .union(join_mob)
      .select("device_id")
      .distinct()
      .withColumn("rand", rand())
      .withColumn("feature", udfFeature(col("rand")))
      .withColumn("id_partner", lit(119))
      .withColumn("device_type", lit("web"))
      .withColumn("activable", lit(1))
      .select("device_id", "feature", "id_partner", "device_type", "activable")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/dummy_havas")

  }
  def get_piis_cl(spark: SparkSession) {
    val xd = spark.read
      .format("csv")
      .option("sep", ",")
      .load("/datascience/audiences/crossdeviced/CL_90d_home_29-1-2020-12h_xd")
      .withColumnRenamed("_c0", "madid")
      .withColumnRenamed("_c1", "device_id")
      .withColumnRenamed("_c7", "lat")
      .withColumnRenamed("_c8", "lon")
      .select("madid", "lat", "lon", "device_id")

    val pii = spark.read.load("/datascience/pii_matching/pii_tuples/")

    xd.join(pii, Seq("device_id"), "inner")
      .select("madid", "lat", "lon", "ml_sh2", "nid_sh2", "mb_sh2")
      .repartition(50)
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/piis_madids_cl")

  }

  def report_user_uniques(spark: SparkSession) {
    val segments = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/misc/taxo_gral.csv")
      .select("seg_id")
      .collect()
      .map(_(0).toString.toInt)
      .toSeq

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(0)
    val days = (0 until 10).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_triplets/segments"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=AR".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val tierUDF = udf(
      (count: Int) =>
        if (count <= 50000) "1"
        else if (count >= 500000) "3"
        else "2"
    )

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select("feature", "count")
      .withColumnRenamed("feature", "segment")
      .filter(col("segment").isin(segments: _*))
      .groupBy("segment")
      .agg(sum(col("count")).as("count"))
      .withColumn("tier", tierUDF(col("count")))
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .partitionBy("tier")
      .save("/datascience/custom/users_tiers")
  }

  def generate_users_for_report_uniques(spark: SparkSession) {
    // 5 segments from each tier
    val segments = List(
      "24621",
      "24666",
      "24692",
      "1350",
      "743",
      "224",
      "104619",
      "99638",
      "48334",
      "432",
      "1087",
      "1341",
      "99644",
      "48398",
      "463"
    )
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(0)
    val days = (0 until 10).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_triplets/segments"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=AR".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val udfTier = udf(
      (segment: String) =>
        if (List("24621", "24666", "24692", "1350", "743").contains(segment))
          "tier_1"
        else if (List("224", "104619", "99638", "48334", "432")
                   .contains(segment)) "tier_2"
        else "tier_3"
    )
    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select("device_id", "feature", "count")
      .withColumnRenamed("feature", "segment")
      .filter(col("segment").isin(segments: _*))
      .withColumn("tier", udfTier(col("segment")))
      .select("device_id", "segment", "tier")
      .distinct()
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/users_report_uniques")
  }
  def get_pii_matching_user_report(spark: SparkSession) {
    val pii = spark.read.load("/datascience/pii_matching/pii_tuples/")
    val df = spark.read
      .format("csv")
      .load("/datascience/custom/users_report_uniques")
      .withColumnRenamed("_c0", "device_id")
      .withColumnRenamed("_c1", "segment")
      .withColumnRenamed("_c2", "tier")

    pii
      .join(df, Seq("device_id"), "inner")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/report_user_unique_pii")

  }
  def pedido_bri_tu(spark: SparkSession) {
    val df = spark.read
      .format("csv")
      .option("sep", "\t")
      .load("/datascience/devicer/processed/piis_bri_transunion_grouped")
      .withColumnRenamed("_c1", "device_id")
      .withColumnRenamed("_c2", "segment")
      .select("device_id", "segment")

    val pii = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("ml_sh2 is not null")

    pii
      .join(df, Seq("device_id"), "inner")
      .select("device_id", "ml_sh2", "segment")
      .repartition(1)
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/mails_tu_bri")
  }

  def temp(spark: SparkSession) {
    val nids = spark.read
      .load("/datascience/custom/report_user_unique_pii")
      .select("nid_sh2", "tier")
      .distinct
    val pii = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .select("device_id", "nid_sh2")
      .distinct

    nids
      .join(pii, Seq("nid_sh2"), "inner")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/devices_originales")

  }
  def reprocess_dumps(spark: SparkSession) {
    val files = List(
      "2020-02-03_daily",
      "2020-02-04_daily",
      "2020-02-05_daily",
      "2020-02-06_daily",
      "2020-02-07_daily",
      "2020-02-08_daily",
      "2020-02-09_daily",
      "2020-02-10_daily"
    )
    val udfParse = udf((meta: String, field: String) => {
      def get_field(meta: String, field: String): String = {
        var res = ""
        try {
          val json_parsed = JSON.parseFull(
            meta
              .replace("defaultdict(<class 'dict'>, ", "")
              .replace(")", "")
              .replace("'", "\"")
          )
          if (json_parsed != None) {
            res = json_parsed.get.asInstanceOf[Map[String, Any]](field).toString
          }
        } catch {
          case e: Throwable => {
            println(e)
            res = ""

          }
        }

        res
      }
      get_field(meta, field)
    })
    val hour = DateTime.now().getHourOfDay()
    var date = ""
    for (f <- files) {
      date = f.split("_")(0).replace("-", "")
      println(date)
      spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", "\t")
        .load("/datascience/scraper/dump/%s.csv".format(f))
        .dropDuplicates()
        .withColumn(
          "description",
          udfParse(col("meta_data"), lit("description"))
        )
        .withColumn("keywords", udfParse(col("meta_data"), lit("keywords")))
        .withColumn(
          "og_description",
          udfParse(col("meta_data"), lit("og_description"))
        )
        .withColumn("og_title", udfParse(col("meta_data"), lit("og_title")))
        .withColumn(
          "twitter_description",
          udfParse(col("meta_data"), lit("twitter_description"))
        )
        .withColumn(
          "twitter_title",
          udfParse(col("meta_data"), lit("twitter_title"))
        )
        .selectExpr("*", "parse_url(url, 'HOST') as domain")
        .withColumn("day", lit(date))
        .withColumn("hour", lit(hour))
        .orderBy(col("url").asc)
        .select(
          "url",
          "title",
          "text",
          "description",
          "keywords",
          "og_description",
          "og_title",
          "twitter_description",
          "twitter_title",
          "timestamp",
          "domain",
          "hour",
          "day"
        )
        .write
        .format("parquet")
        .mode("append")
        .partitionBy("day", "hour")
        .save("/datascience/scraper/parsed/processed")
    }

  }

  def licensing_publicis(spark: SparkSession) {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(0)
    val days = (0 until 30).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_triplets/segments"

    val segments = List(
      "104014",
      "104015",
      "104016",
      "104017",
      "104018",
      "104019",
      "98946",
      "3013",
      "150",
      "147"
    )

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=MX".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .withColumnRenamed("feature", "segment")
      .filter(col("segment").isin(segments: _*))

    df.groupBy("segment")
      .agg(approx_count_distinct(col("device_id"), 0.03).as("devices"))
      .show()

  }

  def report_dh(spark: SparkSession) {
    val pii = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'BR'")
    val nids = pii.filter("nid_sh2 is not null").select("nid_sh2", "device_id")
    val bridge = spark.read
      .format("csv")
      .option("header", "true")
      .load("/data/tmp/Bridge_Linkage_File_Retargetly_LATAM_ALL.csv")
      .filter("country = 'br'")
      .withColumnRenamed("email_sha256", "ml_sh2")
      .withColumnRenamed("advertising_id", "device_id")
      .select("ml_sh2", "device_id")

    val emails = pii
      .filter("ml_sh2 is not null")
      .select("ml_sh2", "device_id")
      .union(bridge)
    val all_piis = nids
      .withColumnRenamed("nid_sh2", "pii")
      .union(emails.withColumnRenamed("ml_sh2", "pii"))

    val mapeo = Map(
      "241141" -> "3862661122c5864e6b0872554dc76a60",
      "241143" -> "62aafe9b6c0cbe3331381982616fab53",
      "241145" -> "67f6425c910c0a6f5feed81e1094b8be",
      "241147" -> "b1530597217a23c1e76c022ca43261de",
      "271165" -> "4a5594ff767d0a59b660987cd06f0176"
    )
    val files = List("241141", "241143", "241145", "241147", "271165")
    var df = spark.emptyDataFrame
    var local_piis = spark.emptyDataFrame

    for (f <- files) {
      df = spark.read
        .format("csv")
        .option("header", "true")
        .load("/data/jobs/activator/%s".format(mapeo(f)))
        .withColumnRenamed("ds_email_lower", "ml_sh2")
        .withColumnRenamed("nr_cpf", "nid_sh2")

      local_piis = df
        .select("ml_sh2")
        .withColumnRenamed("ml_sh2", "pii")
        .union(df.select("nid_sh2").withColumnRenamed("nid_sh2", "pii"))

      println(f)
      println("Total Lines: %s".format(df.count))
      println(
        "Total Emails uploaded: %s".format(df.select("ml_sh2").distinct.count)
      )
      println(
        "Total Nids uploaded: %s".format(df.select("nid_sh2").distinct.count)
      )
      println(
        "Nids Matched: %s".format(
          df.select("nid_sh2")
            .join(nids, Seq("nid_sh2"), "inner")
            .select("nid_sh2")
            .distinct
            .count
        )
      )
      println(
        "Emails Matched: %s".format(
          df.select("ml_sh2")
            .join(emails, Seq("ml_sh2"), "inner")
            .select("ml_sh2")
            .distinct
            .count
        )
      )
      println(
        "Devices Found: %s".format(
          all_piis
            .join(local_piis, Seq("pii"), "inner")
            .select("device_id")
            .distinct
            .count
        )
      )
      println("\n")

    }
  }

  def report_dh_fede(spark: SparkSession) {
    val pii = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'BR'")
    val nids = pii.filter("nid_sh2 is not null").select("nid_sh2", "device_id","device_type")
    val bridge = spark.read
      .format("csv")
      .option("header", "true")
      .load("/data/tmp/Bridge_Linkage_File_Retargetly_LATAM_ALL.csv")
      .filter("country = 'br'")
      .withColumnRenamed("email_sha256", "ml_sh2")
      .withColumnRenamed("advertising_id", "device_id")
      .withColumnRenamed("platform", "device_type")
      .select("ml_sh2", "device_id","device_type")

    val emails = pii
      .filter("ml_sh2 is not null")
      .select("ml_sh2", "device_id","device_type")
      .union(bridge)
    
    nids
      .withColumnRenamed("nid_sh2", "pii")
      .union(emails.withColumnRenamed("ml_sh2", "pii"))
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/all_piis_dh_fede")

    val all_piis = spark.read.load("/datascience/custom/all_piis_dh_fede")

    val mapeo = Map(
      "241141" -> "3862661122c5864e6b0872554dc76a60",
      "241143" -> "62aafe9b6c0cbe3331381982616fab53",
      "241145" -> "67f6425c910c0a6f5feed81e1094b8be",
      "241147" -> "b1530597217a23c1e76c022ca43261de",
      "271165" -> "4a5594ff767d0a59b660987cd06f0176"
    )
    val files = List("241141", "241143", "241145", "241147", "271165")
    var df = spark.emptyDataFrame
    var local_piis = spark.emptyDataFrame

    for (f <- files) {
      df = spark.read
        .format("csv")
        .option("header", "true")
        .load("/data/jobs/activator/%s".format(mapeo(f)))
        .withColumnRenamed("ds_email_lower", "ml_sh2")
        .withColumnRenamed("nr_cpf", "nid_sh2")

      local_piis = df
        .select("ml_sh2")
        .withColumnRenamed("ml_sh2", "pii")
        .union(df.select("nid_sh2").withColumnRenamed("nid_sh2", "pii"))

      all_piis
          .join(local_piis, Seq("pii"), "inner")
          .select("device_id","device_type")
          .distinct
          .repartition(1)
          .write
          .format("csv")
          .mode(SaveMode.Overwrite)
          .save("/datascience/custom/dh_devices_%s".format(f))        
    }
  }

  def get_numbers(spark: SparkSession) {
    val pii = spark.read
      .load("/datascience/pii_matching/pii_tuples/day=2020*")
      .filter("country = 'BR'")
    val nids = pii.filter("nid_sh2 is not null").select("nid_sh2", "device_id")
    val mobs = pii.filter("mb_sh2 is not null").select("mb_sh2", "device_id")
    val bridge = spark.read
      .format("csv")
      .option("header", "true")
      .load("/data/tmp/Bridge_Linkage_File_Retargetly_LATAM_ALL.csv")
      .filter("country = 'br'")
      .withColumnRenamed("email_sha256", "ml_sh2")
      .withColumnRenamed("advertising_id", "device_id")
      .select("ml_sh2", "device_id")

    val emails = pii.filter("ml_sh2 is not null").select("ml_sh2", "device_id") //.union(bridge)
    val all_piis = nids
      .withColumnRenamed("nid_sh2", "pii")
      .union(emails.withColumnRenamed("ml_sh2", "pii"))

    //println("Mails Uniques: %s".format(emails.select("ml_sh2").distinct.count))
    //println("Nids Uniques: %s".format(nids.select("nid_sh2").distinct.count))
    println("Nids Uniques: %s".format(mobs.select("mb_sh2").distinct.count))

  }
  def dataset_test_idx(spark: SparkSession) {

    val bridge = spark.read
      .format("csv")
      .option("header", "true")
      .load("/data/tmp/Bridge_Linkage_File_Retargetly_LATAM_ALL.csv")
      .filter("country = 'ar'")
      .withColumnRenamed("advertising_id", "device_id_bridge")
      .withColumnRenamed("email_sha256", "ml_sh2")
      .withColumn("device_id_bridge", lower(col("device_id_bridge")))
      .select("device_id_bridge", "ml_sh2")

    val pii_tuples = spark.read
      .load("/datascience/pii_matching/pii_tuples/")
      .filter("country = 'AR' and ml_sh2 is not null")

    bridge
      .join(pii_tuples, Seq("ml_sh2"), "inner")
      .write
      .format("parquet")
      .save("/datascience/custom/test_idx")

  }
  def get_coronavirus(spark: SparkSession,country:String) {

    val windowSpec = Window.partitionBy("ad_id").orderBy("utc_timestamp")

    val raw = spark.read
      .format("parquet")
      .option("basePath", "/datascience/geo/safegraph/")
      .load("/datascience/geo/safegraph/day=202003*/country=%s/".format(country))
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

    val udfFeature = udf((r: Double) => if (r > 0.5) 1 else 0)

    // Select sample of 1000 users
    val initial_seed = spark.read
      .load("/datascience/custom/coronavirus_seed_%s".format(country))
      .select("device_id", "geo_hash", "window")

    // Get the distinct moments to filter the raw data
    val moments = initial_seed.select("geo_hash", "window").distinct()


    // Join raw data with the moments and store
    raw
      .join(moments, Seq("geo_hash", "window"))
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/coronavirus_contacts_%s".format(country))

    val joint = spark.read.load("/datascience/custom/coronavirus_contacts_%s".format(country))

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
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/coronavirus_contacts_per_day_%s".format(country))

  }
  def generate_seed(spark:SparkSession,country:String){
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(0)

    //val days = (0 until 24).map(start.minusDays(_)).map(_.toString(format))
    val days = List("20200331","20200330","20200329","20200328","20200327","20200326","20200325","20200324","20200323","20200322","20200321","20200320","20200319","20200318","20200317","20200316","20200315","20200314","20200313","20200312","20200311","20200310",
                    "20200309","20200308","20200307","20200306","20200305","20200304","20200303","20200302","20200301")
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
            .limit(10000)
      )

    val users = dfs.reduce((df1, df2) => df1.union(df2)).select("device_id").distinct
    
    spark.read.format("parquet").option("basePath", "/datascience/geo/safegraph/").load("/datascience/geo/safegraph/day=202003*/country=%s/".format(country))
          .withColumnRenamed("ad_id", "device_id")
          .withColumn("device_id", lower(col("device_id")))
          .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
          .withColumn("Hour", date_format(col("Time"), "YYYYMMddHH"))
          .withColumn("window", date_format(col("Time"), "mm"))
          .withColumn("window",
            when(col("window") > 40, 3)
              .otherwise(when(col("window") > 20, 2).otherwise(1))
          )
          .withColumn("window", concat(col("Hour"), col("window")))
          .drop("Time")
          .join(users,Seq("device_id"),"inner")
          .select("device_id","geo_hash", "window")
          .distinct
          .write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .save("/datascience/custom/coronavirus_seed_%s".format(country))

  }

  def coronavirus_barrios(spark:SparkSession,country:String, barrios:DataFrame, name:String){
    val udfGeo = udf((d: String) => d.substring(0, 7))
    
    val initial_seed = spark.read
      .load("/datascience/custom/coronavirus_seed_%s".format(country))
      .select("device_id", "geo_hash", "window")
    
    val contacts = spark.read
                        .load("/datascience/custom/coronavirus_contacts_%s".format(country))
                        .withColumn("geo_hash_join",udfGeo(col("geo_hash")))

    val joint = contacts.join(broadcast(barrios),Seq("geo_hash_join"),"inner")

    // Calculate it by day
    val udfDay = udf((d: String) => d.substring(0, 8))

    joint
      .join(
        initial_seed.withColumnRenamed("device_id", "original_id"),
        Seq("geo_hash", "window")
      )
      .withColumn("day", udfDay(col("window")))
      .groupBy("original_id", "day",name,"FNA")
      .agg(collect_set(col("device_id")).as("devices"))
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/coronavirus_contacts_partidos_%s".format(country))

  }

  // Funcion que busca todos los usuarios del pais para normalizar
  def get_users_coronavirus(spark:SparkSession,country:String){
    val days = List("20200323","20200322","20200321","20200320","20200319","20200318","20200317","20200316","20200315","20200314","20200313","20200312","20200311","20200310",
                    "20200309","20200308","20200307","20200306","20200305","20200304","20200303","20200302","20200301")
    val path = "/datascience/geo/safegraph/"
    val dfs = days
      .map(day => spark.read.load(path + "day=%s/".format(day) + "country=%s".format(country)).withColumnRenamed("ad_id", "device_id").withColumn("day",lit(day)).select("device_id","day"))

    dfs.reduce((df1, df2) => df1.union(df2))
                                .select("device_id","day")
                                .groupBy("day")
                                .agg(approx_count_distinct(col("device_id"), 0.02).as("devices_unique"),
                                    count(col("device_id")).as("devices_count"))
                                .write
                                .format("parquet")
                                .mode(SaveMode.Overwrite)
                                .save("/datascience/custom/users_safegraph_coronavirus_%s".format(country))


  }

  def urgente_sebas(spark:SparkSession){
    val pii_tuples = spark.read.load("/datascience/pii_matching/pii_tuples/")
                          .filter("country = 'BR'")

    println("Emails: %s".format(pii_tuples.filter("ml_sh2 is not null")
              .select("ml_sh2")
              .distinct
              .count))

    println("Moblies: %s".format(pii_tuples.filter("mb_sh2 is not null")
              .select("mb_sh2")
              .distinct
              .count))

    println("Nids: %s".format(pii_tuples.filter("nid_sh2 is not null")
              .select("nid_sh2")
              .distinct
              .count))

  }

  def generate_kepler(spark:SparkSession,country:String){
    
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    // Get users from airport and take the oldeset timestamp
    val w = Window.partitionBy(col("device_id")).orderBy(col("timestamp").asc)
    
    spark.read.format("csv")
        .option("header",true)
        .option("delimiter","\t")
        .load("/datascience/geo/raw_output/airportsCO_30d_CO_30-3-2020-16h")
        .withColumn("rn", row_number.over(w))
        .where(col("rn") === 1)
        .drop("rn")
        .withColumnRenamed("timestamp","timestamp_airport")
        .select("device_id","timestamp_airport","audience")
        .withColumn("device_id", lower(col("device_id")))
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .save("/datascience/custom/users_airport_%s".format(country))

    val users_airport = spark.read.load("/datascience/custom/users_airport_%s".format(country))

    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(0)
    val path = "/datascience/geo/safegraph/"
    val days = (0 until 30).map(start.minusDays(_)).map(_.toString(format))

    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    // Get Data Raw and join with users from airport
    val joint = spark.read
                    .option("basePath", path)
                    .parquet(hdfs_files: _*)
                    .withColumnRenamed("ad_id", "device_id")
                    .withColumn("device_id", lower(col("device_id")))
                    .withColumnRenamed("utc_timestamp", "timestamp_raw") 
                    .withColumn(
                      "geohash",
                      ((abs(col("latitude").cast("float")) * 10)
                        .cast("int") * 10000) + (abs(
                        col("longitude").cast("float") * 100
                      ).cast("int"))
                    )
                    .select("device_id","timestamp_raw","geohash","latitude","longitude")
                    .join(users_airport,Seq("device_id"),"inner")
                    .withColumn("timestamp_raw", col("timestamp_raw").cast(IntegerType))
                    .withColumn("timestamp_airport", col("timestamp_airport").cast(IntegerType))
                    .filter("timestamp_raw >= timestamp_airport") // filter timestamp
                    .groupBy("geohash")
                    .agg(first(col("latitude")).as("latitude"),
                        first(col("longitude")).as("longitude"),
                        first(col("timestamp_raw")).as("timestamp"),
                        approx_count_distinct(col("device_id"), 0.02).as("device_unique")
                    )
    joint.repartition(1)
          .write
          .format("csv")
          .option("header","true")
          .mode(SaveMode.Overwrite)
          .save("/datascience/custom/kepler_%s".format(country))
  }

    def get_monthly_data_homes(spark:SparkSession, country:String): DataFrame = {
      val sc = spark.sparkContext
      val conf = sc.hadoopConfiguration
      val fs = org.apache.hadoop.fs.FileSystem.get(conf)
      
      val format = "yyyy-MM"
      val start = DateTime.now.minusDays(0)
      val path = "/datascience/data_insights/homes/"

      val days = (0 until 30).map(start.minusDays(_)).map(_.toString(format))

      val hdfs_files = days
        .map(day => path + "day=%s/country=%s".format(day, country))
        .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

      val data = spark.read
                      .option("basePath", path)
                      .parquet(hdfs_files: _*)
                      .filter("device_type != 'web'") // get only madids
                      .withColumnRenamed("device_id","madid")
                      .select("madid")
      data

  }

  def report_etermax(spark:SparkSession){
    val current_month = DateTime.now().toString("yyyyMM")

     val madids_etermax = spark.read.format("csv")
                              .load("/datascience/data_tapad/madids_etermax.csv")
                              .withColumnRenamed("_c0","madids")
                              .withColumn("madids",lower(col("madids")))

    val madids_factual = spark.read.format("csv").option("sep","\t")
                              .load("/datascience/devicer/processed/madids_factual_%s/".format(current_month))
                              .withColumnRenamed("_c1","madids")
                              .select("madids")

    val madids_startapp = spark.read.format("csv").option("sep","\t")
                              .load("/datascience/devicer/processed/madids_startapp_%s/".format(current_month))
                              .withColumnRenamed("_c1","madids")
                              .select("madids")
    // GEO
    val madids_geo_ar = get_monthly_data_homes(spark,"AR")

    val madids_geo_mx = get_monthly_data_homes(spark,"MX")

    val madids_geo_cl = get_monthly_data_homes(spark,"CL")

    val madids_geo_co = get_monthly_data_homes(spark,"CO")

    val rest = madids_factual.union(madids_startapp)
              .union(madids_geo_ar)
              .union(madids_geo_mx)
              .union(madids_geo_cl)
              .union(madids_geo_co)
              .withColumn("madids",lower(col("madids")))

    println("Devices Unicos Etermax:  %s".format(madids_etermax.select("madids").distinct.count))
    println("Devices Totales Etermax:  %s".format(madids_etermax.select("madids").count))
    println("Devices En comun:  %s".format(madids_etermax.join(rest,Seq("madids"),"inner").select("madids").distinct.count))

  }

  def main(args: Array[String]) {

    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)
 
    val spark = SparkSession.builder
      .appName("Random de Tincho")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()
    
    // generate_seed(spark,"mexico")
    // get_coronavirus(spark,"mexico")
    // get_users_coronavirus(spark,"mexico")
    // val barrios = spark.read
    //                     .format("csv")
    //                     .option("header","true")
    //                     .option("sep","\t")
    //                     .load("/datascience/geo/geo_processed/MX_municipal_mexico_sjoin_polygon")
    //                     .withColumnRenamed("geo_hash_7","geo_hash_join")

      
    // generate_seed(spark,"argentina")
    // get_coronavirus(spark,"argentina")
    //  val barrios =  spark.read
    //                       .format("csv")
    //                       .option("sep","\t")
    //                       .option("header","true")
    //                       .load("/datascience/geo/geo_processed/AR_departamentos_barrios_mexico_sjoin_polygon")
    //                       .withColumnRenamed("geo_hashote","geo_hash_join")
    // coronavirus_barrios(spark,"argentina",barrios,"NAM")
    generate_kepler(spark,"CO")

    //report_etermax(spark)
  }
}
