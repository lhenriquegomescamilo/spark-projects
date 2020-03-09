package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
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
      .option("header","true")
      .load(dfs: _*)
      .filter("country = '%s'".format(country))
      .withColumnRenamed("kw", "kws")
      .withColumnRenamed("url_raw", "url")
      .withColumn("kws", split(col("kws"), " "))
      .select("url","kws")
      .dropDuplicates("url")


    df
  }

  def get_gt_new_taxo(spark: SparkSession, ndays:Int, since:Int, country:String) = {
    
    // Get selected keywords <url, [kws]>
    val selected_keywords = get_selected_keywords(spark, ndays = ndays, since = since, country = country)
    selected_keywords.cache()

    // Get Data urls <url>
    val data_urls = get_data_urls(spark, ndays = ndays, since = since, country = country)
    data_urls.cache()

    // Get queries <seg_id, query (array contains), query (url like)>
    var queries = spark.read.load("/datascience/custom/new_taxo_queries_join")
    
    var dfs: DataFrame = null
    var first = true
    // Iterate queries and get urls
    for (row <- queries.rdd.collect){  
      var segment = row(0).toString
      var query = row(1).toString
      var query_url_like = row(2).toString

      // Filter selected keywords dataframe using query with array contains
      selected_keywords.filter(query)
                        .withColumn("segment",lit(segment))
                        .select("url","segment")
                        .write
                        .format("parquet")
                        .mode("append")
                        .save("/datascience/data_url_classifier/GT_new_taxo_queries")

      // Filter data_urls dataframe using query with array url LIKE                        
      data_urls.filter(query_url_like)
                  .withColumn("segment",lit(segment))
                  .select("url","segment")
                  .write
                  .format("parquet")
                  .mode("append")
                  .save("/datascience/data_url_classifier/GT_new_taxo_queries")
    }

    // Groupby by url and concatenating segments with ;
    spark.read.load("/datascience/data_url_classifier/GT_new_taxo_queries")
          .groupBy("url")
          .agg(collect_list(col("segment")).as("segment"))
          .withColumn("segment", concat_ws(";", col("segment")))
          .write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .save("/datascience/data_url_classifier/GT_new_taxo")
  }


 def get_segments_pmi(spark:SparkSession, ndays:Int, since:Int){

   val files = List("/datascience/misc/cookies_chesterfield.csv",
                 "/datascience/misc/cookies_marlboro.csv",
                 "/datascience/misc/cookies_phillip_morris.csv",
                 "/datascience/misc/cookies_parliament.csv")

   /// Configuraciones de spark
   val sc = spark.sparkContext
   val conf = sc.hadoopConfiguration
   val fs = org.apache.hadoop.fs.FileSystem.get(conf)

   val format = "yyyyMMdd"
   val start = DateTime.now.minusDays(since)

   val days = (0 until ndays).map(start.minusDays(_)).map(_.toString(format))
   val path = "/datascience/data_triplets/segments/"
   val dfs = days.map(day => path + "day=%s/".format(day) + "country=AR")
     .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
     .map(
       x =>
         spark.read
           .option("basePath", "/datascience/data_triplets/segments/")
           .parquet(x)
           .select("device_id","feature")
     )

   var data = dfs.reduce((df1, df2) => df1.union(df2))

   for (filename <- files){
     var cookies = spark.read.format("csv").load(filename)
                                         .withColumnRenamed("_c0","device_id")

     data.join(broadcast(cookies),Seq("device_id"))
         .select("device_id","feature")
         .dropDuplicates()
         .write.format("parquet")
         .mode(SaveMode.Overwrite)
         .save("/datascience/custom/segments_%s".format(filename.split("/").last.split("_").last))
   }
 }
 def test_tokenizer(spark:SparkSession){

    var gtDF = spark.read
                    .load("/datascience/data_url_classifier/gt_new_taxo_filtered")
                    .withColumn("url", lower(col("url")))
                    .withColumn("url_path", regexp_replace(col("url"), """^[^/]*/""", ""))
                    .withColumn("url_keys", split(col("url_path"), "[^a-z0-9]"))
                    .withColumn("keyword", explode(col("url_keys")))
                    .filter(col("keyword").rlike("[a-z]{2,}"))
                    .withColumn("keyword",regexp_replace(col("keyword") ," ", "_"))
                    .withColumn("keyword",regexp_replace(col("keyword") ,"\\(", ""))
                    .withColumn("keyword",regexp_replace(col("keyword") ,"\\)", ""))
                    .withColumn("keyword",regexp_replace(col("keyword") ,",", ""))
                    .groupBy("url","segment").agg(collect_list(col("keyword").as("url_keys")))
                    .withColumn("url_keys", concat_ws(";", col("url_keys")))
                    .write
                    .format("parquet")
                    .mode(SaveMode.Overwrite)
                    .save("/datascience/data_url_classifier/gt_new_taxo_tokenized")
 }

  def get_matching_metrics(spark:SparkSession){

    // Brazil2_101419_FINAL.csv -	dunnhumby/onboarding/GPA-BR.csv.gz
    var partner_br = spark.read.format("csv").option("header","true")
                        .load("/datascience/misc/Brazil2_101419_FINAL.csv")
                        .withColumnRenamed("email_sha256","email")
                        .withColumn("email",lower(col("email")))
                        .select("email")
    partner_br.cache()

    var compared = spark.read.format("csv").option("header","true")
                        .load("/datascience/misc/GPA-BR.csv.gz")
                        .withColumnRenamed("DS_EMAIL_LOWER","email")
                        .withColumn("email",lower(col("email")))
                        .select("email")

    var cant = partner_br.join(compared,Seq("email"),"inner")
                      .select("email")
                      .distinct
                      .count

    println("Brazil2_101419_FINAL.csv -	dunnhumby/onboarding/GPA-BR.csv.gz: %s".format(cant))

    // Brazil2_101419_FINAL.csv	- dunnhumby/onboarding/RD-BR.csv.gz
    compared = spark.read.format("csv").option("header","true")
                        .load("/datascience/misc/RD-BR.csv.gz")
                        .withColumnRenamed("ds_email_lower","email")
                        .withColumn("email",lower(col("email")))
                        .select("email")
                        .na.drop

    cant = partner_br.join(compared,Seq("email"),"inner")
                      .select("email")
                      .distinct
                      .count

    println("Brazil2_101419_FINAL.csv	- dunnhumby/onboarding/RD-BR.csv.gz: %s".format(cant))


    //Brazil2_101419_FINAL.csv	Retargetly
    compared = spark.read.load("/datascience/pii_matching/pii_tuples/")
                      .filter("country = 'BR'")
                      .select("ml_sh2")
                      .withColumnRenamed("ml_sh2","email")
                      .withColumn("email",lower(col("email")))
                      .select("email")

    cant = partner_br.join(compared,Seq("email"),"inner")
                      .select("email")
                      .distinct
                      .count

    println("Brazil2_101419_FINAL.csv	Retargetly: %s".format(cant))


    //Brazil2_101419_FINAL.csv	acxiom/files/acxiom_BR_Partner_Universe_Extract_20190809.tsv.gz
    var df1 = spark.read.format("csv").option("header","true").option("sep","\t").load("/datascience/misc/acxiom_BR_Partner_Universe_Extract_20190809.tsv.gz").select("email_addr_01").withColumnRenamed("email_addr_01","email")
    var df2 = spark.read.format("csv").option("header","true").option("sep","\t").load("/datascience/misc/acxiom_BR_Partner_Universe_Extract_20190809.tsv.gz").select("email_addr_02").withColumnRenamed("email_addr_02","email")
    var df3 = spark.read.format("csv").option("header","true").option("sep","\t").load("/datascience/misc/acxiom_BR_Partner_Universe_Extract_20190809.tsv.gz").select("email_addr_03").withColumnRenamed("email_addr_03","email")
    var df4 = spark.read.format("csv").option("header","true").option("sep","\t").load("/datascience/misc/acxiom_BR_Partner_Universe_Extract_20190809.tsv.gz").select("email_addr_04").withColumnRenamed("email_addr_04","email")
    var df5 = spark.read.format("csv").option("header","true").option("sep","\t").load("/datascience/misc/acxiom_BR_Partner_Universe_Extract_20190809.tsv.gz").select("email_addr_05").withColumnRenamed("email_addr_05","email")
    var df6 = spark.read.format("csv").option("header","true").option("sep","\t").load("/datascience/misc/acxiom_BR_Partner_Universe_Extract_20190809.tsv.gz").select("email_addr_06").withColumnRenamed("email_addr_06","email")

    compared = df1.union(df2)
                  .union(df3)
                  .union(df4)
                  .union(df5)
                  .union(df6)
                  .withColumn("email",lower(col("email")))

    println("acxiom/files/acxiom_BR_Partner_Universe_Extract_20190809.tsv.gz: %s".format(compared.select("email").distinct.count))

    cant = partner_br.join(compared,Seq("email"),"inner")
                      .select("email")
                      .distinct
                      .count

    println("Brazil2_101419_FINAL.csv	acxiom/files/acxiom_BR_Partner_Universe_Extract_20190809.tsv.gz: %s".format(cant))


    // Colombia2_101419_FINAL.csv	dunnhumby/onboarding/Exito-CO.csv.gz
    val partner_co = spark.read.format("csv").option("header","true")
                        .load("/datascience/misc/Colombia2_101419_FINAL.csv")
                        .withColumnRenamed("email_sha256","email")
                        .withColumn("email",lower(col("email")))
                        .select("email")
    partner_co.cache()

    compared = spark.read.format("csv").option("header","true")
                        .load("/datascience/misc/Exito-CO.csv.gz")
                        .withColumnRenamed("distinct_correo","email")
                        .withColumn("email",lower(col("email")))
                        .select("email")
                        .na.drop

    cant = partner_co.join(compared,Seq("email"),"inner")
                      .select("email")
                      .distinct
                      .count

    println("Colombia2_101419_FINAL.csv -	dunnhumby/onboarding/Exito-CO.csv.gz: %s".format(cant))


    // Colombia2_101419_FINAL.csv	Retargetly
    compared = spark.read.load("/datascience/pii_matching/pii_tuples/")
                      .filter("country = 'CO'")
                      .select("ml_sh2")
                      .withColumnRenamed("ml_sh2","email")
                      .withColumn("email",lower(col("email")))
                      .select("email")

    cant = partner_co.join(compared,Seq("email"),"inner")
                  .select("email")
                  .distinct
                  .count

    println("Colombia2_101419_FINAL.csv -	Retargetly: %s".format(cant))

   // Argentina2_101419_FINAL.csv	Retargetly
    val partner_ar = spark.read.format("csv").option("header","true")
                        .load("/datascience/misc/Argentina2_101419_FINAL.csv")
                        .withColumnRenamed("email_sha256","email")
                        .withColumn("email",lower(col("email")))
                        .select("email")
    partner_ar.cache()

    compared = spark.read.load("/datascience/pii_matching/pii_tuples/")
                      .filter("country = 'AR'")
                      .select("ml_sh2")
                      .withColumnRenamed("ml_sh2","email")
                      .withColumn("email",lower(col("email")))

    cant = partner_ar.join(compared,Seq("email"),"inner")
                  .select("email")
                  .distinct
                  .count

    println("Argentina2_101419_FINAL.csv	Retargetly: %s".format(cant))

    // Mexico2_101419_FINAL.csv -	Retargetly
    val partner_mx = spark.read.format("csv")
                        .option("header","true")
                        .load("/datascience/misc/Mexico2_101419_FINAL.csv")
                        .select("email_sha256")
                        .withColumnRenamed("email_sha256","email")
                        .withColumn("email",lower(col("email")))
    partner_mx.cache()

    compared = spark.read.load("/datascience/pii_matching/pii_tuples/")
                        .filter("country = 'MX'")
                        .select("ml_sh2")
                        .withColumnRenamed("ml_sh2","email")
                        .withColumn("email",lower(col("email")))

    cant = partner_mx.join(compared,Seq("email"),"inner")
                  .select("email")
                  .distinct
                  .count
                        
    println("Mexico2_101419_FINAL.csv -	Retargetly: %s".format(cant))


  // Mexico2_101419_FINAL.csv	acxiom/files/acxiom_MX_Partner_Universe_Extract_20190809.tsv.gz
    df1 = spark.read.format("csv").option("header","true").option("sep","\t").load("/datascience/misc/acxiom_MX_Partner_Universe_Extract_20190809.tsv.gz").select("email1").withColumnRenamed("email1","email")
    df2 = spark.read.format("csv").option("header","true").option("sep","\t").load("/datascience/misc/acxiom_MX_Partner_Universe_Extract_20190809.tsv.gz").select("email2").withColumnRenamed("email2","email")
    

    compared = df1.union(df2)
                  .withColumn("email",lower(col("email")))

    println("acxiom/files/acxiom_MX_Partner_Universe_Extract_20190809.tsv.gz: %s".format(compared.select("email").distinct.count))

    cant = partner_mx.join(compared,Seq("email"),"inner")
                      .select("email")
                      .distinct
                      .count

    println("Mexico2_101419_FINAL.csv	acxiom/files/acxiom_MX_Partner_Universe_Extract_20190809.tsv.gz: %s".format(cant))



  // Chile2_101419_FINAL.csv	Retargetly
  val partner_cl = spark.read.format("csv")
                      .option("header","true")
                      .load("/datascience/misc/Chile2_101419_FINAL.csv")
                      .select("email_sha256")
                      .withColumnRenamed("email_sha256","email")
                      .withColumn("email",lower(col("email")))

  cant = spark.read.load("/datascience/pii_matching/pii_tuples/")
                      .filter("country = 'CL'")
                      .select("ml_sh2")
                      .withColumnRenamed("ml_sh2","email")
                      .withColumn("email",lower(col("email")))
                      .join(partner_cl,Seq("email"),"inner")
                      .select("email")
                      .distinct
                      .count

  println("Chile2_101419_FINAL.csv	Retargetly: %s".format(cant))



  // Peru2_101419_FINAL.csv	Retargetly
  val partner_pe = spark.read.format("csv")
                    .option("header","true")
                    .load("/datascience/misc/Peru2_101419_FINAL.csv")
                    .select("email_sha256")
                    .withColumnRenamed("email_sha256","email")
                    .withColumn("email",lower(col("email")))

  cant = spark.read.load("/datascience/pii_matching/pii_tuples/")
                      .filter("country = 'PE'")
                      .select("ml_sh2")
                      .withColumnRenamed("ml_sh2","email")
                      .withColumn("email",lower(col("email")))
                      .join(partner_pe,Seq("email"),"inner")
                      .select("email")
                      .distinct
                      .count

  println("Peru2_101419_FINAL.csv	Retargetly: %s".format(cant))

 }
 
 def keywords_embeddings(spark:SparkSession, kws_path:String, embeddings_path:String){
   val word_embeddings = spark.read
                              .format("csv")
                              .option("header","true")
                              .load("/datascience/custom/word_embeddings.csv")
                              
    def isAllDigits(x: String) = x forall Character.isDigit

    val myUDF = udf((keyword: String) => if (isAllDigits(keyword)) "DIGITO" else keyword)

    val dataset_kws = spark.read
                            .load(kws_path)
                            .withColumn("keywords",myUDF(col("keywords")))
                            .withColumnRenamed("keywords","word")
                            .withColumn("word",lower(col("word")))

    // Checkpoint
    var join = dataset_kws.join(word_embeddings,Seq("word"),"inner")
                         .write
                         .format("parquet")
                         .mode(SaveMode.Overwrite)
                         .save("/datascience/data_url_classifier/dataset_keyword_embedding_chkpt")

    var df = spark.read.format("parquet").load("/datascience/data_url_classifier/dataset_keyword_embedding_chkpt")
    df.cache()
    
    for (i <- 1 to 300){
      df = df.withColumn(i.toString, col(i.toString)*col("count"))
    } 

    var df_preprocessed = processURL(df).drop("word")
                                        .groupBy("url")
                                        .sum()

    for (i <- 1 to 300){
      df_preprocessed = df_preprocessed.withColumn("sum(%s)".format(i.toString), col("sum(%s)".format(i.toString))/col("sum(count)"))
    }

    df_preprocessed.write
                  .format("csv")
                  .option("header","true")
                  .mode(SaveMode.Overwrite)
                  .save(embeddings_path)
 }

 def processURL_qs(url: String): String = {
  val columns = List("r_mobile", "r_mobile_type", "r_app_name", "r_campaign", "r_lat_long", "id_campaign")
  var res = ""

  try {
    if (url.toString.contains("?")){
      val params = url.split("\\?", -1)(1).split("&").map(p => p.split("=", -1)).map(p => (p(0), p(1))).toMap

      if (params.contains("r_mobile") && params("r_mobile").length>0 && !params("r_mobile").contains("[")){
          res = columns.map(col => if (params.contains(col)) params(col) else "").mkString(",")
      }
    }
  } 
  catch {
    case _: Throwable => println("Error")
  }

  res
}

 def get_report_gcba_1134(spark:SparkSession, ndays: Int, since:Int){

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

  val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)
                .filter("event_type = 'tk'")
                .select("url")

  df.write.format("parquet")
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
      .withColumn(field,lower(col(field)))
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
      .withColumn(field,lower(col(field)))
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

  def get_gt_contextual(spark:SparkSession){

    val segments = List(  26,   32,   36,   59,   61,   82,   85,   92,  104,  118,  129,
                          131,  141,  144,  145,  147,  149,  150,  152,  154,  155,  158,
                          160,  165,  166,  177,  178,  210,  213,  218,  224,  225,  226,
                          230,  245,  247,  250,  264,  265,  270,  275,  276,  302,  305,
                          311,  313,  314,  315,  316,  317,  318,  322,  323,  325,  326,
                          352,  353,  354,  356,  357,  358,  359,  363,  366,  367,  374,
                          377,  378,  379,  380,  384,  385,  386,  389,  395,  396,  397,
                          398,  399,  401,  402,  403,  404,  405,  409,  410,  411,  412,
                          413,  418,  420,  421,  422,  429,  430,  432,  433,  434,  440,
                          441,  446,  447,  450,  451,  453,  454,  456,  457,  458,  459,
                          460,  462,  463,  464,  465,  467,  895,  898,  899,  909,  912,
                          914,  915,  916,  917,  919,  920,  922,  923,  928,  929,  930,
                          931,  932,  933,  934,  935,  937,  938,  939,  940,  942,  947,
                          948,  949,  950,  951,  952,  953,  955,  956,  957, 1005, 1116,
                          1159, 1160, 1166, 2623, 2635, 2636, 2660, 2719, 2720, 2721, 2722,
                          2723, 2724, 2725, 2726, 2727, 2733, 2734, 2735, 2736, 2737, 2743,
                          3010, 3011, 3012, 3013, 3014, 3015, 3016, 3017, 3018, 3019, 3020,
                          3021, 3022, 3023, 3024, 3025, 3026, 3027, 3028, 3029, 3030, 3031,
                          3032, 3033, 3034, 3035, 3036, 3037, 3038, 3039, 3040, 3041, 3055,
                          3076, 3077, 3084, 3085, 3086, 3087, 3302, 3303, 3308, 3309, 3310,
                          3388, 3389, 3418, 3420, 3421, 3422, 3423, 3470, 3472, 3473, 3564,
                          3565, 3566, 3567, 3568, 3569, 3570, 3571, 3572, 3573, 3574, 3575,
                          3576, 3577, 3578, 3579, 3580, 3581, 3582, 3583, 3584, 3585, 3586,
                          3587, 3588, 3589, 3590, 3591, 3592, 3593, 3594, 3595, 3596, 3597,
                          3598, 3599, 3600, 3779, 3782, 3913, 3914, 3915, 4097, 103917,103918,
                          103919,103920,103921,103922,103923,103924,103925,103926,103927,103928,
                          103929,103930,103931,103932,103933,103934,103935,103936,103937,103938,
                          103939,103940,103941,103942,103943,103945,103946,103947,103948,103949,
                          103950,103951,103952,103953,103954,103955,103956,103957,103958,103959,
                          103960,103961,103962,103963,103964,103965,103966,103967,103968,103969,
                          103970,103971,103972,103973,103974,103975,103976,103977,103978,103979,
                          103980,103981,103982,103983,103984,103985,103986,103987,103988,103989,
                          103990,103991,103992,104389,104390,104391,104392,104393,104394,104395,
                          104396,104397,104398,104399,104400,104401,104402,104403,104404,104405,
                          104406,104407,104408,104409,104410,104411,104412,104413,104414,104415,
                          104416,104417,104418,104419,104420,104421,104422,104423,104424,104425,
                          104426,104427,104428,104429,104430,104431,104432,104433,104434,104435,
                          104608,104609,104610,104611,104612,104613,104614,104615,104616,104617,
                          104618,104619,104620,104621,104622)     
    
    val data_urls = get_data_urls(spark, ndays = 30, since = 1, country = "AR")
                              .select("url", "segments")
                              .withColumn("segments", explode(col("segments")))
                              .filter(
                                col("segments")
                                  .isin(segments: _*)
                              ).distinct()

    val urls_contextual = spark.read.format("csv")
                                .option("header","true")
                                .load("/datascience/custom/scrapped_urls.csv")
                                .select("url")

    val urls_contextual_processed = processURL(urls_contextual)

    val joint = data_urls.join(urls_contextual_processed,Seq("url"),"inner").select("url","segments")

    joint.groupBy("url")
          .agg(collect_list(col("segments")).as("segments"))
          .withColumn("segments", concat_ws(";", col("segments")))
          .write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .save("/datascience/data_url_classifier/gt_contextual")

  }

  def get_dataset_contextual(spark:SparkSession, scrapped_path:String){

    val stopwords = List("a","aca","ahi","al","algo","alla","ante",
                          "antes","aquel","aqui","arriba","asi","atras",
                          "aun","aunque","bien","cada","casi","como","con",
                          "cual","cuales","cuan","cuando","de","del","demas",
                          "desde","donde","en","eres","etc","hasta","me",
                          "mientras","muy","para","pero","pues","que","si",
                          "siempre","siendo","sin","sino","sobre","su","sus",
                          "te","tu","tus","y","ya","yo")

    val title_kws = spark.read.format("csv")
                              .option("header","true")
                              .load(scrapped_path)
                              .filter("title is not null")
                              .select("url","title")
                              .withColumn("title", split(col("title"), " "))
                              .withColumn("keywords", explode(col("title")))
                              .filter(!col("keywords").isin(stopwords: _*))
                              .select("url","keywords")
                              .withColumn("count",lit(1))

    title_kws.write.format("parquet")
                    .mode(SaveMode.Overwrite)
                    .save("/datascience/custom/kws_title_contextual")

    keywords_embeddings(spark,
                        kws_path = "/datascience/custom/kws_title_contextual",
                        embeddings_path = "/datascience/data_url_classifier/embeddings_title_contextual")

    val path_kws = spark.read.format("csv")
                              .option("header","true")
                              .load(scrapped_path)
                              .select("url")
                              .withColumn("url", lower(col("url")))
                              .withColumn("url_path", regexp_replace(col("url"), """^[^/]*/""", ""))
                              .withColumn("url_keys", split(col("url_path"), "[^a-z0-9]"))
                              .withColumn("keywords", explode(col("url_keys")))
                              .filter(col("keywords").rlike("[a-z]{2,}"))
                              .filter(!col("keywords").isin(stopwords: _*))
                              .select("url","keywords")
                              .withColumn("count", lit(1))
    
    path_kws.write.format("parquet")
                .mode(SaveMode.Overwrite)
                .save("/datascience/custom/kws_path_contextual")

    keywords_embeddings(spark,
                        kws_path = "/datascience/custom/kws_path_contextual",
                        embeddings_path = "/datascience/data_url_classifier/embeddings_path_contextual")


    title_kws.union(path_kws).write
                            .format("parquet")
                            .mode(SaveMode.Overwrite)
                            .save("/datascience/custom/kws_path_title_contextual")

    keywords_embeddings(spark,
                        kws_path = "/datascience/custom/kws_path_title_contextual",
                        embeddings_path = "/datascience/data_url_classifier/embeddings_path_title_contextual")

  }

  def get_dataset_contextual_augmented(spark:SparkSession, scrapped_path:String){

    val udfLength = udf((xs: Seq[String]) => xs.toList.length * 95/100)
    val udfRandom = udf((xs: Seq[String], n: Int ) => shuffle(xs.toList).take(n))

    val stopwords = List("a","aca","ahi","al","algo","alguna","alguno","algunos","algunas","alla","ambos","ante",
                          "antes","aquel","aquella","aquello","aqui","arriba","asi","atras","aun","aunque","bien",
                          "cada","casi","como","con","cual","cuales","cualquier","cualquiera","cuan","cuando","cuanto",
                          "cuanta","de","del","demas","desde","donde","el","ella","ello","ellos","ellas","en","eres",
                          "esa","ese","eso","esos","esas","esta","este","etc","hasta","la","los","las","me","mi","mia",
                          "mientras","muy","nosotras","nosotros","nuestra","nuestro","nuestras","nuestros","otra","otro",
                          "para","pero","pues","que","si","siempre","siendo","sin","sino","sobre","sr","sra","sres","sta",
                          "su","sus","te","tu","tus","un","una","usted","ustedes","vosotras","vosotros","vuestra","vuestro",
                          "vuestras","vuestros","y","ya","yo")

    val title_kws = spark.read.format("csv")
                              .option("header","true")
                              .load(scrapped_path)
                              .filter("title is not null")
                              .select("url","title")
                              .withColumn("title", split(col("title"), " "))
                              .withColumn("n",udfLength(col("title")))
                              .withColumn("title",udfRandom(col("title"),col("n")))
                              .withColumn("title", split(col("title"), " "))
                              .withColumn("keywords", explode(col("title")))
                              .filter(!col("keywords").isin(stopwords: _*))
                              .select("url","keywords")
                              .withColumn("count",lit(1))

    title_kws.write.format("parquet")
                    .mode(SaveMode.Overwrite)
                    .save("/datascience/custom/kws_title_augmented_contextual")

    keywords_embeddings(spark,
                        kws_path = "/datascience/custom/kws_title_augmented_contextual",
                        embeddings_path = "/datascience/data_url_classifier/embeddings_title_augmented_contextual")

    val path_kws = spark.read.format("csv")
                              .option("header","true")
                              .load(scrapped_path)
                              .select("url")
                              .withColumn("url", lower(col("url")))
                              .withColumn("url_path", regexp_replace(col("url"), """^[^/]*/""", ""))
                              .withColumn("url_keys", split(col("url_path"), "[^a-z0-9]"))
                              .withColumn("n",udfLength(col("url_keys")))
                              .withColumn("url_keys",udfRandom(col("url_keys"),col("n")))
                              .withColumn("keywords", explode(col("url_keys")))
                              .filter(col("keywords").rlike("[a-z]{2,}"))
                              .filter(!col("keywords").isin(stopwords: _*))
                              .select("url","keywords")
                              .withColumn("count", lit(1))
    
    path_kws.write.format("parquet")
                .mode(SaveMode.Overwrite)
                .save("/datascience/custom/kws_path_augmented_contextual")

    keywords_embeddings(spark,
                        kws_path = "/datascience/custom/kws_path_augmented_contextual",
                        embeddings_path = "/datascience/data_url_classifier/embeddings_path_augmented_contextual")


    title_kws.union(path_kws).write
                            .format("parquet")
                            .mode(SaveMode.Overwrite)
                            .save("/datascience/custom/kws_path_title_augmented_contextual")

    keywords_embeddings(spark,
                        kws_path = "/datascience/custom/kws_path_title_contextual",
                        embeddings_path = "/datascience/data_url_classifier/embeddings_path_title_augmented_contextual")

  }


  def get_urls_for_ingester(spark:SparkSession){
   
    val replicationFactor = 8

    val df = processURLHTTP(spark.read.load("/datascience/data_demo/data_urls/day=20191110/").select("url","country"))

    val df_processed = df .withColumn(
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
                ).groupBy("composite_key")
                  .count
                  .withColumn("split", split(col("composite_key"), "@"))
                  .withColumn("url",col("split")(0))
                  .withColumn("country",col("split")(1))
                  .groupBy("url","country")
                  .agg(sum(col("count")).as("count"))
                  .sort(desc("count"))
                  .limit(500000)
        
    df_processed.select("url","country","count").write
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .partitionBy("country")
                .save("/datascience/url_ingester/to_scrap")

  }


  def get_keywords_for_equifax(spark:SparkSession){
    
     val replicationFactor = 8

    val nids = spark.read.load("/datascience/pii_matching/pii_tuples/")
                          .filter("country = 'AR' and nid_sh2 is not null")
                          .select("device_id","nid_sh2")
                          .distinct()
    nids.cache()

     val keywords_diciembre = spark.read
                             .load("/datascience/data_keywords/day=201912*/country=AR/")
                             .select("device_id","content_keys")
                             .distinct()
 

    val keywords_enero = spark.read
                            .load("/datascience/data_keywords/day=202001*/country=AR/")
                            .select("device_id","content_keys")
                            .distinct()


 

    nids.join(keywords_enero,Seq("device_id"),"inner").select("nid_sh2","content_keys") 
                                                    .groupBy("nid_sh2")
                                                    .agg(collect_list(col("content_keys")).as("keywords"))
                                                    .withColumn("keywords", concat_ws(";", col("keywords")))
                                                    .select("nid_sh2","keywords")
                                                    .write
                                                    .format("parquet")
                                                    .mode(SaveMode.Overwrite)
                                                    .save("/datascience/custom/kws_equifax_enero")

  }

  def get_mails_mobs_equifax(spark:SparkSession){
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
                        .select("device_id","content_keys")
                        .distinct()


      joint.join(keywords_oct,Seq("device_id"),"inner").select("pii","content_keys") 
                                              .groupBy("pii")
                                              .agg(collect_list(col("content_keys")).as("keywords"))
                                              .withColumn("keywords", concat_ws(";", col("keywords")))
                                              .select("pii","keywords")
                                              .write
                                              .format("parquet")
                                              .mode(SaveMode.Overwrite)
                                              .save("/datascience/custom/ml_mb_kws_equifax_octubre")

      
      val keywords_nov = spark.read
                        .load("/datascience/data_keywords/day=201911*/country=AR/")
                        .select("device_id","content_keys")
                        .distinct()


      joint.join(keywords_nov,Seq("device_id"),"inner").select("pii","content_keys") 
                                              .groupBy("pii")
                                              .agg(collect_list(col("content_keys")).as("keywords"))
                                              .withColumn("keywords", concat_ws(";", col("keywords")))
                                              .select("pii","keywords")
                                              .write
                                              .format("parquet")
                                              .mode(SaveMode.Overwrite)
                                              .save("/datascience/custom/ml_mb_kws_equifax_noviembre")
  }

  def get_data_BR_matching(spark:SparkSession){
    spark.read.load("/datascience/pii_matching/pii_tuples/")
              .filter("country = 'BR' and nid_sh2 is not null")
              .select("nid_sh2")
              .distinct()
              .write
              .format("parquet")
              .mode(SaveMode.Overwrite)
              .save("/datascience/custom/nids_BR")


    spark.read.load("/datascience/pii_matching/pii_tuples/")
            .filter("country = 'BR' and ml_sh2 is not null")
            .select("ml_sh2")
            .distinct()
            .write
            .format("parquet")
            .mode(SaveMode.Overwrite)
            .save("/datascience/custom/ml_BR")


  }

  def get_data_dani(spark:SparkSession){

    val days = List("01","02","03","04","05")
    for (day <- days){
      spark.read.format("csv").option("sep", "\t").option("header", "true")
                .load("/data/eventqueue/%s/*.tsv.gz".format("2019/12/%s".format(day)))
                .filter("id_partner = 879 and device_id is not null")
                .select("time","id_partner","device_id","campaign_id","campaign_name","segments","device_type","country","data_type","nid_sh2")
                .write
                .format("parquet")
                .save("/datascience/custom/sample_dani/day=%s".format(day))

    }
    
  }

  def process_day_sync(spark:SparkSession,day:String){
        spark.read
            .format("csv")
            .option("sep", "\t")
            .option("header", "true")
            .load("/data/eventqueue/%s/*.tsv.gz".format(day))
            .filter("(event_type = 'sync' or event_type = 'ltm_sync') and (d11 is not null or d13 is not null or d10 is not null or d2 is not null)")
            .select("device_id","d11","d2","d13","d10","id_partner")
            .withColumn("day",lit(day))
            .withColumn("day",regexp_replace(col("day") ,"/", ""))
            .write
            .format("parquet")
            .partitionBy("day")
            .mode("append")
            .save("/datascience/custom/data_sync")

  }

  def report_dada_sync_v2(spark:SparkSession){

    val devices = spark.read.load("/datascience/custom/data_sync")

    // Mediamath
    val devices_mm_across = devices.filter("d10 is not null and id_partner = 47")  
                                      .select("d10")
                                      .distinct()
    devices_mm_across.cache()
    println("D10 aportados por across (Total): %s ".format(devices_mm_across.count))

    val count_mm = devices_mm_across.join(devices.filter("id_partner != 47 and d10 is not null"),Seq("d10"),"inner")
                                      .select("d10")
                                      .distinct()
                                      .count
    println("D10 aportados por across que ya teniamos: %s ".format(count_mm))
    
    // DBM
    val devices_dbm_across = devices.filter("d11 is not null and id_partner = 47")  
                                      .select("d11")
                                      .distinct()
    devices_dbm_across.cache()
    println("d11 aportados por across (Total): %s ".format(devices_dbm_across.count))

    val count_dbm = devices_dbm_across.join(devices.filter("id_partner != 47 and d11 is not null"),Seq("d11"),"inner")
                                      .select("d11")
                                      .distinct()
                                      .count
    println("d11 aportados por across que ya teniamos: %s ".format(count_dbm))

    // TTD
    val devices_ttd_across = devices.filter("d13 is not null and id_partner = 47")  
                                  .select("d13")
                                  .distinct()
    devices_ttd_across.cache()
    println("d13 aportados por across (Total): %s ".format(devices_ttd_across.count))

    val count_ttd = devices_ttd_across.join(devices.filter("id_partner != 47 and d13 is not null"),Seq("d13"),"inner")
                                      .select("d13")
                                      .distinct()
                                      .count
    println("d13 aportados por across que ya teniamos: %s ".format(count_ttd))

    // APPNXS
    val devices_apn_across = devices.filter("d2 is not null and id_partner = 47")  
                              .select("d2")
                              .distinct()
    devices_apn_across.cache()
    println("d2 aportados por across (Total): %s ".format(devices_apn_across.count))

    val count_apn = devices_ttd_across.join(devices.filter("id_partner != 47 and d2 is not null"),Seq("d2"),"inner")
                                      .select("d2")
                                      .distinct()
                                      .count
    println("d2 aportados por across que ya teniamos: %s ".format(count_apn))
    
  }


  def report_dada_sync(spark:SparkSession){

    val devices = spark.read.load("/datascience/custom/data_sync")

    // Mediamath
    val devices_mm_across = devices.filter("d10 is not null and id_partner = 47")  
                                      .select("device_id")
                                      .distinct()
    devices_mm_across.cache()
    println("Devices de MM aportados por across (Total): %s ".format(devices_mm_across.count))

    val count_mm = devices_mm_across.join(devices.filter("id_partner != 47 and d10 is not null"),Seq("device_id"),"inner")
                                      .select("device_id")
                                      .distinct()
                                      .count
    println("Devices de MM aportados por across que ya teniamos: %s ".format(count_mm))
    
    // DBM
    val devices_dbm_across = devices.filter("d11 is not null and id_partner = 47")  
                                      .select("device_id")
                                      .distinct()
    devices_dbm_across.cache()
    println("Devices de DBM aportados por across (Total): %s ".format(devices_dbm_across.count))

    val count_dbm = devices_dbm_across.join(devices.filter("id_partner != 47 and d11 is not null"),Seq("device_id"),"inner")
                                      .select("device_id")
                                      .distinct()
                                      .count
    println("Devices de DBM aportados por across que ya teniamos: %s ".format(count_dbm))

    // TTD
    val devices_ttd_across = devices.filter("d13 is not null and id_partner = 47")  
                                  .select("device_id")
                                  .distinct()
    devices_ttd_across.cache()
    println("Devices de TTD aportados por across (Total): %s ".format(devices_ttd_across.count))

    val count_ttd = devices_ttd_across.join(devices.filter("id_partner != 47 and d13 is not null"),Seq("device_id"),"inner")
                                      .select("device_id")
                                      .distinct()
                                      .count
    println("Devices de TTD aportados por across que ya teniamos: %s ".format(count_ttd))

    // APPNXS
    val devices_apn_across = devices.filter("d2 is not null and id_partner = 47")  
                              .select("device_id")
                              .distinct()
    devices_apn_across.cache()
    println("Devices de Appnexus aportados por across (Total): %s ".format(devices_apn_across.count))

    val count_apn = devices_ttd_across.join(devices.filter("id_partner != 47 and d2 is not null"),Seq("device_id"),"inner")
                                      .select("device_id")
                                      .distinct()
                                      .count
    println("Devices de Appnexus aportados por across que ya teniamos: %s ".format(count_apn))
    
  }

  def get_join_kws(spark:SparkSession){

    val encodeUdf = udf((s: String) => scala.io.Source.fromBytes(s.getBytes(), "UTF-8").mkString)

    val df = spark.read
      .option("basePath", "/datascience/data_audiences_streaming/")
      .parquet("/datascience/data_audiences_streaming/hour=%s*".format(20200121)) // We read the data
      .withColumn("url",regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")) 
      .withColumn("url",encodeUdf(col("url"))).withColumn("url",regexp_replace(col("url"), "'", ""))
      .select( "url")
      .distinct()

    val kws = spark.read.format("csv").option("header","true")
                    .load("/datascience/selected_keywords/2020-01-21.csv")
                    .withColumnRenamed("url_raw","url")
                    .withColumn("url",regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", ""))
                    .select("url")
                    .distinct()

    df.join(kws,Seq("url"),"inner")
      .write
      .format("parquet").mode(SaveMode.Overwrite)
      .save("/datascience/custom/join_kws")

  }

  def get_kws_sharethis(spark:SparkSession){
    spark.read.json("/data/providers/sharethis/keywords/")
          .select("url")
          .write
          .format("csv")
          .option("delimiter","\t")
          .mode("overwrite")
          .save("/datascience/custom/kws_sharethis/")

  }

  def get_piis_bridge(spark:SparkSession){
    val pii_1 = spark.read.format("csv").option("header","true")
                      .load("/data/providers/Bridge/Bridge_Linkage_File_Retargetly_LATAM.csv")
                      .withColumnRenamed("SHA256_Email_Hash","pii")
                      .select("pii")
    val pii_2 = spark.read.format("csv").option("header","true")
                      .load("/data/providers/Bridge/Bridge_Linkage_File_Retargetly_LATAM_Historical.csv")
                      .withColumnRenamed("SHA256_Email_Hash","pii")
                      .select("pii")
    val pii_3 = spark.read.format("csv").option("header","true")
                      .load("/data/providers/Bridge/Retargetly_Bridge_Linkage_LATAM_01_2020.csv")
                      .withColumnRenamed("SHA256_Email_Hash","pii")
                      .select("pii")
    val pii_4 = spark.read.format("csv").option("header","true")
                      .load("/data/providers/Bridge/Retargetly_Bridge_Linkage_LATAM_11_2019.csv")
                      .withColumnRenamed("SHA256_Email_Hash","pii")
                      .select("pii")
    val pii_5 = spark.read.format("csv").option("header","true")
                      .load("/data/providers/Bridge/Retargetly_Bridge_Linkage_LATAM_12_2019.csv")
                      .withColumnRenamed("SHA256_Email_Hash","pii")
                      .select("pii")
    
    pii_1.unionAll(pii_2)
          .unionAll(pii_3)
          .unionAll(pii_4)
          .unionAll(pii_5)
          .select("pii")
          .distinct()
          .write.format("csv")
          .save("/datascience/custom/piis_bridge")

    }

  val udfGet = udf(
        (segments: Seq[Row], pos: Int) => segments.map(record => record(pos).toString)
      )    
    

  def get_dataset_sharethis_kws(spark:SparkSession){
    val kws_scrapper = spark.read.format("csv")
                            .option("header","true")
                            .load("/datascience/custom/kws_sharethis_scrapper_20200130.csv")
                            .withColumnRenamed("url_raw","url")
                            .withColumnRenamed("kw","kw_scrapper")
                            .select("url","kw_scrapper")

  val kws_sharethis = spark.read.json("/data/providers/sharethis/keywords/")
                            .na.drop()
                            .withColumnRenamed("keywords","kws_sharethis")
                            .withColumn("kws_sharethis",udfGet(col("kws_sharethis"),lit(2)))
                            .withColumn("concepts",udfGet(col("concepts"),lit(1)))
                            .withColumn("entities",udfGet(col("entities"),lit(3)))
                            .withColumn("concepts", concat_ws(";", col("concepts")))
                            .withColumn("kws_sharethis", concat_ws(";", col("kws_sharethis")))
                            .withColumn("entities", concat_ws(";", col("entities")))
                            .select("url","kws_sharethis","entities","concepts")

    kws_sharethis.join(kws_scrapper,Seq("url"),"inner")
                  .write.format("csv")
                  .mode(SaveMode.Overwrite)
                  .save("/datascience/custom/dataset_kws_sharethis")
  }

  def email_to_madid(spark:SparkSession){
    val piis = spark.read
                    .load("/datascience/pii_matching/pii_tuples/")
                    .filter("country = 'CL' and ml_sh2 is not null")
                    .select("device_id","ml_sh2")
                    .distinct()

    val crossdevice = spark.read.format("csv")
                            .load("/datascience/audiences/crossdeviced/cookies_cl_xd")
                            .withColumnRenamed("_c0","device_id")

    val count = piis.join(crossdevice,Seq("device_id"),"inner")
                    .select("ml_sh2")
                    .distinct
                    .count()

    println("Count ml unique: %s".format(count))
  }

  def report_33across(spark:SparkSession){

    spark.read.format("csv")
          .option("sep", "\t")
          .option("header", "true")
          .load("/datascience/33accross/20200*/*/*views-ar-*.gz")
          .groupBy("PAGE_URL")
          .agg(approx_count_distinct(col("COOKIE"), 0.01).as("devices"))
          .sort(desc("devices"))
          .write.format("csv").save("/datascience/custom/urls_ar_across")


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
          .write.format("csv").save("/datascience/custom/urls_mx_across")

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


  def report_sharethis(spark:SparkSession){

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(0)
    val days = (0 until 15).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_partner_streaming/"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days.flatMap(day =>(0 until 24).map(hour =>path + "hour=%s%02d/id_partner=%s".format(day, hour, 411))).filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*).select("url","device_id","country")

    df.filter("country = 'AR'")
      .groupBy("url")
      .agg(approx_count_distinct(col("device_id"), 0.01).as("devices"))
      .sort(desc("devices"))
      .write.format("csv").save("/datascience/custom/urls_ar_st")


    val grouped_domain_ar = df.filter("country = 'AR'")
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
      .write.format("csv").save("/datascience/custom/urls_mx_st")


    val grouped_domain_mx = df.filter("country = 'MX'")
                              .selectExpr("*", "parse_url(%s, 'HOST') as domain".format("url"))
                              .groupBy("domain")
                              .agg(approx_count_distinct(col("device_id"), 0.01).as("devices"))
                              .sort(desc("devices"))

    println("Top Domains MX")
    grouped_domain_mx.show(15)


  }
  
  def report_bridge(spark:SparkSession){

    val bridge = spark.read.format("csv")
                      .option("header","true")
                      .load("/data/tmp/Bridge_Linkage_File_Retargetly_LATAM_ALL.csv")
                      .filter("country = 'ar'")
                      .withColumnRenamed("advertising_id","device_id")
                      .withColumn("device_id",lower(col("device_id")))

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(0)
    val days = (0 until 30).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_partner_streaming/"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files_factual = days.flatMap(day =>(0 until 24).map(hour =>path + "hour=%s%02d/id_partner=%s".format(day, hour, 1008))).filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val factual = spark.read.option("basePath", path).parquet(hdfs_files_factual: _*)
                        .filter("country = 'AR'")
                        .withColumn("device_id",lower(col("device_id")))
                        .select("device_id")


    // Now we obtain the list of hdfs folders to be read
    val hdfs_files_startapp = days.flatMap(day =>(0 until 24).map(hour =>path + "hour=%s%02d/id_partner=%s".format(day, hour, 1139))).filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val startapp = spark.read.option("basePath", path).parquet(hdfs_files_startapp: _*)
                        .filter("country = 'AR'")
                        .withColumn("device_id",lower(col("device_id")))
                        .select("device_id")

    val geo = spark.read.format("csv")
                    .option("sep","\t")
                    .option("header","true")
                    .load("/datascience/geo/NSEHomes/argentina_365d_home_21-1-2020-12h_xd_push")
                    .withColumn("device_id",lower(col("device_id")))

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
    bridge.join(startapp,Seq("device_id"),"inner")
          .write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .save("/datascience/custom/join_startapp")

    bridge.join(geo,Seq("device_id"),"inner")
          .write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .save("/datascience/custom/join_geo")

  

  }

    def report_gcba(spark:SparkSession){

    val gcba = spark.read.format("csv")
                      .option("header","true")
                      .load("/datascience/custom/devices_gcba.csv")
                      .withColumnRenamed("Device Id","device_id")
                      .withColumn("device_id",lower(col("device_id")))
                      .select("device_id")
                      .distinct

    val bridge = spark.read.format("csv")
                  .option("header","true")
                  .load("/data/tmp/Bridge_Linkage_File_Retargetly_LATAM_ALL.csv")
                  .filter("country = 'ar'")
                  .withColumnRenamed("advertising_id","device_id")
                  .withColumn("device_id",lower(col("device_id")))

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(0)
    val days = (0 until 30).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_partner_streaming/"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files_factual = days.flatMap(day =>(0 until 24).map(hour =>path + "hour=%s%02d/id_partner=%s".format(day, hour, 1008))).filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val factual = spark.read.option("basePath", path).parquet(hdfs_files_factual: _*)
                        .filter("country = 'AR'")
                        .withColumn("device_id",lower(col("device_id")))
                        .select("device_id")


    // Now we obtain the list of hdfs folders to be read
    val hdfs_files_startapp = days.flatMap(day =>(0 until 24).map(hour =>path + "hour=%s%02d/id_partner=%s".format(day, hour, 1139))).filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val startapp = spark.read.option("basePath", path).parquet(hdfs_files_startapp: _*)
                        .filter("country = 'AR'")
                        .withColumn("device_id",lower(col("device_id")))
                        .select("device_id")

    val geo = spark.read.format("csv")
                    .option("sep","\t")
                    .option("header","true")
                    .load("/datascience/geo/NSEHomes/argentina_365d_home_21-1-2020-12h_xd_push")
                    .withColumn("device_id",lower(col("device_id")))


    // val join_bridge = gcba.join(bridge,Seq("device_id"),"inner")
    
    // val join_factual = gcba.join(factual,Seq("device_id"),"inner")
    // val join_startapp = gcba.join(startapp,Seq("device_id"),"inner")
    // val join_geo = gcba.join(geo,Seq("device_id"),"inner")


    val hdfs_files_gcba = days.flatMap(day =>(0 until 24).map(hour =>path + "hour=%s%02d/id_partner=%s".format(day, hour, 349))).filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val gcba_first = spark.read.option("basePath", path).parquet(hdfs_files_factual: _*)
                        .filter("country = 'AR'")
                        .withColumn("device_id",lower(col("device_id")))
                        .select("device_id")

    println("Join Final Devices:")
    //println(gcba.join(factual,Seq("device_id"),"inner").join(startapp,Seq("device_id"),"inner").join(geo,Seq("device_id"),"inner").join(bridge,Seq("device_id"),"inner").join(gcba_first,Seq("device_id"),"inner").select("device_id").distinct().count())
    println("GCBA First Devices:")
    println(gcba.join(gcba_first,Seq("device_id"),"inner").select("device_id").distinct().count())

  }

  def analisis_domains(spark:SparkSession){
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    var start = DateTime.now.minusDays(11 + 15)
    var end = DateTime.now.minusDays(11)
    var daysCount = Days.daysBetween(start, end).getDays()
    var days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))
    val path = "/datascience/data_keywords"

    // Now we obtain the list of hdfs folders to be read
    var hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, "AR")) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"


    val data_new = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .groupBy("content_keys")//groupBy("domain")
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
      .groupBy("content_keys")//groupBy("domain")
      .agg(approx_count_distinct(col("device_id"), 0.03).as("devices_old"))


    //data_old.join(data_new,Seq("domain"),"inner")
    data_old.join(data_new,Seq("content_keys"),"outer").na.fill(0)
    .write.format("csv").option("header","true")
    .mode(SaveMode.Overwrite)
    .save("/datascience/custom/domains_scrapper_2102_all_kws")

  }

  def matching_detergentes(spark:SparkSession){
    val detergentes_nid = spark.read.format("csv").option("header","true").load("/datascience/custom/limpiadores_detergentes.csv")
                            .filter("device_type = 'nid'")
                            .select("device_id")
                            .withColumnRenamed("device_id","nid_sh2")
                            .distinct()

    val detergentes_ml = spark.read.format("csv").option("header","true").load("/datascience/custom/limpiadores_detergentes.csv")
                            .filter("device_type = 'email'")
                            .select("device_id")
                            .withColumnRenamed("device_id","ml_sh2")
                            .distinct()

    val detergentes_mob = spark.read.format("csv").option("header","true").load("/datascience/custom/limpiadores_detergentes.csv")
                            .filter("device_type = 'phone'")
                            .select("device_id")
                            .withColumnRenamed("device_id","mb_sh2")
                            .distinct()

    val nids = spark.read.load("/datascience/pii_matching/pii_tuples/")
                          .filter("country = 'AR' and nid_sh2 is not null")
                          .select("nid_sh2")
                          .distinct()

    val mob = spark.read.load("/datascience/pii_matching/pii_tuples/")
                      .filter("country = 'AR' and mb_sh2 is not null")
                      .select("mb_sh2")
                      .distinct()

    val mls = spark.read.load("/datascience/pii_matching/pii_tuples/")
                    .filter("country = 'AR' and ml_sh2 is not null")
                    .select("ml_sh2")
                    .distinct()

//    println("Devices Nid:")
 //   println(detergentes_nid.join(nids,Seq("nid_sh2"),"inner").select("nid_sh2").distinct().count())

    println("Devices Mail:")
    println(detergentes_ml.join(mls,Seq("ml_sh2"),"inner").select("ml_sh2").distinct().count())

    println("Devices Phone:")
    println(detergentes_mob.join(mob,Seq("mb_sh2"),"inner").select("mb_sh2").distinct().count())
  
  }

  def enrichment_target_data(spark:SparkSession){
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val since = 1
    val ndays = 30
    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(since)
    val days = (0 until ndays).map(start.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_triplets/segments/"
    val dfs = days.map(day => path + "day=%s/".format(day) + "country=BR")
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      .map(
        x =>
          spark.read
            .option("basePath", "/datascience/data_triplets/segments/")
            .parquet(x)
            .select("device_id","feature")
      )

    val segments = List(463, 105154, 105155, 105156, 32, 103977)

    val triplets = dfs.reduce((df1, df2) => df1.union(df2)).filter(col("feature").isin(segments: _*)).select("device_id","feature")
    
    val mails = spark.read
                  .load("/datascience/pii_matching/pii_tuples/")
                  .filter("country = 'BR' and ml_sh2 is not  null")
                  .select("device_id","ml_sh2")
                  .withColumnRenamed("ml_sh2","pii")

    val nids = spark.read
                  .load("/datascience/pii_matching/pii_tuples/")
                  .filter("country = 'BR' and nid_sh2 is not null")
                  .select("device_id","nid_sh2")
                  .withColumnRenamed("nid_sh2","pii")

    val piis = mails.union(nids)

    val joint = piis.join(triplets,Seq("device_id"),"inner")
                                  .groupBy("pii")
                                  .agg(collect_list(col("feature")).as("feature"))
                                  .withColumn("feature", concat_ws(";", col("feature")))
                                  .select("pii","feature")
                                  .write
                                  .format("parquet")
                                  .mode(SaveMode.Overwrite)
                                  .save("/datascience/custom/enrichment_target_data")
  
  }

  def report_havas(spark:SparkSession){
    
    val detergentes_nid = spark.read.format("csv").option("header","true").load("/datascience/custom/limpiadores_detergentes.csv")
                        .filter("device_type = 'nid'")
                        .select("device_id")
                        .withColumnRenamed("device_id","nid_sh2")
                        .distinct()

    val detergentes_ml = spark.read.format("csv").option("header","true").load("/datascience/custom/limpiadores_detergentes.csv")
                            .filter("device_type = 'email'")
                            .select("device_id")
                            .withColumnRenamed("device_id","ml_sh2")
                            .distinct()

    val detergentes_mob = spark.read.format("csv").option("header","true").load("/datascience/custom/limpiadores_detergentes.csv")
                            .filter("device_type = 'phone'")
                            .select("device_id")
                            .withColumnRenamed("device_id","mb_sh2")
                            .distinct()

    val nids = spark.read.load("/datascience/pii_matching/pii_tuples/")
                          .filter("country = 'AR' and nid_sh2 is not null")
                          .select("device_id","nid_sh2")
                          .distinct()

    val mob = spark.read.load("/datascience/pii_matching/pii_tuples/")
                      .filter("country = 'AR' and mb_sh2 is not null")
                      .select("device_id","mb_sh2")
                      .distinct()

    val mls = spark.read.load("/datascience/pii_matching/pii_tuples/")
                    .filter("country = 'AR' and ml_sh2 is not null")
                    .select("device_id","ml_sh2")
                    .distinct()

    // Get pii data <device_id, pii>
    val join_nids = detergentes_nid.join(nids,Seq("nid_sh2"),"inner").select("device_id","nid_sh2").withColumnRenamed("nid_sh2","pii")
    val join_ml = detergentes_ml.join(mls,Seq("ml_sh2"),"inner").select("device_id","ml_sh2").withColumnRenamed("ml_sh2","pii")
    val join_mob = detergentes_mob.join(mob,Seq("mb_sh2"),"inner").select("device_id","mb_sh2").withColumnRenamed("mb_sh2","pii")

    val piis = join_nids.union(join_ml)
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
    val dfs = days.map(day => path + "day=%s/".format(day) + "country=AR")
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      .map(
        x =>
          spark.read
            .option("basePath", "/datascience/data_triplets/segments/")
            .parquet(x)
            .select("device_id","feature")
      )

    val segments = List(129, 61, 141, 302, 144, 2, 3, 4, 5, 6, 7, 8, 9, 352, 35360, 35361, 35362, 35363, 20107,
                      20108, 20109,20110,20111,20112,20113,20114,20115,20116,20117,20118,20119,20120,20121,20122,
                      20123,20124,20125,20126)

    val triplets = dfs.reduce((df1, df2) => df1.union(df2)).filter(col("feature").isin(segments: _*)).select("device_id","feature").distinct()

    triplets.join(piis,Seq("device_id"),"inner").write.format("parquet").mode(SaveMode.Overwrite).save("/datascience/custom/report_havas")
  }

  def report_tapad_madids(spark:SparkSession){

    val madids_factual = spark.read.format("csv").option("sep","\t")
                              .load("/datascience/devicer/processed/madids_factual/part-00000-3ac5df52-df3c-4bba-b64c-997007ce486d-c000.csv")
                              .withColumnRenamed("_c1","madids")
                              .select("madids")
    val madids_startapp = spark.read.format("csv").option("sep","\t")
                              .load("/datascience/devicer/processed/madids_startapp/part-00000-d1ed18a6-48a5-4e68-bb5a-a5c303704f45-c000.csv")
                              .withColumnRenamed("_c1","madids")
                              .select("madids")
    // GEO
    val madids_geo_ar = spark.read.format("csv").option("delimiter","\t")
                              .load("/datascience/geo/NSEHomes/argentina_365d_home_21-1-2020-12h")
                              .withColumnRenamed("_c0","madids")
                              .select("madids")

    val madids_geo_mx = spark.read.format("csv").option("delimiter","\t")
                          .load("/datascience/geo/NSEHomes/mexico_200d_home_29-1-2020-12h")
                          .withColumnRenamed("_c0","madids")
                          .select("madids")

    val madids_geo_cl = spark.read.format("csv").option("delimiter","\t")
                                  .load("/datascience/geo/NSEHomes/CL_90d_home_29-1-2020-12h")
                                  .withColumnRenamed("_c0","madids")
                                  .select("madids")

    val madids_geo_co = spark.read.format("csv").option("delimiter","\t")
                              .load("/datascience/geo/NSEHomes/CO_90d_home_18-2-2020-12h")
                              .withColumnRenamed("_c0","madids")
                              .select("madids")

    madids_factual.union(madids_startapp)
                  .union(madids_geo_ar)
                  .union(madids_geo_mx)
                  .union(madids_geo_cl)
                  .union(madids_geo_co)
                  .withColumn("madids",lower(col("madids")))
                  .distinct
                  .write.format("csv")
                  .save("/datascience/custom/tapad_madids")

  }
  def report_tapad_bridge(spark:SparkSession){
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val path = "/data/providers/Bridge/"
    val dfs = fs
      .listStatus(new Path(path))
      .map(x =>  spark.read.format("csv").option("header","true").load(path + x.getPath.toString.split("/").last))
      .toList

    val df_union = dfs.reduce((df1, df2) => df1.unionAll(df2)).select("Timestamp","IP_Address","Device_ID","Device_Type")

    df_union.write.format("csv").save("/datascience/custom/report_tapad_bridge")

  }
  def dummy_havas(spark:SparkSession){

    val udfFeature = udf((r: Double) => if (r > 0.5) 34316 else 34323)

    val detergentes_nid = spark.read.format("csv").option("header","true").load("/datascience/custom/limpiadores_detergentes.csv")
                            .filter("device_type = 'nid'")
                            .select("device_id")
                            .withColumnRenamed("device_id","nid_sh2")
                            .distinct()

    val detergentes_ml = spark.read.format("csv").option("header","true").load("/datascience/custom/limpiadores_detergentes.csv")
                            .filter("device_type = 'email'")
                            .select("device_id")
                            .withColumnRenamed("device_id","ml_sh2")
                            .distinct()

    val detergentes_mob = spark.read.format("csv").option("header","true").load("/datascience/custom/limpiadores_detergentes.csv")
                            .filter("device_type = 'phone'")
                            .select("device_id")
                            .withColumnRenamed("device_id","mb_sh2")
                            .distinct()

    val nids = spark.read.load("/datascience/pii_matching/pii_tuples/")
                          .filter("country = 'AR' and nid_sh2 is not null")
                          .select("device_id","nid_sh2")
                          .distinct()

    val mob = spark.read.load("/datascience/pii_matching/pii_tuples/")
                      .filter("country = 'AR' and mb_sh2 is not null")
                      .select("device_id","mb_sh2")
                      .distinct()

    val mls = spark.read.load("/datascience/pii_matching/pii_tuples/")
                    .filter("country = 'AR' and ml_sh2 is not null")
                    .select("device_id","ml_sh2")
                    .distinct()

    // Get pii data <device_id, pii>
      val join_nids = detergentes_nid.join(nids,Seq("nid_sh2"),"inner").select("device_id","nid_sh2").withColumnRenamed("nid_sh2","pii")
      val join_ml = detergentes_ml.join(mls,Seq("ml_sh2"),"inner").select("device_id","ml_sh2").withColumnRenamed("ml_sh2","pii")
      val join_mob = detergentes_mob.join(mob,Seq("mb_sh2"),"inner").select("device_id","mb_sh2").withColumnRenamed("mb_sh2","pii")

      val piis = join_nids.union(join_ml)
                          .union(join_mob)
                          .select("device_id")
                          .distinct()
                          .withColumn("rand",rand()).withColumn("feature",udfFeature(col("rand")))
                          .withColumn("id_partner",lit(119))
                          .withColumn("device_type",lit("web"))
                          .withColumn("activable",lit(1))
                          .select("device_id","feature","id_partner","device_type","activable")
                          .write.format("parquet")
                          .mode(SaveMode.Overwrite)
                          .save("/datascience/custom/dummy_havas")


  }
  def get_piis_cl(spark:SparkSession){
    val xd = spark.read
                  .format("csv")
                  .option("sep",",")
                  .load("/datascience/audiences/crossdeviced/CL_90d_home_29-1-2020-12h_xd")
                  .withColumnRenamed("_c0","madid")
                  .withColumnRenamed("_c1","device_id")
                  .withColumnRenamed("_c7","lat")
                  .withColumnRenamed("_c8","lon")
                  .select("madid","lat","lon","device_id")

    val pii = spark.read.load("/datascience/pii_matching/pii_tuples/")

    xd.join(pii,Seq("device_id"),"inner")
      .select("madid","lat","lon","ml_sh2","nid_sh2","mb_sh2")
      .repartition(50)
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/piis_madids_cl")
      
  }

  def report_user_uniques(spark:SparkSession){
    val segments = List("34343",
                        "34375",
                        "34407",
                        "35958",
                        "35990",
                        "36022",
                        "36054",
                        "84679",
                        "121505",
                        "136569",
                        "144751",
                        "145389",
                        "201321",
                        "201431",
                        "201495",
                        "201561",
                        "201625",
                        "201689",
                        "201753",
                        "201821",
                        "201885",
                        "202157",
                        "287163",
                        "162025",
                        "181863",
                        "181937",
                        "12",
                        "182007",
                        "104013",
                        "182173",
                        "147",
                        "182433",
                        "316",
                        "182637",
                        "3018",
                        "182725",
                        "103930",
                        "182795",
                        "103962",
                        "182873",
                        "353",
                        "182981",
                        "418",
                        "183215",
                        "914",
                        "183489",
                        "1159",
                        "183597",
                        "3473",
                        "183733",
                        "3915",
                        "183821",
                        "104397",
                        "183901",
                        "104429",
                        "183997",
                        "573",
                        "184121",
                        "609",
                        "184193",
                        "641",
                        "184357",
                        "673",
                        "184497",
                        "705",
                        "184589",
                        "737",
                        "184957",
                        "769",
                        "185201",
                        "801",
                        "185283",
                        "1194",
                        "245727",
                        "4642",
                        "245807",
                        "4538",
                        "245901",
                        "4749",
                        "245975",
                        "4800",
                        "246081",
                        "24626",
                        "246181",
                        "24658",
                        "246275",
                        "24733",
                        "246361",
                        "24673",
                        "246485",
                        "24705",
                        "246559",
                        "49909",
                        "246633",
                        "49941",
                        "246705",
                        "260985",
                        "246789",
                        "48333",
                        "246861",
                        "48365",
                        "246937",
                        "48397",
                        "247005",
                        "48429",
                        "247073",
                        "48461",
                        "118245",
                        "48084",
                        "118428",
                        "48116",
                        "120719",
                        "48148",
                        "4117",
                        "48180",
                        "4191",
                        "48212",
                        "48244",
                        "48276",
                        "104611",
                        "105057",
                        "105089",
                        "105121",
                        "105153",
                        "105185",
                        "105217",
                        "105249",
                        "105281",
                        "105313",
                        "20110",
                        "22504",
                        "34354",
                        "34386",
                        "34418",
                        "35969",
                        "36001",
                        "36033",
                        "81445",
                        "84690",
                        "136505",
                        "136591",
                        "145301",
                        "157453",
                        "201365",
                        "201453",
                        "201517",
                        "201583",
                        "201647",
                        "201711",
                        "201775",
                        "201843",
                        "202041",
                        "209773",
                        "289867",
                        "162093",
                        "181887",
                        "181961",
                        "98948",
                        "182059",
                        "104024",
                        "182231",
                        "178",
                        "182531",
                        "2719",
                        "182667",
                        "3913",
                        "182747",
                        "103941",
                        "182819",
                        "103973",
                        "182911",
                        "378",
                        "183061",
                        "446",
                        "183305",
                        "931",
                        "183531",
                        "3030",
                        "183679",
                        "3574",
                        "183757",
                        "99598",
                        "183849",
                        "104408",
                        "183927",
                        "219057",
                        "184071",
                        "588",
                        "184149",
                        "620",
                        "184231",
                        "652",
                        "184427",
                        "684",
                        "184525",
                        "716",
                        "184615",
                        "748",
                        "185049",
                        "780",
                        "185229",
                        "812",
                        "185305",
                        "1339",
                        "245751",
                        "99643",
                        "245843",
                        "4757",
                        "245923",
                        "4779",
                        "245999",
                        "4811",
                        "246115",
                        "24637",
                        "246223",
                        "24669",
                        "246303",
                        "24744",
                        "246409",
                        "24684",
                        "246509",
                        "34786",
                        "246585",
                        "49920",
                        "246655",
                        "49952",
                        "246733",
                        "261007",
                        "246813",
                        "48344",
                        "246889",
                        "48376",
                        "246961",
                        "48408",
                        "247029",
                        "48440",
                        "247095",
                        "48310",
                        "118370",
                        "48095",
                        "120629",
                        "48127",
                        "120737",
                        "48159",
                        "4128",
                        "48191",
                        "6591",
                        "48223",
                        "48255",
                        "48287",
                        "104622",
                        "105068",
                        "105100",
                        "105132",
                        "105164",
                        "105196",
                        "105228",
                        "105260",
                        "105292",
                        "105324",
                        "20121",
                        "34322",
                        "34333",
                        "34365",
                        "34397",
                        "35367",
                        "35980",
                        "36012",
                        "36044",
                        "84669",
                        "121495",
                        "136547",
                        "136661",
                        "145369",
                        "157483",
                        "201387",
                        "201475",
                        "201541",
                        "201605",
                        "201669",
                        "201733",
                        "201799",
                        "201865",
                        "202135",
                        "210283",
                        "289889",
                        "181785",
                        "181915",
                        "181987",
                        "104003",
                        "182137",
                        "82",
                        "182365",
                        "264",
                        "182613",
                        "2736",
                        "182705",
                        "103920",
                        "182773",
                        "103952",
                        "182845",
                        "103984",
                        "182947",
                        "399",
                        "183119",
                        "462",
                        "183425",
                        "948",
                        "183567",
                        "3041",
                        "183713",
                        "3585",
                        "183789",
                        "99638",
                        "183879",
                        "104419",
                        "183963",
                        "563",
                        "184097",
                        "599",
                        "184173",
                        "631",
                        "184311",
                        "663",
                        "184453",
                        "695",
                        "184567",
                        "727",
                        "184639",
                        "759",
                        "185165",
                        "791",
                        "185263",
                        "823",
                        "245705",
                        "1351",
                        "245779",
                        "3043",
                        "245875",
                        "4768",
                        "245953",
                        "4790",
                        "246031",
                        "24616",
                        "246143",
                        "24648",
                        "246251",
                        "24723",
                        "246339",
                        "24755",
                        "246457",
                        "24695",
                        "246533",
                        "34797",
                        "246607",
                        "49931",
                        "246681",
                        "49963",
                        "246757",
                        "261029",
                        "246837",
                        "48355",
                        "246915",
                        "48387",
                        "246985",
                        "48419",
                        "247053",
                        "48451",
                        "247119",
                        "48320",
                        "118388",
                        "48106",
                        "120647",
                        "48138",
                        "287197",
                        "48170",
                        "4026",
                        "48202",
                        "48234",
                        "48266",
                        "48298",
                        "289831",
                        "105079",
                        "105111",
                        "105143",
                        "105175",
                        "105207",
                        "105239",
                        "105271",
                        "105303",
                        "105335",
                        "22484",
                        "34344",
                        "34376",
                        "34408",
                        "35959",
                        "35991",
                        "36023",
                        "81435",
                        "84680",
                        "136483",
                        "136571",
                        "144755",
                        "145391",
                        "201323",
                        "201433",
                        "201497",
                        "201563",
                        "201627",
                        "201691",
                        "201755",
                        "201823",
                        "201887",
                        "202161",
                        "287165",
                        "162031",
                        "181865",
                        "181939",
                        "13",
                        "182009",
                        "104014",
                        "182177",
                        "149",
                        "182441",
                        "317",
                        "182639",
                        "3019",
                        "182727",
                        "103931",
                        "182797",
                        "103963",
                        "182875",
                        "354",
                        "183009",
                        "420",
                        "183267",
                        "915",
                        "183493",
                        "1160",
                        "183601",
                        "3564",
                        "183735",
                        "5022",
                        "183829",
                        "104398",
                        "183903",
                        "104430",
                        "184025",
                        "574",
                        "184123",
                        "610",
                        "184199",
                        "642",
                        "184359",
                        "674",
                        "184499",
                        "706",
                        "184591",
                        "738",
                        "184973",
                        "770",
                        "185203",
                        "802",
                        "185285",
                        "1195",
                        "245729",
                        "4643",
                        "245811",
                        "12900",
                        "245903",
                        "4769",
                        "245977",
                        "4801",
                        "246083",
                        "24627",
                        "246183",
                        "24659",
                        "246277",
                        "24734",
                        "246371",
                        "24674",
                        "246487",
                        "24706",
                        "246561",
                        "49910",
                        "246635",
                        "49942",
                        "246707",
                        "260987",
                        "246791",
                        "48334",
                        "246863",
                        "48366",
                        "246939",
                        "48398",
                        "247009",
                        "48430",
                        "247075",
                        "48462",
                        "118246",
                        "48085",
                        "118429",
                        "48117",
                        "120720",
                        "48149",
                        "4118",
                        "48181",
                        "4214",
                        "48213",
                        "48245",
                        "48277",
                        "104612",
                        "105058",
                        "105090",
                        "105122",
                        "105154",
                        "105186",
                        "105218",
                        "105250",
                        "105282",
                        "105314",
                        "20111",
                        "22506",
                        "34355",
                        "34387",
                        "34419",
                        "35970",
                        "36002",
                        "36034",
                        "81446",
                        "84691",
                        "136507",
                        "136593",
                        "145303",
                        "157455",
                        "201367",
                        "201455",
                        "201519",
                        "201585",
                        "201649",
                        "201713",
                        "201777",
                        "201845",
                        "202043",
                        "210263",
                        "289869",
                        "162097",
                        "181889",
                        "181963",
                        "103993",
                        "182061",
                        "104025",
                        "182245",
                        "210",
                        "182533",
                        "2721",
                        "182675",
                        "4097",
                        "182749",
                        "103942",
                        "182821",
                        "103974",
                        "182913",
                        "379",
                        "183065",
                        "447",
                        "183309",
                        "932",
                        "183533",
                        "3031",
                        "183681",
                        "3575",
                        "183759",
                        "99599",
                        "183851",
                        "104409",
                        "183929",
                        "219059",
                        "184073",
                        "589",
                        "184151",
                        "621",
                        "184283",
                        "653",
                        "184429",
                        "685",
                        "184529",
                        "717",
                        "184617",
                        "749",
                        "185051",
                        "781",
                        "185233",
                        "813",
                        "185307",
                        "1340",
                        "245753",
                        "99644",
                        "245847",
                        "4758",
                        "245927",
                        "4780",
                        "246001",
                        "4812",
                        "246117",
                        "24638",
                        "246225",
                        "24670",
                        "246305",
                        "24745",
                        "246413",
                        "24685",
                        "246511",
                        "34787",
                        "246587",
                        "49921",
                        "246657",
                        "49953",
                        "246735",
                        "261009",
                        "246815",
                        "48345",
                        "246891",
                        "48377",
                        "246965",
                        "48409",
                        "247031",
                        "48441",
                        "247097",
                        "48311",
                        "118371",
                        "48096",
                        "120637",
                        "48128",
                        "120738",
                        "48160",
                        "4129",
                        "48192",
                        "6592",
                        "48224",
                        "48256",
                        "48288",
                        "5025",
                        "105069",
                        "105101",
                        "105133",
                        "105165",
                        "105197",
                        "105229",
                        "105261",
                        "105293",
                        "105325",
                        "20122",
                        "34323",
                        "34334",
                        "34366",
                        "34398",
                        "35949",
                        "35981",
                        "36013",
                        "36045",
                        "84670",
                        "121496",
                        "136549",
                        "136663",
                        "145371",
                        "157485",
                        "201389",
                        "201477",
                        "201543",
                        "201607",
                        "201671",
                        "201735",
                        "201801",
                        "201867",
                        "202137",
                        "210285",
                        "289891",
                        "181787",
                        "181917",
                        "181989",
                        "104004",
                        "182145",
                        "85",
                        "182371",
                        "265",
                        "182615",
                        "2737",
                        "182707",
                        "103921",
                        "182775",
                        "103953",
                        "182849",
                        "103985",
                        "182949",
                        "401",
                        "183121",
                        "463",
                        "183427",
                        "949",
                        "183569",
                        "3302",
                        "183715",
                        "3587",
                        "183791",
                        "99639",
                        "183881",
                        "104420",
                        "183965",
                        "564",
                        "184101",
                        "600",
                        "184175",
                        "632",
                        "184319",
                        "664",
                        "184455",
                        "696",
                        "184569",
                        "728",
                        "184641",
                        "760",
                        "185169",
                        "792",
                        "185265",
                        "824",
                        "245707",
                        "1352",
                        "245785",
                        "3044",
                        "245877",
                        "4815",
                        "245955",
                        "4791",
                        "246045",
                        "24617",
                        "246149",
                        "24649",
                        "246253",
                        "24724",
                        "246341",
                        "24756",
                        "246459",
                        "24696",
                        "246535",
                        "34798",
                        "246609",
                        "49932",
                        "246683",
                        "49964",
                        "246759",
                        "261031",
                        "246839",
                        "48356",
                        "246917",
                        "48388",
                        "246987",
                        "48420",
                        "247055",
                        "48452",
                        "247121",
                        "48321",
                        "118389",
                        "48107",
                        "120648",
                        "48139",
                        "287201",
                        "48171",
                        "4027",
                        "48203",
                        "48235",
                        "48267",
                        "48299",
                        "289833",
                        "105080",
                        "105112",
                        "105144",
                        "105176",
                        "105208",
                        "105240",
                        "105272",
                        "105304",
                        "105336",
                        "22486",
                        "34345",
                        "34377",
                        "34409",
                        "35960",
                        "35992",
                        "36024",
                        "81436",
                        "84681",
                        "136487",
                        "136573",
                        "144757",
                        "145393",
                        "201335",
                        "201435",
                        "201499",
                        "201565",
                        "201629",
                        "201693",
                        "201757",
                        "201825",
                        "201889",
                        "202163",
                        "289849",
                        "162039",
                        "181867",
                        "181943",
                        "14",
                        "182011",
                        "104015",
                        "182185",
                        "150",
                        "182503",
                        "318",
                        "182641",
                        "3020",
                        "182729",
                        "103932",
                        "182799",
                        "103964",
                        "182881",
                        "356",
                        "183011",
                        "421",
                        "183269",
                        "916",
                        "183505",
                        "1166",
                        "183603",
                        "3565",
                        "183737",
                        "11227",
                        "183831",
                        "104399",
                        "183905",
                        "104431",
                        "184027",
                        "579",
                        "184127",
                        "611",
                        "184201",
                        "643",
                        "184361",
                        "675",
                        "184501",
                        "707",
                        "184593",
                        "739",
                        "184981",
                        "771",
                        "185205",
                        "803",
                        "185287",
                        "1323",
                        "245731",
                        "4644",
                        "245815",
                        "12902",
                        "245905",
                        "4770",
                        "245979",
                        "4802",
                        "246087",
                        "24628",
                        "246185",
                        "24660",
                        "246281",
                        "24735",
                        "246373",
                        "24675",
                        "246489",
                        "24707",
                        "246563",
                        "49911",
                        "246637",
                        "49943",
                        "246709",
                        "260989",
                        "246793",
                        "48335",
                        "246865",
                        "48367",
                        "246941",
                        "48399",
                        "247011",
                        "48431",
                        "247077",
                        "48463",
                        "118247",
                        "48086",
                        "118430",
                        "48118",
                        "120721",
                        "48150",
                        "4119",
                        "48182",
                        "4224",
                        "48214",
                        "48246",
                        "48278",
                        "104613",
                        "105059",
                        "105091",
                        "105123",
                        "105155",
                        "105187",
                        "105219",
                        "105251",
                        "105283",
                        "105315",
                        "20112",
                        "22508",
                        "34356",
                        "34388",
                        "34420",
                        "35971",
                        "36003",
                        "36035",
                        "81447",
                        "84692",
                        "136527",
                        "136595",
                        "145305",
                        "157457",
                        "201369",
                        "201457",
                        "201521",
                        "201587",
                        "201651",
                        "201715",
                        "201779",
                        "201847",
                        "202045",
                        "210265",
                        "289871",
                        "162101",
                        "181891",
                        "181965",
                        "103994",
                        "182067",
                        "104026",
                        "182267",
                        "213",
                        "182535",
                        "2722",
                        "182677",
                        "6115",
                        "182751",
                        "103943",
                        "182823",
                        "103975",
                        "182915",
                        "380",
                        "183067",
                        "450",
                        "183327",
                        "933",
                        "183535",
                        "3032",
                        "183683",
                        "3576",
                        "183763",
                        "99600",
                        "183855",
                        "104410",
                        "183931",
                        "219061",
                        "184077",
                        "590",
                        "184153",
                        "622",
                        "184285",
                        "654",
                        "184431",
                        "686",
                        "184533",
                        "718",
                        "184619",
                        "750",
                        "185057",
                        "782",
                        "185235",
                        "814",
                        "185309",
                        "1341",
                        "245755",
                        "99645",
                        "245849",
                        "4759",
                        "245929",
                        "4781",
                        "246003",
                        "4813",
                        "246121",
                        "24639",
                        "246229",
                        "24671",
                        "246311",
                        "24746",
                        "246419",
                        "24686",
                        "246513",
                        "34788",
                        "246589",
                        "49922",
                        "246661",
                        "49954",
                        "246737",
                        "261011",
                        "246817",
                        "48346",
                        "246893",
                        "48378",
                        "246967",
                        "48410",
                        "247033",
                        "48442",
                        "247099",
                        "48312",
                        "118372",
                        "48097",
                        "120638",
                        "48129",
                        "120739",
                        "48161",
                        "4130",
                        "48193",
                        "280585",
                        "48225",
                        "48257",
                        "48289",
                        "104030",
                        "105070",
                        "105102",
                        "105134",
                        "105166",
                        "105198",
                        "105230",
                        "105262",
                        "105294",
                        "105326",
                        "20123",
                        "34324",
                        "34335",
                        "34367",
                        "34399",
                        "35950",
                        "35982",
                        "36014",
                        "36046",
                        "84671",
                        "121497",
                        "136551",
                        "136665",
                        "145373",
                        "157487",
                        "201391",
                        "201479",
                        "201545",
                        "201609",
                        "201673",
                        "201737",
                        "201803",
                        "201869",
                        "202139",
                        "210287",
                        "289893",
                        "181789",
                        "181919",
                        "2",
                        "181991",
                        "104005",
                        "182147",
                        "92",
                        "182373",
                        "270",
                        "182617",
                        "3010",
                        "182709",
                        "103922",
                        "182777",
                        "103954",
                        "182853",
                        "103986",
                        "182951",
                        "402",
                        "183131",
                        "464",
                        "183433",
                        "950",
                        "183571",
                        "3303",
                        "183717",
                        "3588",
                        "183793",
                        "104389",
                        "183883",
                        "104421",
                        "183967",
                        "565",
                        "184103",
                        "601",
                        "184177",
                        "633",
                        "184321",
                        "665",
                        "184457",
                        "697",
                        "184573",
                        "729",
                        "184643",
                        "761",
                        "185173",
                        "793",
                        "185267",
                        "825",
                        "245709",
                        "1354",
                        "245787",
                        "3045",
                        "245879",
                        "4816",
                        "245957",
                        "4792",
                        "246055",
                        "24618",
                        "246153",
                        "24650",
                        "246255",
                        "24725",
                        "246343",
                        "24757",
                        "246461",
                        "24697",
                        "246537",
                        "34799",
                        "246611",
                        "49933",
                        "246685",
                        "49965",
                        "246763",
                        "261033",
                        "246841",
                        "48357",
                        "246919",
                        "48389",
                        "246989",
                        "48421",
                        "247057",
                        "48453",
                        "114391",
                        "48322",
                        "118420",
                        "48108",
                        "120649",
                        "48140",
                        "287203",
                        "48172",
                        "4029",
                        "48204",
                        "48236",
                        "48268",
                        "48300",
                        "289835",
                        "105081",
                        "105113",
                        "105145",
                        "105177",
                        "105209",
                        "105241",
                        "105273",
                        "105305",
                        "105337",
                        "22488",
                        "34346",
                        "34378",
                        "34410",
                        "35961",
                        "35993",
                        "36025",
                        "81437",
                        "84682",
                        "136489",
                        "136575",
                        "144761",
                        "145395",
                        "201337",
                        "201437",
                        "201501",
                        "201567",
                        "201631",
                        "201695",
                        "201759",
                        "201827",
                        "201891",
                        "202165",
                        "289851",
                        "162043",
                        "181869",
                        "181945",
                        "15",
                        "182017",
                        "104016",
                        "182189",
                        "152",
                        "182505",
                        "322",
                        "182645",
                        "3021",
                        "182731",
                        "103933",
                        "182801",
                        "103965",
                        "182883",
                        "357",
                        "183013",
                        "422",
                        "183273",
                        "917",
                        "183511",
                        "2720",
                        "183605",
                        "3566",
                        "183739",
                        "99585",
                        "183833",
                        "104400",
                        "183907",
                        "104432",
                        "184029",
                        "580",
                        "184129",
                        "612",
                        "184203",
                        "644",
                        "184365",
                        "676",
                        "184503",
                        "708",
                        "184595",
                        "740",
                        "184985",
                        "772",
                        "185207",
                        "804",
                        "185289",
                        "1324",
                        "245733",
                        "4645",
                        "245819",
                        "12905",
                        "245907",
                        "4771",
                        "245981",
                        "4803",
                        "246089",
                        "24629",
                        "246193",
                        "24661",
                        "246285",
                        "24736",
                        "246377",
                        "24676",
                        "246491",
                        "24708",
                        "246565",
                        "49912",
                        "246639",
                        "49944",
                        "246711",
                        "260991",
                        "246795",
                        "48336",
                        "246867",
                        "48368",
                        "246943",
                        "48400",
                        "247013",
                        "48432",
                        "247079",
                        "48464",
                        "118249",
                        "48087",
                        "118431",
                        "48119",
                        "120722",
                        "48151",
                        "4120",
                        "48183",
                        "4231",
                        "48215",
                        "48247",
                        "48279",
                        "104614",
                        "105060",
                        "105092",
                        "105124",
                        "105156",
                        "105188",
                        "105220",
                        "105252",
                        "105284",
                        "105316",
                        "20113",
                        "22510",
                        "34357",
                        "34389",
                        "34540",
                        "35972",
                        "36004",
                        "36036",
                        "84557",
                        "84693",
                        "136529",
                        "136599",
                        "145307",
                        "157459",
                        "201371",
                        "201459",
                        "201523",
                        "201589",
                        "201653",
                        "201717",
                        "201781",
                        "201849",
                        "202047",
                        "210267",
                        "289873",
                        "162105",
                        "181893",
                        "181967",
                        "103995",
                        "182069",
                        "104027",
                        "182271",
                        "218",
                        "182539",
                        "2723",
                        "182679",
                        "6116",
                        "182753",
                        "103944",
                        "182825",
                        "103976",
                        "182917",
                        "384",
                        "183073",
                        "451",
                        "183329",
                        "934",
                        "183537",
                        "3033",
                        "183685",
                        "3577",
                        "183773",
                        "99601",
                        "183859",
                        "104411",
                        "183933",
                        "219063",
                        "184081",
                        "591",
                        "184157",
                        "623",
                        "184289",
                        "655",
                        "184433",
                        "687",
                        "184539",
                        "719",
                        "184621",
                        "751",
                        "185097",
                        "783",
                        "185237",
                        "815",
                        "185311",
                        "1342",
                        "245757",
                        "99646",
                        "245851",
                        "4760",
                        "245931",
                        "4782",
                        "246005",
                        "4814",
                        "246123",
                        "24640",
                        "246231",
                        "24672",
                        "246313",
                        "24747",
                        "246423",
                        "24687",
                        "246515",
                        "34789",
                        "246591",
                        "49923",
                        "246663",
                        "49955",
                        "246739",
                        "261013",
                        "246819",
                        "48347",
                        "246895",
                        "48379",
                        "246969",
                        "48411",
                        "247035",
                        "48443",
                        "247103",
                        "48313",
                        "118373",
                        "48098",
                        "120639",
                        "48130",
                        "120740",
                        "48162",
                        "4131",
                        "48194",
                        "48226",
                        "48258",
                        "48290",
                        "104259",
                        "105071",
                        "105103",
                        "105135",
                        "105167",
                        "105199",
                        "105231",
                        "105263",
                        "105295",
                        "105327",
                        "20124",
                        "34325",
                        "34336",
                        "34368",
                        "34400",
                        "35951",
                        "35983",
                        "36015",
                        "36047",
                        "84672",
                        "121498",
                        "136555",
                        "144703",
                        "145375",
                        "157489",
                        "201393",
                        "201481",
                        "201547",
                        "201611",
                        "201675",
                        "201739",
                        "201805",
                        "201871",
                        "202141",
                        "210289",
                        "289895",
                        "181849",
                        "181921",
                        "3",
                        "181993",
                        "104006",
                        "182149",
                        "104",
                        "182375",
                        "275",
                        "182621",
                        "3011",
                        "182711",
                        "103923",
                        "182781",
                        "103955",
                        "182855",
                        "103987",
                        "182953",
                        "403",
                        "183133",
                        "465",
                        "183435",
                        "951",
                        "183573",
                        "3308",
                        "183719",
                        "3592",
                        "183797",
                        "104390",
                        "183885",
                        "104422",
                        "183969",
                        "566",
                        "184105",
                        "602",
                        "184179",
                        "634",
                        "184325",
                        "666",
                        "184473",
                        "698",
                        "184575",
                        "730",
                        "184645",
                        "762",
                        "185175",
                        "794",
                        "185269",
                        "826",
                        "245711",
                        "1357",
                        "245789",
                        "3046",
                        "245881",
                        "4750",
                        "245959",
                        "4793",
                        "246063",
                        "24619",
                        "246155",
                        "24651",
                        "246257",
                        "24726",
                        "246345",
                        "24758",
                        "246465",
                        "24698",
                        "246539",
                        "34800",
                        "246613",
                        "49934",
                        "246687",
                        "49966",
                        "246765",
                        "261035",
                        "246843",
                        "48358",
                        "246921",
                        "48390",
                        "246991",
                        "48422",
                        "247059",
                        "48454",
                        "114392",
                        "48323",
                        "118421",
                        "48109",
                        "120650",
                        "48141",
                        "287205",
                        "48173",
                        "4030",
                        "48205",
                        "48237",
                        "48269",
                        "48301",
                        "289837",
                        "105082",
                        "105114",
                        "105146",
                        "105178",
                        "105210",
                        "105242",
                        "105274",
                        "105306",
                        "105338",
                        "22490",
                        "34347",
                        "34379",
                        "34411",
                        "35962",
                        "35994",
                        "36026",
                        "81438",
                        "84683",
                        "136491",
                        "136577",
                        "144765",
                        "145397",
                        "201341",
                        "201439",
                        "201503",
                        "201569",
                        "201633",
                        "201697",
                        "201761",
                        "201829",
                        "201893",
                        "202167",
                        "289853",
                        "162047",
                        "181873",
                        "181947",
                        "16",
                        "182031",
                        "104017",
                        "182193",
                        "154",
                        "182509",
                        "323",
                        "182649",
                        "3022",
                        "182733",
                        "103934",
                        "182803",
                        "103966",
                        "182885",
                        "358",
                        "183015",
                        "429",
                        "183279",
                        "919",
                        "183513",
                        "3023",
                        "183665",
                        "3567",
                        "183741",
                        "99586",
                        "183835",
                        "104401",
                        "183911",
                        "104433",
                        "184031",
                        "581",
                        "184135",
                        "613",
                        "184205",
                        "645",
                        "184385",
                        "677",
                        "184505",
                        "709",
                        "184597",
                        "741",
                        "184987",
                        "773",
                        "185209",
                        "805",
                        "185291",
                        "1325",
                        "245735",
                        "4646",
                        "245821",
                        "12906",
                        "245909",
                        "4772",
                        "245985",
                        "4804",
                        "246091",
                        "24630",
                        "246195",
                        "24662",
                        "246287",
                        "24737",
                        "246379",
                        "24677",
                        "246493",
                        "24709",
                        "246567",
                        "49913",
                        "246641",
                        "49945",
                        "246713",
                        "260993",
                        "246797",
                        "48337",
                        "246869",
                        "48369",
                        "246947",
                        "48401",
                        "247015",
                        "48433",
                        "247081",
                        "48465",
                        "118250",
                        "48088",
                        "118433",
                        "48120",
                        "120723",
                        "48152",
                        "4121",
                        "48184",
                        "6583",
                        "48216",
                        "48248",
                        "48280",
                        "104615",
                        "105061",
                        "105093",
                        "105125",
                        "105157",
                        "105189",
                        "105221",
                        "105253",
                        "105285",
                        "105317",
                        "20114",
                        "22512",
                        "34358",
                        "34390",
                        "35360",
                        "35973",
                        "36005",
                        "36037",
                        "84558",
                        "84694",
                        "136531",
                        "136603",
                        "145309",
                        "157469",
                        "201373",
                        "201461",
                        "201525",
                        "201591",
                        "201655",
                        "201719",
                        "201785",
                        "201851",
                        "202049",
                        "210269",
                        "289875",
                        "162109",
                        "181895",
                        "181969",
                        "103996",
                        "182071",
                        "104028",
                        "182275",
                        "224",
                        "182541",
                        "2724",
                        "182685",
                        "6906",
                        "182759",
                        "103945",
                        "182827",
                        "103977",
                        "182919",
                        "385",
                        "183075",
                        "453",
                        "183335",
                        "935",
                        "183539",
                        "3034",
                        "183689",
                        "3578",
                        "183775",
                        "99602",
                        "183861",
                        "104412",
                        "183935",
                        "219065",
                        "184083",
                        "592",
                        "184159",
                        "624",
                        "184291",
                        "656",
                        "184435",
                        "688",
                        "184541",
                        "720",
                        "184623",
                        "752",
                        "185109",
                        "784",
                        "185241",
                        "816",
                        "185313",
                        "1344",
                        "245763",
                        "1087",
                        "245853",
                        "4761",
                        "245933",
                        "4783",
                        "246007",
                        "24609",
                        "246127",
                        "24641",
                        "246233",
                        "24716",
                        "246315",
                        "24748",
                        "246429",
                        "24688",
                        "246517",
                        "34790",
                        "246593",
                        "49924",
                        "246667",
                        "49956",
                        "246741",
                        "261015",
                        "246823",
                        "48348",
                        "246901",
                        "48380",
                        "246971",
                        "48412",
                        "247037",
                        "48444",
                        "247105",
                        "48314",
                        "118374",
                        "48099",
                        "120640",
                        "48131",
                        "120741",
                        "48163",
                        "4132",
                        "48195",
                        "48227",
                        "48259",
                        "48291",
                        "104608",
                        "105072",
                        "105104",
                        "105136",
                        "105168",
                        "105200",
                        "105232",
                        "105264",
                        "105296",
                        "105328",
                        "20125",
                        "34326",
                        "34337",
                        "34369",
                        "34401",
                        "35952",
                        "35984",
                        "36016",
                        "36048",
                        "84673",
                        "121499",
                        "136557",
                        "144713",
                        "145377",
                        "157491",
                        "201395",
                        "201483",
                        "201549",
                        "201613",
                        "201677",
                        "201741",
                        "201807",
                        "201873",
                        "202145",
                        "210291",
                        "289897",
                        "181851",
                        "181923",
                        "4",
                        "181995",
                        "104007",
                        "182151",
                        "118",
                        "182383",
                        "302",
                        "182623",
                        "3012",
                        "182713",
                        "103924",
                        "182783",
                        "103956",
                        "182857",
                        "103988",
                        "182965",
                        "404",
                        "183135",
                        "467",
                        "183437",
                        "952",
                        "183575",
                        "3309",
                        "183721",
                        "3593",
                        "183805",
                        "104391",
                        "183887",
                        "104423",
                        "183981",
                        "567",
                        "184107",
                        "603",
                        "184181",
                        "635",
                        "184329",
                        "667",
                        "184475",
                        "699",
                        "184577",
                        "731",
                        "184647",
                        "763",
                        "185181",
                        "795",
                        "185271",
                        "827",
                        "245715",
                        "3226",
                        "245795",
                        "3048",
                        "245883",
                        "4751",
                        "245961",
                        "4794",
                        "246065",
                        "24620",
                        "246159",
                        "24652",
                        "246259",
                        "24727",
                        "246349",
                        "24759",
                        "246469",
                        "24699",
                        "246545",
                        "34801",
                        "246615",
                        "49935",
                        "246689",
                        "260973",
                        "246767",
                        "261037",
                        "246845",
                        "48359",
                        "246923",
                        "48391",
                        "246993",
                        "48423",
                        "247061",
                        "48455",
                        "118222",
                        "48324",
                        "118422",
                        "48110",
                        "120713",
                        "48142",
                        "4111",
                        "48174",
                        "4031",
                        "48206",
                        "48238",
                        "48270",
                        "48302",
                        "289839",
                        "105083",
                        "105115",
                        "105147",
                        "105179",
                        "105211",
                        "105243",
                        "105275",
                        "105307",
                        "287167",
                        "22492",
                        "34348",
                        "34380",
                        "34412",
                        "35963",
                        "35995",
                        "36027",
                        "81439",
                        "84684",
                        "136493",
                        "136579",
                        "145289",
                        "145399",
                        "201343",
                        "201441",
                        "201505",
                        "201571",
                        "201635",
                        "201699",
                        "201763",
                        "201831",
                        "201895",
                        "202169",
                        "289855",
                        "162053",
                        "181875",
                        "181949",
                        "98942",
                        "182033",
                        "104018",
                        "182205",
                        "155",
                        "182511",
                        "325",
                        "182653",
                        "3055",
                        "182735",
                        "103935",
                        "182805",
                        "103967",
                        "182887",
                        "359",
                        "183017",
                        "430",
                        "183281",
                        "920",
                        "183517",
                        "3024",
                        "183667",
                        "3568",
                        "183743",
                        "99592",
                        "183837",
                        "104402",
                        "183913",
                        "104434",
                        "184033",
                        "582",
                        "184137",
                        "614",
                        "184209",
                        "646",
                        "184387",
                        "678",
                        "184507",
                        "710",
                        "184599",
                        "742",
                        "184989",
                        "774",
                        "185211",
                        "806",
                        "185293",
                        "1326",
                        "245737",
                        "4648",
                        "245823",
                        "12914",
                        "245911",
                        "4773",
                        "245987",
                        "4805",
                        "246093",
                        "24631",
                        "246197",
                        "24663",
                        "246289",
                        "24738",
                        "246385",
                        "24678",
                        "246495",
                        "24710",
                        "246571",
                        "49914",
                        "246643",
                        "49946",
                        "246715",
                        "260995",
                        "246801",
                        "48338",
                        "246875",
                        "48370",
                        "246949",
                        "48402",
                        "247017",
                        "48434",
                        "247083",
                        "48468",
                        "118251",
                        "48089",
                        "118434",
                        "48121",
                        "120724",
                        "48153",
                        "4122",
                        "48185",
                        "6584",
                        "48217",
                        "48249",
                        "48281",
                        "104616",
                        "105062",
                        "105094",
                        "105126",
                        "105158",
                        "105190",
                        "105222",
                        "105254",
                        "105286",
                        "105318",
                        "20115",
                        "34316",
                        "34359",
                        "34391",
                        "35361",
                        "35974",
                        "36006",
                        "36038",
                        "84559",
                        "84695",
                        "136533",
                        "136649",
                        "145311",
                        "157471",
                        "201375",
                        "201463",
                        "201527",
                        "201593",
                        "201657",
                        "201721",
                        "201787",
                        "201853",
                        "202123",
                        "210271",
                        "289877",
                        "162111",
                        "181897",
                        "181971",
                        "103997",
                        "182073",
                        "104029",
                        "182281",
                        "225",
                        "182585",
                        "2725",
                        "182687",
                        "98867",
                        "182761",
                        "103946",
                        "182829",
                        "103978",
                        "182921",
                        "386",
                        "183081",
                        "454",
                        "183341",
                        "937",
                        "183541",
                        "3035",
                        "183693",
                        "3579",
                        "183777",
                        "99603",
                        "183863",
                        "104413",
                        "183937",
                        "219067",
                        "184085",
                        "593",
                        "184161",
                        "625",
                        "184297",
                        "657",
                        "184437",
                        "689",
                        "184543",
                        "721",
                        "184625",
                        "753",
                        "185111",
                        "785",
                        "185249",
                        "817",
                        "185315",
                        "1345",
                        "245765",
                        "1088",
                        "245859",
                        "4762",
                        "245937",
                        "4784",
                        "246009",
                        "24610",
                        "246129",
                        "24642",
                        "246235",
                        "24717",
                        "246319",
                        "24749",
                        "246433",
                        "24689",
                        "246521",
                        "34791",
                        "246595",
                        "49925",
                        "246669",
                        "49957",
                        "246743",
                        "261017",
                        "246825",
                        "48349",
                        "246903",
                        "48381",
                        "246973",
                        "48413",
                        "247039",
                        "48445",
                        "247107",
                        "48315",
                        "118375",
                        "48100",
                        "120641",
                        "48132",
                        "120742",
                        "48164",
                        "4133",
                        "48196",
                        "48228",
                        "48260",
                        "48292",
                        "104609",
                        "105073",
                        "105105",
                        "105137",
                        "105169",
                        "105201",
                        "105233",
                        "105265",
                        "105297",
                        "105329",
                        "20126",
                        "34327",
                        "34338",
                        "34370",
                        "34402",
                        "35953",
                        "35985",
                        "36017",
                        "36049",
                        "84674",
                        "121500",
                        "136559",
                        "144729",
                        "145379",
                        "157493",
                        "201397",
                        "201485",
                        "201551",
                        "201615",
                        "201679",
                        "201743",
                        "201809",
                        "201875",
                        "202147",
                        "210293",
                        "289899",
                        "181853",
                        "181925",
                        "5",
                        "181997",
                        "104008",
                        "182153",
                        "129",
                        "182387",
                        "305",
                        "182625",
                        "3013",
                        "182715",
                        "103925",
                        "182785",
                        "103957",
                        "182863",
                        "103989",
                        "182967",
                        "405",
                        "183153",
                        "895",
                        "183441",
                        "953",
                        "183583",
                        "3388",
                        "183723",
                        "3595",
                        "183807",
                        "104392",
                        "183889",
                        "104424",
                        "183983",
                        "568",
                        "184109",
                        "604",
                        "184183",
                        "636",
                        "184331",
                        "668",
                        "184479",
                        "700",
                        "184579",
                        "732",
                        "184649",
                        "764",
                        "185183",
                        "796",
                        "185273",
                        "1069",
                        "245717",
                        "3227",
                        "245797",
                        "3049",
                        "245887",
                        "4744",
                        "245963",
                        "4795",
                        "246067",
                        "24621",
                        "246161",
                        "24653",
                        "246261",
                        "24728",
                        "246351",
                        "24760",
                        "246471",
                        "24700",
                        "246547",
                        "34802",
                        "246617",
                        "49936",
                        "246691",
                        "260975",
                        "246769",
                        "261039",
                        "246849",
                        "48360",
                        "246925",
                        "48392",
                        "246995",
                        "48424",
                        "247063",
                        "48456",
                        "118223",
                        "48325",
                        "118423",
                        "48111",
                        "120714",
                        "48143",
                        "4112",
                        "48175",
                        "4056",
                        "48207",
                        "48239",
                        "48271",
                        "48303",
                        "289841",
                        "105084",
                        "105116",
                        "105148",
                        "105180",
                        "105212",
                        "105244",
                        "105276",
                        "105308",
                        "287169",
                        "22494",
                        "34349",
                        "34381",
                        "34413",
                        "35964",
                        "35996",
                        "36028",
                        "81440",
                        "84685",
                        "136495",
                        "136581",
                        "145291",
                        "145401",
                        "201345",
                        "201443",
                        "201507",
                        "201573",
                        "201637",
                        "201701",
                        "201765",
                        "201833",
                        "201897",
                        "202171",
                        "289857",
                        "162073",
                        "181877",
                        "181951",
                        "98943",
                        "182035",
                        "104019",
                        "182211",
                        "158",
                        "182517",
                        "326",
                        "182655",
                        "3076",
                        "182737",
                        "103936",
                        "182807",
                        "103968",
                        "182889",
                        "363",
                        "183045",
                        "432",
                        "183283",
                        "922",
                        "183521",
                        "3025",
                        "183669",
                        "3569",
                        "183747",
                        "99593",
                        "183839",
                        "104403",
                        "183915",
                        "104435",
                        "184061",
                        "583",
                        "184139",
                        "615",
                        "184211",
                        "647",
                        "184389",
                        "679",
                        "184509",
                        "711",
                        "184601",
                        "743",
                        "185001",
                        "775",
                        "185213",
                        "807",
                        "185295",
                        "1327",
                        "245739",
                        "4649",
                        "245825",
                        "4752",
                        "245913",
                        "4774",
                        "245989",
                        "4806",
                        "246097",
                        "24632",
                        "246199",
                        "24664",
                        "246291",
                        "24739",
                        "246389",
                        "24679",
                        "246497",
                        "24711",
                        "246573",
                        "49915",
                        "246645",
                        "49947",
                        "246717",
                        "260997",
                        "246803",
                        "48339",
                        "246877",
                        "48371",
                        "246951",
                        "48403",
                        "247019",
                        "48435",
                        "247085",
                        "48305",
                        "118252",
                        "48090",
                        "118436",
                        "48122",
                        "120725",
                        "48154",
                        "4123",
                        "48186",
                        "6585",
                        "48218",
                        "48250",
                        "48282",
                        "104617",
                        "105063",
                        "105095",
                        "105127",
                        "105159",
                        "105191",
                        "105223",
                        "105255",
                        "105287",
                        "105319",
                        "20116",
                        "34317",
                        "34360",
                        "34392",
                        "35362",
                        "35975",
                        "36007",
                        "36039",
                        "84560",
                        "84696",
                        "136535",
                        "136651",
                        "145313",
                        "157473",
                        "201377",
                        "201465",
                        "201529",
                        "201595",
                        "201659",
                        "201723",
                        "201789",
                        "201855",
                        "202125",
                        "210273",
                        "289879",
                        "162119",
                        "181899",
                        "181973",
                        "103998",
                        "182101",
                        "26",
                        "182295",
                        "226",
                        "182603",
                        "2726",
                        "182693",
                        "98868",
                        "182763",
                        "103947",
                        "182831",
                        "103979",
                        "182925",
                        "389",
                        "183085",
                        "456",
                        "183349",
                        "938",
                        "183545",
                        "3036",
                        "183699",
                        "3580",
                        "183779",
                        "99604",
                        "183865",
                        "104414",
                        "183941",
                        "219069",
                        "184087",
                        "594",
                        "184163",
                        "626",
                        "184299",
                        "658",
                        "184439",
                        "690",
                        "184551",
                        "722",
                        "184629",
                        "754",
                        "185113",
                        "786",
                        "185253",
                        "818",
                        "209769",
                        "1346",
                        "245767",
                        "1089",
                        "245861",
                        "4763",
                        "245941",
                        "4785",
                        "246011",
                        "24611",
                        "246131",
                        "24643",
                        "246241",
                        "24718",
                        "246321",
                        "24750",
                        "246445",
                        "24690",
                        "246523",
                        "34792",
                        "246597",
                        "49926",
                        "246671",
                        "49958",
                        "246747",
                        "261019",
                        "246827",
                        "48350",
                        "246905",
                        "48382",
                        "246975",
                        "48414",
                        "247041",
                        "48446",
                        "247109",
                        "48316",
                        "118376",
                        "48101",
                        "120642",
                        "48133",
                        "120743",
                        "48165",
                        "284677",
                        "48197",
                        "48229",
                        "48261",
                        "48293",
                        "289821",
                        "105074",
                        "105106",
                        "105138",
                        "105170",
                        "105202",
                        "105234",
                        "105266",
                        "105298",
                        "105330",
                        "22474",
                        "34328",
                        "34339",
                        "34371",
                        "34403",
                        "35954",
                        "35986",
                        "36018",
                        "36050",
                        "84675",
                        "121501",
                        "136561",
                        "144741",
                        "145381",
                        "201313",
                        "201405",
                        "201487",
                        "201553",
                        "201617",
                        "201681",
                        "201745",
                        "201811",
                        "201877",
                        "202149",
                        "210295",
                        "289901",
                        "181855",
                        "181927",
                        "6",
                        "181999",
                        "104009",
                        "182157",
                        "131",
                        "182395",
                        "311",
                        "182627",
                        "3014",
                        "182717",
                        "103926",
                        "182787",
                        "103958",
                        "182865",
                        "103990",
                        "182969",
                        "410",
                        "183167",
                        "898",
                        "183443",
                        "955",
                        "183585",
                        "3389",
                        "183725",
                        "3596",
                        "183809",
                        "104393",
                        "183891",
                        "104425",
                        "183985",
                        "569",
                        "184113",
                        "605",
                        "184185",
                        "637",
                        "184343",
                        "669",
                        "184481",
                        "701",
                        "184581",
                        "733",
                        "184655",
                        "765",
                        "185191",
                        "797",
                        "185275",
                        "1190",
                        "245719",
                        "3228",
                        "245799",
                        "3051",
                        "245889",
                        "4745",
                        "245965",
                        "4796",
                        "246069",
                        "24622",
                        "246165",
                        "24654",
                        "246263",
                        "24729",
                        "246353",
                        "24761",
                        "246473",
                        "24701",
                        "246551",
                        "34803",
                        "246619",
                        "49937",
                        "246693",
                        "260977",
                        "246771",
                        "48329",
                        "246853",
                        "48361",
                        "246927",
                        "48393",
                        "246997",
                        "48425",
                        "247065",
                        "48457",
                        "118231",
                        "48326",
                        "118424",
                        "48112",
                        "120715",
                        "48144",
                        "4113",
                        "48176",
                        "4059",
                        "48208",
                        "48240",
                        "48272",
                        "48485",
                        "289843",
                        "105085",
                        "105117",
                        "105149",
                        "105181",
                        "105213",
                        "105245",
                        "105277",
                        "105309",
                        "287171",
                        "22496",
                        "34350",
                        "34382",
                        "34414",
                        "35965",
                        "35997",
                        "36029",
                        "81441",
                        "84686",
                        "136497",
                        "136583",
                        "145293",
                        "145403",
                        "201349",
                        "201445",
                        "201509",
                        "201575",
                        "201639",
                        "201703",
                        "201767",
                        "201835",
                        "201899",
                        "202173",
                        "289859",
                        "162077",
                        "181879",
                        "181953",
                        "98944",
                        "182039",
                        "104020",
                        "182213",
                        "160",
                        "182519",
                        "2623",
                        "182659",
                        "3077",
                        "182739",
                        "103937",
                        "182811",
                        "103969",
                        "182897",
                        "366",
                        "183053",
                        "433",
                        "183291",
                        "923",
                        "183523",
                        "3026",
                        "183671",
                        "3570",
                        "183749",
                        "99594",
                        "183841",
                        "104404",
                        "183919",
                        "219049",
                        "184063",
                        "584",
                        "184141",
                        "616",
                        "184217",
                        "648",
                        "184391",
                        "680",
                        "184511",
                        "712",
                        "184603",
                        "744",
                        "185037",
                        "776",
                        "185215",
                        "808",
                        "185297",
                        "1328",
                        "245741",
                        "4650",
                        "245827",
                        "4753",
                        "245915",
                        "4775",
                        "245991",
                        "4807",
                        "246099",
                        "24633",
                        "246203",
                        "24665",
                        "246293",
                        "24740",
                        "246391",
                        "24680",
                        "246501",
                        "24712",
                        "246575",
                        "49916",
                        "246647",
                        "49948",
                        "246719",
                        "260999",
                        "246805",
                        "48340",
                        "246879",
                        "48372",
                        "246953",
                        "48404",
                        "247021",
                        "48436",
                        "247087",
                        "48306",
                        "118254",
                        "48091",
                        "118437",
                        "48123",
                        "120726",
                        "48155",
                        "4124",
                        "48187",
                        "6586",
                        "48219",
                        "48251",
                        "48283",
                        "104618",
                        "105064",
                        "105096",
                        "105128",
                        "105160",
                        "105192",
                        "105224",
                        "105256",
                        "105288",
                        "105320",
                        "20117",
                        "34318",
                        "34361",
                        "34393",
                        "35363",
                        "35976",
                        "36008",
                        "36040",
                        "84634",
                        "84697",
                        "136537",
                        "136653",
                        "145315",
                        "157475",
                        "201379",
                        "201467",
                        "201531",
                        "201597",
                        "201661",
                        "201725",
                        "201791",
                        "201857",
                        "202127",
                        "210275",
                        "289881",
                        "162127",
                        "181903",
                        "181975",
                        "103999",
                        "182103",
                        "32",
                        "182301",
                        "230",
                        "182605",
                        "2727",
                        "182695",
                        "99038",
                        "182765",
                        "103948",
                        "182837",
                        "103980",
                        "182939",
                        "395",
                        "183093",
                        "457",
                        "183411",
                        "939",
                        "183547",
                        "3037",
                        "183703",
                        "3581",
                        "183781",
                        "99605",
                        "183869",
                        "104415",
                        "183955",
                        "219071",
                        "184089",
                        "595",
                        "184165",
                        "627",
                        "184303",
                        "659",
                        "184441",
                        "691",
                        "184555",
                        "723",
                        "184631",
                        "755",
                        "185117",
                        "787",
                        "185255",
                        "819",
                        "209775",
                        "1347",
                        "245769",
                        "2062",
                        "245865",
                        "4764",
                        "245945",
                        "4786",
                        "246013",
                        "24612",
                        "246133",
                        "24644",
                        "246243",
                        "24719",
                        "246323",
                        "24751",
                        "246447",
                        "24691",
                        "246525",
                        "34793",
                        "246599",
                        "49927",
                        "246673",
                        "49959",
                        "246749",
                        "261021",
                        "246829",
                        "48351",
                        "246907",
                        "48383",
                        "246977",
                        "48415",
                        "247043",
                        "48447",
                        "247111",
                        "48317",
                        "118377",
                        "48102",
                        "120643",
                        "48134",
                        "120744",
                        "48166",
                        "284679",
                        "48198",
                        "48230",
                        "48262",
                        "48294",
                        "289823",
                        "105075",
                        "105107",
                        "105139",
                        "105171",
                        "105203",
                        "105235",
                        "105267",
                        "105299",
                        "105331",
                        "22476",
                        "34329",
                        "34340",
                        "34372",
                        "34404",
                        "35955",
                        "35987",
                        "36019",
                        "36051",
                        "84676",
                        "121502",
                        "136563",
                        "144743",
                        "145383",
                        "201315",
                        "201407",
                        "201489",
                        "201555",
                        "201619",
                        "201683",
                        "201747",
                        "201815",
                        "201879",
                        "202151",
                        "210297",
                        "289903",
                        "181857",
                        "181929",
                        "7",
                        "182001",
                        "104010",
                        "182159",
                        "141",
                        "182419",
                        "313",
                        "182629",
                        "3015",
                        "182719",
                        "103927",
                        "182789",
                        "103959",
                        "182867",
                        "103991",
                        "182975",
                        "411",
                        "183175",
                        "899",
                        "183447",
                        "956",
                        "183587",
                        "3418",
                        "183727",
                        "3597",
                        "183811",
                        "104394",
                        "183895",
                        "104426",
                        "183991",
                        "570",
                        "184115",
                        "606",
                        "184187",
                        "638",
                        "184345",
                        "670",
                        "184487",
                        "702",
                        "184583",
                        "734",
                        "184657",
                        "766",
                        "185193",
                        "798",
                        "185277",
                        "1191",
                        "245721",
                        "3229",
                        "245801",
                        "3450",
                        "245893",
                        "4746",
                        "245967",
                        "4797",
                        "246071",
                        "24623",
                        "246175",
                        "24655",
                        "246265",
                        "24730",
                        "246355",
                        "24762",
                        "246477",
                        "24702",
                        "246553",
                        "34804",
                        "246621",
                        "49938",
                        "246697",
                        "260979",
                        "246773",
                        "48330",
                        "246855",
                        "48362",
                        "246929",
                        "48394",
                        "246999",
                        "48426",
                        "247067",
                        "48458",
                        "118240",
                        "48327",
                        "118425",
                        "48113",
                        "120716",
                        "48145",
                        "4114",
                        "48177",
                        "4136",
                        "48209",
                        "48241",
                        "48273",
                        "48486",
                        "289845",
                        "105086",
                        "105118",
                        "105150",
                        "105182",
                        "105214",
                        "105246",
                        "105278",
                        "105310",
                        "20107",
                        "22498",
                        "34351",
                        "34383",
                        "34415",
                        "35966",
                        "35998",
                        "36030",
                        "81442",
                        "84687",
                        "136499",
                        "136585",
                        "145295",
                        "145405",
                        "201351",
                        "201447",
                        "201511",
                        "201577",
                        "201641",
                        "201705",
                        "201769",
                        "201837",
                        "201901",
                        "202175",
                        "289861",
                        "162081",
                        "181881",
                        "181955",
                        "98945",
                        "182043",
                        "104021",
                        "182223",
                        "165",
                        "182525",
                        "2635",
                        "182661",
                        "3085",
                        "182741",
                        "103938",
                        "182813",
                        "103970",
                        "182903",
                        "367",
                        "183055",
                        "434",
                        "183293",
                        "928",
                        "183525",
                        "3027",
                        "183673",
                        "3571",
                        "183751",
                        "99595",
                        "183843",
                        "104405",
                        "183921",
                        "219051",
                        "184065",
                        "585",
                        "184143",
                        "617",
                        "184219",
                        "649",
                        "184393",
                        "681",
                        "184513",
                        "713",
                        "184607",
                        "745",
                        "185039",
                        "777",
                        "185217",
                        "809",
                        "185299",
                        "1335",
                        "245745",
                        "99640",
                        "245831",
                        "4754",
                        "245917",
                        "4776",
                        "245993",
                        "4808",
                        "246105",
                        "24634",
                        "246215",
                        "24666",
                        "246297",
                        "24741",
                        "246395",
                        "24681",
                        "246503",
                        "24713",
                        "246579",
                        "49917",
                        "246649",
                        "49949",
                        "246721",
                        "261001",
                        "246807",
                        "48341",
                        "246883",
                        "48373",
                        "246955",
                        "48405",
                        "247023",
                        "48437",
                        "247089",
                        "48307",
                        "118255",
                        "48092",
                        "120617",
                        "48124",
                        "120734",
                        "48156",
                        "4125",
                        "48188",
                        "6587",
                        "48220",
                        "48252",
                        "48284",
                        "104619",
                        "105065",
                        "105097",
                        "105129",
                        "105161",
                        "105193",
                        "105225",
                        "105257",
                        "105289",
                        "105321",
                        "20118",
                        "34319",
                        "34362",
                        "34394",
                        "35364",
                        "35977",
                        "36009",
                        "36041",
                        "84635",
                        "84698",
                        "136541",
                        "136655",
                        "145317",
                        "157477",
                        "201381",
                        "201469",
                        "201533",
                        "201599",
                        "201663",
                        "201727",
                        "201793",
                        "201859",
                        "202129",
                        "210277",
                        "289883",
                        "162135",
                        "181909",
                        "181977",
                        "104000",
                        "182105",
                        "36",
                        "182307",
                        "245",
                        "182607",
                        "2733",
                        "182699",
                        "103917",
                        "182767",
                        "103949",
                        "182839",
                        "103981",
                        "182941",
                        "396",
                        "183097",
                        "458",
                        "183413",
                        "940",
                        "183549",
                        "3038",
                        "183705",
                        "3582",
                        "183783",
                        "99606",
                        "183871",
                        "104416",
                        "183957",
                        "560",
                        "184091",
                        "596",
                        "184167",
                        "628",
                        "184305",
                        "660",
                        "184445",
                        "692",
                        "184559",
                        "724",
                        "184633",
                        "756",
                        "185125",
                        "788",
                        "185257",
                        "820",
                        "209777",
                        "1348",
                        "245771",
                        "2063",
                        "245867",
                        "4765",
                        "245947",
                        "4787",
                        "246015",
                        "24613",
                        "246135",
                        "24645",
                        "246245",
                        "24720",
                        "246325",
                        "24752",
                        "246449",
                        "24692",
                        "246527",
                        "34794",
                        "246601",
                        "49928",
                        "246675",
                        "49960",
                        "246751",
                        "261023",
                        "246831",
                        "48352",
                        "246909",
                        "48384",
                        "246979",
                        "48416",
                        "247045",
                        "48448",
                        "247113",
                        "48467",
                        "118378",
                        "48103",
                        "120644",
                        "48135",
                        "120745",
                        "48167",
                        "287175",
                        "48199",
                        "48231",
                        "48263",
                        "48295",
                        "289825",
                        "105076",
                        "105108",
                        "105140",
                        "105172",
                        "105204",
                        "105236",
                        "105268",
                        "105300",
                        "105332",
                        "22478",
                        "34330",
                        "34341",
                        "34373",
                        "34405",
                        "35956",
                        "35988",
                        "36020",
                        "36052",
                        "84677",
                        "121503",
                        "136565",
                        "144745",
                        "145385",
                        "201317",
                        "201409",
                        "201491",
                        "201557",
                        "201621",
                        "201685",
                        "201749",
                        "201817",
                        "201881",
                        "202153",
                        "287159",
                        "289905",
                        "181859",
                        "181933",
                        "8",
                        "182003",
                        "104011",
                        "182165",
                        "144",
                        "182421",
                        "314",
                        "182631",
                        "3016",
                        "182721",
                        "103928",
                        "182791",
                        "103960",
                        "182869",
                        "103992",
                        "182977",
                        "412",
                        "183203",
                        "909",
                        "183449",
                        "957",
                        "183593",
                        "3420",
                        "183729",
                        "3599",
                        "183813",
                        "104395",
                        "183897",
                        "104427",
                        "183993",
                        "571",
                        "184117",
                        "607",
                        "184189",
                        "639",
                        "184351",
                        "671",
                        "184489",
                        "703",
                        "184585",
                        "735",
                        "184659",
                        "767",
                        "185195",
                        "799",
                        "185279",
                        "1192",
                        "245723",
                        "3230",
                        "245803",
                        "3843",
                        "245897",
                        "4747",
                        "245969",
                        "4798",
                        "246073",
                        "24624",
                        "246177",
                        "24656",
                        "246267",
                        "24731",
                        "246357",
                        "24763",
                        "246481",
                        "24703",
                        "246555",
                        "34805",
                        "246627",
                        "49939",
                        "246699",
                        "260981",
                        "246781",
                        "48331",
                        "246857",
                        "48363",
                        "246931",
                        "48395",
                        "247001",
                        "48427",
                        "247069",
                        "48459",
                        "118241",
                        "48328",
                        "118426",
                        "48114",
                        "120717",
                        "48146",
                        "4115",
                        "48178",
                        "4141",
                        "48210",
                        "48242",
                        "48274",
                        "3050",
                        "105055",
                        "105087",
                        "105119",
                        "105151",
                        "105183",
                        "105215",
                        "105247",
                        "105279",
                        "105311",
                        "20108",
                        "22500",
                        "34352",
                        "34384",
                        "34416",
                        "35967",
                        "35999",
                        "36031",
                        "81443",
                        "84688",
                        "136501",
                        "136587",
                        "145297",
                        "145407",
                        "201361",
                        "201449",
                        "201513",
                        "201579",
                        "201643",
                        "201707",
                        "201771",
                        "201839",
                        "201903",
                        "202177",
                        "289863",
                        "162085",
                        "181883",
                        "181957",
                        "98946",
                        "182045",
                        "104022",
                        "182225",
                        "166",
                        "182527",
                        "2636",
                        "182663",
                        "3086",
                        "182743",
                        "103939",
                        "182815",
                        "103971",
                        "182905",
                        "374",
                        "183057",
                        "440",
                        "183295",
                        "929",
                        "183527",
                        "3028",
                        "183675",
                        "3572",
                        "183753",
                        "99596",
                        "183845",
                        "104406",
                        "183923",
                        "219053",
                        "184067",
                        "586",
                        "184145",
                        "618",
                        "184221",
                        "650",
                        "184395",
                        "682",
                        "184519",
                        "714",
                        "184609",
                        "746",
                        "185041",
                        "778",
                        "185221",
                        "810",
                        "185301",
                        "1336",
                        "245747",
                        "99641",
                        "245833",
                        "4755",
                        "245919",
                        "4777",
                        "245995",
                        "4809",
                        "246107",
                        "24635",
                        "246217",
                        "24667",
                        "246299",
                        "24742",
                        "246397",
                        "24682",
                        "246505",
                        "24714",
                        "246581",
                        "49918",
                        "246651",
                        "49950",
                        "246727",
                        "261003",
                        "246809",
                        "48342",
                        "246885",
                        "48374",
                        "246957",
                        "48406",
                        "247025",
                        "48438",
                        "247091",
                        "48308",
                        "118368",
                        "48093",
                        "120618",
                        "48125",
                        "120735",
                        "48157",
                        "4126",
                        "48189",
                        "6588",
                        "48221",
                        "48253",
                        "48285",
                        "104620",
                        "105066",
                        "105098",
                        "105130",
                        "105162",
                        "105194",
                        "105226",
                        "105258",
                        "105290",
                        "105322",
                        "20119",
                        "34320",
                        "34363",
                        "34395",
                        "35365",
                        "35978",
                        "36010",
                        "36042",
                        "84636",
                        "84699",
                        "136543",
                        "136657",
                        "145337",
                        "157479",
                        "201383",
                        "201471",
                        "201535",
                        "201601",
                        "201665",
                        "201729",
                        "201795",
                        "201861",
                        "202131",
                        "210279",
                        "289885",
                        "162145",
                        "181911",
                        "181979",
                        "104001",
                        "182107",
                        "59",
                        "182359",
                        "247",
                        "182609",
                        "2734",
                        "182701",
                        "103918",
                        "182769",
                        "103950",
                        "182841",
                        "103982",
                        "182943",
                        "397",
                        "183101",
                        "459",
                        "183417",
                        "942",
                        "183553",
                        "3039",
                        "183707",
                        "3583",
                        "183785",
                        "99607",
                        "183873",
                        "104417",
                        "183959",
                        "561",
                        "184093",
                        "597",
                        "184169",
                        "629",
                        "184307",
                        "661",
                        "184447",
                        "693",
                        "184561",
                        "725",
                        "184635",
                        "757",
                        "185161",
                        "789",
                        "185259",
                        "821",
                        "209779",
                        "1349",
                        "245773",
                        "2064",
                        "245869",
                        "4766",
                        "245949",
                        "4788",
                        "246017",
                        "24614",
                        "246139",
                        "24646",
                        "246247",
                        "24721",
                        "246329",
                        "24753",
                        "246451",
                        "24693",
                        "246529",
                        "34795",
                        "246603",
                        "49929",
                        "246677",
                        "49961",
                        "246753",
                        "261025",
                        "246833",
                        "48353",
                        "246911",
                        "48385",
                        "246981",
                        "48417",
                        "247047",
                        "48449",
                        "247115",
                        "48318",
                        "118379",
                        "48104",
                        "120645",
                        "48136",
                        "171543",
                        "48168",
                        "4007",
                        "48200",
                        "48232",
                        "48264",
                        "48296",
                        "289827",
                        "105077",
                        "105109",
                        "105141",
                        "105173",
                        "105205",
                        "105237",
                        "105269",
                        "105301",
                        "105333",
                        "22480",
                        "34331",
                        "34342",
                        "34374",
                        "34406",
                        "35957",
                        "35989",
                        "36021",
                        "36053",
                        "84678",
                        "121504",
                        "136567",
                        "144749",
                        "145387",
                        "201319",
                        "201429",
                        "201493",
                        "201559",
                        "201623",
                        "201687",
                        "201751",
                        "201819",
                        "201883",
                        "202155",
                        "287161",
                        "162023",
                        "181861",
                        "181935",
                        "9",
                        "182005",
                        "104012",
                        "182167",
                        "145",
                        "182427",
                        "315",
                        "182633",
                        "3017",
                        "182723",
                        "103929",
                        "182793",
                        "103961",
                        "182871",
                        "352",
                        "182979",
                        "413",
                        "183209",
                        "912",
                        "183461",
                        "1005",
                        "183595",
                        "3421",
                        "183731",
                        "3600",
                        "183815",
                        "104396",
                        "183899",
                        "104428",
                        "183995",
                        "572",
                        "184119",
                        "608",
                        "184191",
                        "640",
                        "184353",
                        "672",
                        "184495",
                        "704",
                        "184587",
                        "736",
                        "184661",
                        "768",
                        "185197",
                        "800",
                        "185281",
                        "1193",
                        "245725",
                        "4641",
                        "245805",
                        "3844",
                        "245899",
                        "4748",
                        "245971",
                        "4799",
                        "246077",
                        "24625",
                        "246179",
                        "24657",
                        "246273",
                        "24732",
                        "246359",
                        "24764",
                        "246483",
                        "24704",
                        "246557",
                        "34806",
                        "246631",
                        "49940",
                        "246703",
                        "260983",
                        "246783",
                        "48332",
                        "246859",
                        "48364",
                        "246935",
                        "48396",
                        "247003",
                        "48428",
                        "247071",
                        "48460",
                        "118243",
                        "48083",
                        "118427",
                        "48115",
                        "120718",
                        "48147",
                        "4116",
                        "48179",
                        "4156",
                        "48211",
                        "48243",
                        "48275",
                        "104610",
                        "105056",
                        "105088",
                        "105120",
                        "105152",
                        "105184",
                        "105216",
                        "105248",
                        "105280",
                        "105312",
                        "20109",
                        "22502",
                        "34353",
                        "34385",
                        "34417",
                        "35968",
                        "36000",
                        "36032",
                        "81444",
                        "84689",
                        "136503",
                        "136589",
                        "145299",
                        "145409",
                        "201363",
                        "201451",
                        "201515",
                        "201581",
                        "201645",
                        "201709",
                        "201773",
                        "201841",
                        "201905",
                        "202179",
                        "289865",
                        "162089",
                        "181885",
                        "181959",
                        "98947",
                        "182057",
                        "104023",
                        "182227",
                        "177",
                        "182529",
                        "2660",
                        "182665",
                        "3087",
                        "182745",
                        "103940",
                        "182817",
                        "103972",
                        "182907",
                        "377",
                        "183059",
                        "441",
                        "183303",
                        "930",
                        "183529",
                        "3029",
                        "183677",
                        "3573",
                        "183755",
                        "99597",
                        "183847",
                        "104407",
                        "183925",
                        "219055",
                        "184069",
                        "587",
                        "184147",
                        "619",
                        "184225",
                        "651",
                        "184425",
                        "683",
                        "184523",
                        "715",
                        "184611",
                        "747",
                        "185045",
                        "779",
                        "185225",
                        "811",
                        "185303",
                        "1338",
                        "245749",
                        "99642",
                        "245835",
                        "4756",
                        "245921",
                        "4778",
                        "245997",
                        "4810",
                        "246111",
                        "24636",
                        "246221",
                        "24668",
                        "246301",
                        "24743",
                        "246403",
                        "24683",
                        "246507",
                        "24715",
                        "246583",
                        "49919",
                        "246653",
                        "49951",
                        "246729",
                        "261005",
                        "246811",
                        "48343",
                        "246887",
                        "48375",
                        "246959",
                        "48407",
                        "247027",
                        "48439",
                        "247093",
                        "48309",
                        "118369",
                        "48094",
                        "120628",
                        "48126",
                        "120736",
                        "48158",
                        "4127",
                        "48190",
                        "6589",
                        "48222",
                        "48254",
                        "48286",
                        "104621",
                        "105067",
                        "105099",
                        "105131",
                        "105163",
                        "105195",
                        "105227",
                        "105259",
                        "105291",
                        "105323",
                        "20120",
                        "34321",
                        "34364",
                        "34396",
                        "35366",
                        "35979",
                        "36011",
                        "36043",
                        "84637",
                        "121494",
                        "136545",
                        "136659",
                        "145339",
                        "157481",
                        "201385",
                        "201473",
                        "201537",
                        "201603",
                        "201667",
                        "201731",
                        "201797",
                        "201863",
                        "202133",
                        "210281",
                        "289887",
                        "162153",
                        "181913",
                        "181983",
                        "104002",
                        "182109",
                        "61",
                        "182361",
                        "250",
                        "182611",
                        "2735",
                        "182703",
                        "103919",
                        "182771",
                        "103951",
                        "182843",
                        "103983",
                        "182945",
                        "398",
                        "183113",
                        "460",
                        "183419",
                        "947",
                        "183561",
                        "3040",
                        "183709",
                        "3584",
                        "183787",
                        "99637",
                        "183877",
                        "104418",
                        "183961",
                        "562",
                        "184095",
                        "598",
                        "184171",
                        "630",
                        "184309",
                        "662",
                        "184451",
                        "694",
                        "184565",
                        "726",
                        "184637",
                        "758",
                        "185163",
                        "790",
                        "185261",
                        "822",
                        "245703",
                        "1350",
                        "245775",
                        "3042",
                        "245873",
                        "4767",
                        "245951",
                        "4789",
                        "246027",
                        "24615",
                        "246141",
                        "24647",
                        "246249",
                        "24722",
                        "246337",
                        "24754",
                        "246455",
                        "24694",
                        "246531",
                        "34796",
                        "246605",
                        "49930",
                        "246679",
                        "49962",
                        "246755",
                        "261027",
                        "246835",
                        "48354",
                        "246913",
                        "48386",
                        "246983",
                        "48418",
                        "247051",
                        "48450",
                        "247117",
                        "48319",
                        "118380",
                        "48105",
                        "120646",
                        "48137",
                        "287177",
                        "48169",
                        "4008",
                        "48201",
                        "48233",
                        "48265",
                        "48297",
                        "289829",
                        "105078",
                        "105110",
                        "105142",
                        "105174",
                        "105206",
                        "105238",
                        "105270",
                        "105302",
                        "105334",
                        "22482",
                        "34332"
                        )
    
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(1)
    val days = (0 until 30).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_triplets/segments"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"
    
    val tierUDF = udf((count: Int) =>
              if (count <= 50000) "1"
              else if(count >= 500000) "3"
                      else "2"
            )

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .filter("country = 'AR' or country = 'MX'")
      .select("feature","count")
      .withColumnRenamed("feature", "segment")
      .filter(col("segment").isin(segments: _*))
      .groupBy("segment").agg(sum(col("count")).as("count"))
      .withColumn("tier",tierUDF(col("count")))

    df.filter("tier = 1").show()
    df.filter("tier = 2").show()
    df.filter("tier = 3").show()
  }

  def main(args: Array[String]) {
     
    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder
        .appName("Random de Tincho")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .getOrCreate()
    
    report_user_uniques(spark)
    


  }

}
