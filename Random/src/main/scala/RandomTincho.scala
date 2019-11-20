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
 
 def keywords_embeddings(spark:SparkSession, keyword_path:String){
   val word_embeddings = spark.read
                              .format("csv")
                              .option("header","true")
                              .load("/datascience/custom/word_embeddings.csv")
                              
    def isAllDigits(x: String) = x forall Character.isDigit

    val myUDF = udf((keyword: String) => if (isAllDigits(keyword)) "DIGITO" else keyword)

    val dataset_kws = spark.read
                            .load(keyword_path)
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

    val df_preprocessed = processURL(df)

    df_preprocessed.drop("count","word")
      .groupBy("url")
      .mean()
      .write
      .format("csv")
      .option("header","true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_url_classifier/dataset_path_title_embedding_contextual")
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

  def get_dataset_contextual(spark:SparkSession){

    val title_kws = spark.read.format("csv")
                              .option("sep","\t")
                              .load("/datascience/custom/article_results_full.csv")
                              .withColumnRenamed("_c0","url")
                              .withColumnRenamed("_c2","title")
                              .filter("title is not null")
                              .select("url","title")
                              .withColumn("title", split(col("title"), " "))
                              .withColumn("keywords", explode(col("title")))
                              .filter(col("keywords").rlike("[a-z]{2,}"))
                              .select("url","keywords")
                              .withColumn("count",lit(1))

    val path_kws = spark.read.format("csv")
                              .option("sep","\t")
                              .load("/datascience/custom/article_results_full.csv")
                              .withColumnRenamed("_c0","url")
                              .select("url")
                              .withColumn("url", lower(col("url")))
                              .withColumn("url_path", regexp_replace(col("url"), """^[^/]*/""", ""))
                              .withColumn("url_keys", split(col("url_path"), "[^a-z0-9]"))
                              .withColumn("keywords", explode(col("url_keys")))
                              .filter(col("keywords").rlike("[a-z]{2,}"))
                              .select("url","keywords")
                              .withColumn("count", lit(1))

    

    title_kws.union(path_kws).write
                            .format("parquet")
                            .mode(SaveMode.Overwrite)
                            .save("/datascience/custom/kws_path_title_contextual")


  }

  def get_urls_for_ingester(spark){
    
    val df = spark.read.load("/datascience/data_demo/data_urls/day=20191110").groupBy("url").count.sort(desc("count")).limit(1000000)
    val df_processed = processURLHTTP(df).select("url")
    
    df_processed.write
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .save("/datascience/url_ingester/top_urls")

  }

  def main(args: Array[String]) {
     
    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder
        .appName("Random de Tincho")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .getOrCreate()
    
    //keywords_embeddings(spark,"/datascience/custom/kws_path_title_contextual")
    get_urls_for_ingester(spark)
  }

}
