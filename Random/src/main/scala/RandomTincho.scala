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

object RandomTincho {

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

    val urls = spark.read
                    .option("basePath", path)
                    .parquet(hdfs_files: _*)
                    .select("url")
    urls
  }

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
 
 def keywords_embeddings(spark:SparkSession){
   val word_embeddings = spark.read
                              .format("csv")
                              .option("header","true")
                              .load("/datascience/custom/word_embeddings.csv")
                              
    def isAllDigits(x: String) = x forall Character.isDigit

    val myUDF = udf((keyword: String) => if (isAllDigits(keyword)) "DIGITO" else keyword)

    val dataset_kws = spark.read
                            .load("/datascience/data_url_classifier/dataset_keyword_content_training/country=AR/")
                            .withColumn("content_keys",myUDF(col("content_keys")))
                            .withColumnRenamed("content_keys","word")
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

    df.drop("count","word")
      .groupBy("url")
      .mean()
      .write
      .format("csv")
      .option("header","true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_url_classifier/dataset_keyword_embedding")
 }

 def processURL(url: String): String = {
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

  val myUDF = udf((url: String) => processURL(url))

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

  def main(args: Array[String]) {
     
    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder
        .appName("Random de Tincho")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .getOrCreate()
    
    get_segments_pmi(spark, ndays = 30, since = 1)
  }

}
