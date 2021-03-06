package main.scala
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.joda.time.{Days, DateTime}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.log4j.{Level, Logger}

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object BigRandom {

def getUsersCustom(
      spark: SparkSession,
      nDays: Integer,
      since: Integer
  ) = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_triplets/segments"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val segList=List(154,155,158,177,178,103928,103937,103939,103966,103986)
    val dir = "/datascience/misc/"
    val fileNameFinal = dir + "danone_mx_juizzy"

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .filter("country = 'MX'")
      .withColumnRenamed("feature", "seg_id")
      .select("seg_id","device_id")
      .filter(col("seg_id").isin(segList:_*))
      .dropDuplicates()
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(fileNameFinal)
  }

 def checkNulls(
      spark: SparkSession,
      nDays: Integer,
      since: Integer
  ) = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_triplets/segments"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select("id_partner","day")
      .filter("id_partner is null").select("day").distinct()
      .write.format("csv")
      .option("header",true)
      .option("delimiter","\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/misc/checknulls.csv")

  }


def getDataEventQueue_27(
      spark: SparkSession,
      query_27: String,
      nDays: Integer,
      since: Integer) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val path = "/data/eventqueue"
        // Get the days to be loaded
    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/%s".format(day)) //
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read
        .option("sep", "\t")
        .option("header", "true")
        .format("csv")
        .load(hdfs_files: _*)
        .select("country", "device_id","platforms")
        .na
        .drop()
        .withColumn("platforms", split(col("platforms"), "\u0001"))
        .filter(query_27)
        .select("country", "device_id").distinct()
        .write.format("csv")
        .option("header",true)
        .option("delimiter","\t")
        .mode(SaveMode.Overwrite)
        .save("/datascience/misc/pv_platform27.csv")
  }


def getDataEventQueue(
      spark: SparkSession,
      query: String,
      nDays: Integer,
      since: Integer) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val path = "/data/eventqueue"
        // Get the days to be loaded
    val format = "yyyy/MM/dd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/%s".format(day)) //
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read
        .option("sep", "\t")
        .option("header", "true")
        .format("csv")
        .load(hdfs_files: _*)
        .filter(query)
        .select("country", "device_id").distinct()
        .na
        .drop()        
        .write.format("csv")
        .option("header","false")
        .option("delimiter","\t")
        .mode(SaveMode.Overwrite)
        .save("/datascience/misc/pv_mx_br.csv")
  }


def getSelectedKeywords(
      spark: SparkSession,
      nDays: Integer,
      since: Integer
  ) = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyy-MM-dd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/selected_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/%s.csv".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val df = spark.read
      .format("csv")
      .option("header", "True")
      .load(hdfs_files: _*)
      .select("url_raw","kw","domain")
    df  
  }

  def getDataKeywords(
      spark: SparkSession,
      country: String,
      nDays: Integer,
      since: Integer,
      stemming: Integer
  ): DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_keywords"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, country)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val to_select =
      if (stemming == 1) List("stemmed_keys", "device_id","domain")
      else List("content_keys", "device_id","domain")

    val columnName = to_select(0).toString

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select(to_select.head, to_select.tail: _*)
      .withColumnRenamed(columnName, "content_keywords")
      .na
      .drop()

    df
  }


  def getJointKeys(
      df_keys: DataFrame,
      df: DataFrame,
      verbose: Boolean
  ): DataFrame = {

    val df_joint = df
      .join(broadcast(df_keys), Seq("content_keywords"))
      .select("content_keywords", "device_id","domain")
      .dropDuplicates()

    /**
    if verbose {
      println(
        "count del join con duplicados: %s"
          .format(df_joint.select("device_id").distinct().count())
      )
    }
    */   
    val df_grouped = df_joint
      .groupBy("device_id")
      .agg(collect_list("content_keywords").as("kws"),collect_list("domain").as("domain"))
      .withColumn("device_type", lit("web"))
      .select("device_type", "device_id", "kws","domain")
    df_grouped
  }


///////////////////////7////////////////////////////////////

import scala.collection.mutable.ListBuffer

val domains = List("ac", "ad", "ae", "af", "ag", "ai", "al", "am", "an", "ao", "aq", "ar", "as", "asia", "at", "au", "aw", "ax", "az", "ba", "bb", "bd", "be", "bf", "bg", "bh", "bi", "biz", "bj", "bm", "bn", "bo", "br", "bs", "bt", "bv", "bw", "by", "bz", "ca", "cat", "cc", "cd", "cf", "cg", "ch", "ci", "ck", "cl", "cm", "cn", "co", "com", "coop", "cr", "cu", "cv", "cx", "cy", "cz", "de", "dj", "dk", "dm", "do", "dz", "ec", "edu", "ee", "eg", "er", "es", "et", "eu", "fi", "fj", "fk", "fm", "fo", "fr", "ga", "gb", "gd", "ge", "gf", "gg", "gh", "gi", "gl", "gm", "gn", "gob", "gov", "gp", "gq", "gr", "gs", "gt", "gu", "gw", "gy", "hk", "hm", "hn", "hr", "ht", "hu", "id", "ie", "il", "im", "in", "info", "int", "io", "iq", "ir", "is", "it", "je", "jm", "jo", "jobs", "jp", "ke", "kg", "kh", "ki", "km", "kn", "kp", "kr", "kw", "ky", "kz", "la", "lb", "lc", "li", "lk", "lr", "ls", "lt", "lu", "lv", "ly", "ma", "mc", "md", "me", "mg", "mh", "mil", "mk", "ml", "mm", "mn", "mo", "mobi", "mp", "mq", "mr", "ms", "mt", "mu", "mv", "mw", "mx", "my", "mz", "na", "nc", "ne", "net", "nf", "ng", "ni", "nl", "no", "np", "nr", "nu", "nz", "om", "org", "pa", "pe", "pf", "pg", "ph", "pk", "pl", "pm", "pn", "pr", "pro", "ps", "pt", "pw", "py", "qa", "re", "ro", "rs", "ru", "rw", "sa", "sb", "sc", "sd", "se", "sg", "sh", "si", "sj", "sk", "sl", "sm", "sn", "so", "sr", "st", "su", "sv", "sy", "sz", "tc", "td", "tel", "tf", "tg", "th", "tj", "tk", "tl", "tm", "tn", "to", "tp", "tr", "tt", "tv", "tw", "tz", "ua", "ug", "uk", "us", "uy", "uz", "va", "vc", "ve", "vg", "vi", "vn", "vu", "wf", "ws", "xxx", "ye", "yt", "za", "zm", "zw")

def parseURL(url: String): String = {

  // First we obtain the query string and the URL divided in 
  val split = url.split("\\?")

  val fields = split(0).split('/')

  // Now we can get the URL path and the section with no path at all
  val path = (if (url.startsWith("http")) fields.slice(3, fields.length)
  else fields.slice(1, fields.length))
  val non_path = (if (url.startsWith("http")) fields(2) else fields(0)).split("\\:")(0)

  // From the non-path, we can get the extension, the domain, and the subdomain
  val parts = non_path.split("\\.").toList
  var extension: ListBuffer[String] = new ListBuffer[String]()
  var count = parts.length
  if (count > 0) {
    var part = parts(count - 1)
    // First we get the extension
    while (domains.contains(part) && count > 1) {
      extension += part
      count = count - 1
      part = parts(count - 1)
    }

  // Now we obtain the domain and subdomain.
  val domain = if (count > 0) parts(count - 1) else ""

  domain

  } else ("")
  }

val udfGetDomain = udf(
    (url: String) =>
    parseURL(url)
  )

 def getDataURLS(
      spark: SparkSession,
      country: String,
      nDays: Integer,
      since: Integer
  ): DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/url_ingester/data"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, country)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .withColumn("domain",udfGetDomain(col("url")))
      .na
      .drop()

    df
  }

def getDataPipeline(
      spark: SparkSession,
      path: String,
      nDays: String,
      since: String,
      country: String) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
      
    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day,country)) //
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    df
  }

 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark = SparkSession.builder
    .appName("BigRandom")
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .getOrCreate()

    val path = "/datascience/keywiser/processed/*fortnight*"
    spark.read
        .format("csv")
        .option("sep", "\t")
        .option("header", "false")
        .load(path)
        .toDF("device_type","device_id","segments")
        .withColumn("segments", split(col("segments"), ","))
        .withColumn("segment", explode(col("segments")))
        .groupBy("segment")
        .agg(approxCountDistinct("device_id", 0.03) as "device_unique")          
        .write
        .format("csv")
        .option("sep", "\t")
        .mode("overwrite")
        .save("/datascience/misc/volumes_keywiser/202004")



    /**
    // PARA LEER Y CONTAR DATA DE STARTAPP PREVIA A LA INGESTA
    val path = "/datascience/misc/startapp/data/"
    spark.read
        .format("csv")
        .option("sep", " ")
        .load(path)
        .select("_c0")
        .withColumn("_c0", regexp_replace(col("_c0"), "[ \t]+", "\u0020"))
        .withColumn("_c0", split(col("_c0"), "\u0020"))
        .withColumn("device_type", col("_c0").getItem(0))
        .withColumn("device_id", col("_c0").getItem(1))
        .withColumn("segments", col("_c0").getItem(2))
        .withColumn("country", col("_c0").getItem(3))
        .drop("_c0")
        .filter("device_type!='device_type'")
        .withColumn("segments", split(col("segments"), ","))
        .withColumn("segment", explode(col("segments")))
        .groupBy("segment","country")
        .agg(approxCountDistinct("device_id", 0.03) as "device_unique")
        .select("segment","country","device_unique")
        .write
        .format("csv")
        .option("sep", "\t")
        .mode("overwrite")
        .save("/datascience/misc/startapp/volumes_full")

    */

   /** 

  spark.read.format("csv")
      .option("delimiter","\t")
      .load("/datascience/misc/kws_NSE_MX_scrapper")
      .toDF("url","segment","kw","TFIDF")
      .withColumn("TFIDF", col("TFIDF").cast("double"))
      .orderBy(desc("TFIDF"))
      .limit(5000)
      .write
      .format("csv")
      .option("sep", "\t")
      .mode("overwrite")
      .save("/datascience/misc/kws_NSE_MX_scrapper_2")

    */ 

    /**

    val pii_table =  spark.read
                          .load("/datascience/pii_matching/pii_tuples/")
                          .filter("country = 'MX'")
                          .select("device_id","ml_sh2","nid_sh2", "mb_sh2")
                          .withColumnRenamed("ml_sh2", "email")
                          .withColumnRenamed("nid_sh2", "dni")
                          .withColumnRenamed("mb_sh2", "phone")
                          .withColumn("email", lower(col("email")))
                          .withColumn("dni", lower(col("dni")))
                          .withColumn("phone", lower(col("phone")))                  

    val nids = spark.read
    .format("csv")
    .option("sep", "\t")
    .option("header", "true")
    .load("/datascience/custom/Mx0902.tsv.gz")
    .select("device_id")
    .withColumnRenamed("device_id", "dni")
    .withColumn("dni", lower(col("dni")))
    .dropDuplicates()

    val phones = spark.read
    .format("csv")
    .option("sep", "\t")
    .option("header", "true")
    .load("/datascience/custom/Mx1402.tsv")
    .select("device_id")
    .withColumnRenamed("device_id", "phone")
    .withColumn("phone", lower(col("phone")))   
    .dropDuplicates()

    val phones_match = pii_table
    .select("phone","device_id")
    .distinct
    .join(phones,Seq("phone"))

    println("Phones match:")
    println(phones_match.count())

    val nids_match = pii_table
    .select("dni","device_id")
    .distinct
    .join(nids,Seq("dni"))

    println("Nids match:")
    println(nids_match.count())  

    println("Total devices:")
    println(phones_match.select("device_id").union(nids_match.select("device_id")).distinct().count())

  */

  /**

    val typeMap = Map(
          "web" -> "web",
          "coo" -> "web",
          "and" -> "android",
          "maid" -> "android",
          "ios" -> "ios",
          "con" -> "TV",
          "dra" -> "drawbridge",
          "idfa" -> "ios",
          "aaid"->"android",
          "android"->"android",
          "unknown"->"unknown")
    val mapUDF = udf((dev_type: String) => typeMap(dev_type))

    val path = "/datascience/geo/raw_output/BASE_DE_DATOS_EDIFICIOS_TELECENTRO_Pois_180d_argentina_8-4-2020-16h"
    spark.conf.set("spark.sql.session.timeZone",  "GMT-3")
    val db = spark.read.format("csv")
    .option("delimiter","\t")
    .option("header",true)
    .load(path)
    .withColumn("device_type",mapUDF(col("device_type")))
    .filter("device_type IN ('android', 'ios')")

    val distances = List(10,20)
    val audiences = "bajo,medio,alto".split(",").toList

    for (distance <- distances) {    
    var df = db.filter("distance <= %d".format(distance))
    .withColumn("Time", to_timestamp(from_unixtime(col("timestamp"))))
    .withColumn("Hour", date_format(col("Time"), "HH"))
    .filter(
      !date_format(col("Time"), "EEEE").isin(List("Saturday", "Sunday"): _*)
    )
    .withColumn(
      "Period",
      when((col("Hour") >= 24 || col("Hour") <= 6), "Hogar")
        .otherwise("Trabajo")
    ).filter("Period = 'Hogar'")

    for (aud <- audiences){

    df.filter("audience == '%s'".format(aud))
      .select("device_id")
      .write
      .format("csv")
      .option("sep", "\t")
      .mode("overwrite")
      .save("/datascience/custom/telecentro_buildings_%d_%s".format(distance,aud))

      }

    }

  */

    /**

    val countries = "AR,MX,CL,CO,PE,UY".split(",").toList
    val audiences = List(306279,306281,306283)

    for (country <- countries) {    
      println(country)
      var df = spark.read.format("csv")
          .option("sep", "\t")
          .load("/datascience/custom/cuadras_per_user_%s_to_push".format(country) )  
          .toDF("device_type","device_id","segment")
          .filter("device_type IN ('android', 'ios')")
      for (aud <- audiences) {
        df.filter("segment == %d".format(aud))
        .select("device_id")
        .write
        .format("csv")
        .option("sep", "\t")
        .mode("append")
        .save("/datascience/custom/cuadras_per_user_madids_%d".format(aud))
          
      }   
          
    }

    */    

/**
    val segments = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/misc/axciom_segs.csv")
      .select("segmentId","cluster")
      .withColumnRenamed("segmentId", "segment")
      .withColumn("segment", col("segment").cast(IntegerType))

    val df = spark.read.format("csv")
    .option("sep", "\t")
    .load("/datascience/ipg/month=202003/IPG_maids_enriched_gz")
    .toDF("rely_id","segments")
    .withColumn("segments", split(col("segments"), ","))
    .withColumn("segment", explode(col("segments")))
    .withColumn("segment", col("segment").cast(IntegerType))

    df.join(segments,Seq("segment"),"outer").na.fill("Custom")
    .groupBy("cluster")
    .agg(approx_count_distinct(col("rely_id"), 0.02).as("approx_count"))
    .write
    .format("csv")
    .option("sep", "\t")
    .mode("overwrite")
    .save("/datascience/misc/axciom_count")


  */

    /**

    val files = "BR_302875_2020-04-02T20-38-12-262987,CL_302875_2020-04-02T20-38-16-018111,CO_302875_2020-04-02T20-38-08-364538".split(",").toList
    for (file <- files) {    

    var path =  "/datascience/devicer/processed/%s".format(file) 
    println(path)

    var df = spark.read.format("csv")
    .option("sep", "\t")
    .load(path)
    .toDF("device_type","device_id","segment")

    println(df.count())        

    var df_grouped = df
            .groupBy("device_id")
            .agg(countDistinct(col("segment")).as("segment_count"))
            .filter("segment_count>1")        

    println(df_grouped.count())

    }

    */

    /**

    val countries = "AR,MX".split(",").toList
    for (country <- countries) {    
    println(country)
    var df = spark.read.format("csv")
        .option("sep", "\t")
        .load("/datascience/custom/cuadras_per_user_%s_to_push".format(country) )  
        .toDF("device_type","device_id","segment")
        .groupBy("segment").agg(approx_count_distinct(col("device_id"), 0.02).as("total_devices"))  
          

    println(df.show())

    }

    */

  /**

    val getSeg = udf((lista: Seq[Integer]) =>
        if (lista.contains(306283)) 306283
        else if(lista.contains(306281)) 306281
        else if(lista.contains(306279)) 306279
        else lista.last.toInt)

    //val countries = "AR,MX".split(",").toList
    val countries = "CL,CO,PE,UY".split(",").toList

    for (country <- countries) {    
    println(country)
    var final_path = "/datascience/custom/cuadras_per_user_%s_to_push".format(country) 
    var path1 = "/datascience/custom/cuadras_per_user_%s_csv".format(country)
    var df = spark.read.format("csv")
        .option("sep", "\t")
        .load(path1)  
        .toDF("device_type","device_id","segment")
        .groupBy("device_type","device_id")
        .agg(collect_list("segment").as("segment"))
        .withColumn("segment",getSeg(col("segment"))) 
        .select("device_type", "device_id", "segment")

    println(df.show())

    df.write
    .format("csv")
    .option("sep", "\t")
    .mode("overwrite")
    .save(final_path)
    
    var path2 = "/datascience/audiences/crossdeviced/cuadras_per_user_%s_csv_xd".format(country)
    var df_xd = spark.read.format("csv")
        .load(path2)
        .withColumnRenamed("_c1", "device_id")
        .withColumnRenamed("_c2", "device_type")
        .withColumnRenamed("_c5", "segment")
        .withColumn("segment", col("segment").cast(IntegerType))
        .withColumn("device_type", when(col("device_type")==="and", "android").otherwise(when(col("device_type")==="ios", "ios").otherwise("web")))
        .groupBy("device_type","device_id")
        .agg(collect_list("segment").as("segment"))       
        .withColumn("segment",getSeg(col("segment")))    
        .select("device_type", "device_id", "segment")

    println(df_xd.show())
    df_xd.write
    .format("csv")
    .option("sep", "\t")
    .mode("append")
    .save(final_path)

    var db = spark.read.format("csv")
        .option("sep", "\t")
        .load(final_path)  
        .toDF("device_type","device_id","segment")
        .groupBy("segment").agg(approx_count_distinct(col("device_id"), 0.02).as("total_devices"))    

    println(db.show())    

    }   

*/

/**
    val countries = "AR,MX".split(",").toList
    for (country <- countries) {    

    println(country)  
    
    var df = spark.read.format("csv")
    .option("sep", "\t")
    .load("/datascience/custom/cuadras_per_user_%s_csv".format(country))  
    .toDF("device_type","device_id","segment")
    .groupBy("segment").agg(approx_count_distinct(col("device_id"), 0.02).as("devices_original"))    

    println("TO PUSH COUNT")
    println(df.orderBy(asc("devices_original")).show())

    }

*/

/**
  val df = spark.read.format("parquet")
  .load("/datascience/custom/cl_geo_movement")
  .filter("geohashes<=3 AND occurrences>5")
  .withColumnRenamed("ad_id", "device_id")
  .withColumn("device_type", lit("android"))
  .withColumn("segment", lit(302479))
  .select("device_type", "device_id", "segment")

  println(df.show()) 

  df.write.format("csv")
  .option("sep", "\t")
  .mode("overwrite")
  .save("/datascience/custom/cl_geo_movement_to_push")

  val df_xd = spark.read.format("csv")
      .load("/datascience/audiences/crossdeviced/cl_geo_movement_csv_xd")
      .withColumnRenamed("_c1", "device_id")
      .withColumnRenamed("_c2", "device_type")
      .withColumnRenamed("_c4", "category")
      .withColumn("segment", lit(302479))
      .withColumn("device_type", when(col("device_type")==="and", "android").otherwise(when(col("device_type")==="ios", "ios").otherwise("web")))
      .select("device_type", "device_id", "segment")
      .distinct()

  println(df_xd.show())
  df_xd.write
  .format("csv")
  .option("sep", "\t")
  .mode("append")
  .save("/datascience/misc/cl_geo_movement_to_push")
*/


/**
    spark.read.format("parquet")
    .load("/datascience/custom/cl_geo_movement")
    .filter("geohashes<=3 AND occurrences>5")
    .withColumn("type", lit("android"))
    .write.format("csv")
    .option("sep", "\t")
    .mode("overwrite")
    .save("/datascience/custom/cl_geo_movement_csv")

  **/  

/**
    val getSeg = udf((lista: Seq[Integer]) =>
    if (lista.contains(303353)) 303353
    else if(lista.contains(303361)) 303361
    else if(lista.contains(303357)) 303357
    else lista.last.toInt)
    
    val countries = "AR,BR,CL,CO,MX,PE".split(",").toList

    for (country <- countries) {    

    var df = spark.read.format("csv")
    .option("sep", "\t")
    .load("/datascience/misc/covid_%s_to_push_new_pure".format(country))  
    .toDF("device_type","device_id","segment")
    .groupBy("segment").agg(approx_count_distinct(col("device_id"), 0.02).as("devices_original"))  
    
    println(country)
    println(df.orderBy(asc("segment")).show())

    }   

*/
    /**
    for (country <- countries) {    

    //path (AR segment ids were modified)
    var path = if (country == "AR")
      "/datascience/misc/covid_%s_to_push_pure".format(country.toLowerCase())
     else
      "/datascience/misc/covid_%s_to_push".format(country)

    spark.read.format("csv")
    .option("sep", "\t")
    .load(path)  
    .toDF("device_type","device_id","segment")
    .groupBy("device_type","device_id")
    .agg(collect_list("segment").as("segment"))
    .withColumn("segment",getSeg(col("segment"))) 
    .select("device_type", "device_id", "segment")
    .write
    .format("csv")
    .option("sep", "\t")
    .mode("overwrite")
    .save("/datascience/misc/covid_%s_to_push_new_pure".format(country))

    }

    */

    

    /**
    val countries = "ar,BR,CL,CO,MX,PE".split(",").toList
    for (country <- countries) {    

    println(country)  
    
    var df = spark.read.format("csv")
    .option("sep", "\t")
    .load("/datascience/misc/covid_%s_to_push".format(country))  
    .toDF("device_type","device_id","segment")
    .groupBy("segment").agg(approx_count_distinct(col("device_id"), 0.02).as("devices_original"))    

    println("TO PUSH COUNT")
    println(df.show())
  
    var db = spark.read.format("csv")
    .option("sep", "\t")
    .load("/datascience/misc/covid_%s_to_remove".format(country))
    .toDF("device_type","device_id","segment")    
    .groupBy("segment").agg(approx_count_distinct(col("device_id"), 0.02).as("devices_to_remove"))

    println("COUNT WHEN REMOVED")
    println(df.join(db,Seq("segment")).withColumn("Result", col("devices_original")-col("devices_to_remove")).show())

    var dc = spark.read.format("csv")
    .option("sep", "\t")
    .load("/datascience/data_lookalike/expansion/ondemand/jobId=coronavius%s".format(country.toUpperCase()))
    .toDF("device_type","device_id","segment")
    .groupBy("segment").agg(approx_count_distinct(col("device_id"), 0.02).as("LALS"))    

    println("LAL COUNT")
    println(dc.show())
       
    }

    */

    //'/datascience/data_lookalike/expansion/ondemand/jobId=coronavius{}


    /**
    val countries = "ar,BR,CL,CO,MX,PE".split(",").toList
    for (country <- countries) {    
    println(country)  
    println("volumen")
    println(spark.read.format("csv").option("sep", "\t").load("/datascience/misc/covid_%s_to_remove".format(country)).count())
    }

    **/    

    /**

    val countries = "ar,BR,CL,CO,MX,PE".split(",").toList
    for (country <- countries) {    
    var df = spark.read.format("csv")
    .option("sep", "\t")
    .load("/datascience/misc/covid_%s_to_push".format(country))  
    .toDF("device_type","device_id","segment")

    var db = df.withColumn("count", lit(1))
    .groupBy("device_id").agg(sum(col("count")) as "total")
    .filter("total>1")
    
    df.join(db,Seq("device_id"))
    .select("device_type", "device_id", "segment")
    .write
    .format("csv")  
    .option("sep", "\t")
    .save("/datascience/misc/covid_%s_to_remove".format(country))    

    }

*/

 /**
    val path_triplets = "/datascience/data_triplets/segments/"  
    var triplets = getDataPipeline(spark,path_triplets,"25","2","%s".format("AR"))
              .select("device_id","feature")

    var df = spark.read.format("csv")
    .option("sep", "\t")
    .load("/datascience/misc/covid_%s_to_push".format("ar"))
    .toDF("device_type","device_id","segment")

    var aff = df.join(triplets,Seq("device_id"))
    .groupBy("segment","feature").agg(approx_count_distinct(col("device_id"), 0.02).as("score"))

    aff.write
     .format("csv")
     .mode("overwrite")
     .save("/datascience/misc/covid_%s_aff".format("AR"))    

    */


    /**
    val countries = "ar,BR,CL,CO,MX,PE".split(",").toList
    for (country <- countries) {    
    println(country)  
    println(spark.read.format("csv").option("sep", "\t").load("/datascience/custom/coronavirus_%s_lal".format(country)).count())
    }
    

    val countries = "BR,CL,CO,MX,PE".split(",").toList
    val path_triplets = "/datascience/data_triplets/segments/"

    for (country <- countries) {    
    var triplets = getDataPipeline(spark,path_triplets,"25","2","%s".format(country))
              .select("device_id","feature")

    var df = spark.read.format("csv")
    .option("sep", "\t")
    .load("/datascience/misc/covid_%s_to_push".format(country))
    .toDF("device_type","device_id","segment")

    var aff = df.join(triplets,Seq("device_id"))
    .groupBy("segment","feature").agg(approx_count_distinct(col("device_id"), 0.02).as("score"))

    aff.write
     .format("csv")
     .mode("overwrite")
     .save("/datascience/misc/covid_%s_aff".format(country))

    }

  */
    /**

    val df = spark.read.format("csv")
    .option("sep", "\t")
    .load("/datascience/misc/covid_MX_to_push")
    .toDF("device_type","device_id","segment")
    .groupBy("segment")
    .agg(approx_count_distinct(col("device_id"), 0.02).as("devices"))
    
    println(df.show())

    **/
/**
    val countries = "BR,CL,CO,MX,PE".split(",").toList

    for (country <- countries) {
        
    spark.read.format("csv")
    .option("header",false)
    .load("/datascience/misc/covid_last_%s".format(country))
    .toDF("device_id","category")
     .withColumn("segment", when(col("category")===3, 303353).otherwise(when(col("category")===2, 303357).otherwise(when(col("category")===1, 303359).otherwise(303361))))
     .withColumn("device_type", lit("web"))
     .select("device_type", "device_id", "segment")
     .write
     .format("csv")
     .option("sep", "\t")
     .mode("overwrite")
     .save("/datascience/misc/covid_%s_to_push".format(country))

 
    spark.read.format("csv")
     .load("/datascience/audiences/crossdeviced/covid_last_%s_xd".format(country))
     .withColumnRenamed("_c1", "device_id")
     .withColumnRenamed("_c2", "device_type")
     .withColumnRenamed("_c4", "category")
     .withColumn("segment", when(col("category")===3, 303353).otherwise(when(col("category")===2, 303357).otherwise(when(col("category")===1, 303359).otherwise(303361))))
     .withColumn("device_type", when(col("device_type")==="and", "android").otherwise(when(col("device_type")==="ios", "ios").otherwise("web")))
     .select("device_type", "device_id", "segment")
     //.dropDuplicates("device_id")
     .distinct()
     .write
     .format("csv")
     .option("sep", "\t")
     .mode("append")
     .save("/datascience/misc/covid_%s_to_push".format(country))


}

*/    

    /**
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val df =spark.read.format("csv")
    .option("header",true)
    .load("/datascience/misc/covid_end_march")

    df.select("device_id","day","url_count_total","ratio")
    .dropDuplicates("device_id","day")
    .write
    .format("parquet")
    .partitionBy("day")
    .mode("append")
    .save("/datascience/misc/covid_unduplicated_march")
*/

/***
    def getData(
      spark: SparkSession,
      nDays: Integer,
      since: Integer,
      path: String
  ): DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s".format(day)) //for each day from the list it returns the day path.
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)

    df
  }  

  val path_urls = "/datascience/data_demo/data_urls"
  val data_urls = getData(spark,15,1,path_urls)
  .select("device_id","url","id_partner","event_type","segments","country")
  .filter("id_partner==1395")
  .filter("country=='CL'")
  .withColumn("segment", explode(col("segments")))
  .groupBy("segment")
  .agg(approx_count_distinct(col("device_id"), 0.02).as("device_unique"))
      .write.format("csv").option("header","true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/misc/tl13")  


    **/

    /**
    val format = "yyyyMMdd"
    val end = new DateTime(2020,2,10,0,0,0,0) 

    val nDays = 15
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/url_ingester/data"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/".format(day))
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    
    import spark.implicits._
    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select("url","count")
      .filter($"url".contains("zonajobs"))
      .dropDuplicates("url")

    df.write.format("csv").option("header","true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/misc/testzj")  

    **/


    /*
    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = new DateTime(2020,2,10,0,0,0,0) 

    val nDays = 15
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/url_ingester/data"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/".format(day))
      .filter(file_path => fs.exists(new org.apache.hadoop.fs.Path(file_path))) //analogue to "os.exists"

    
    import spark.implicits._
    val df = spark.read
      .option("basePath", path)
      .parquet(hdfs_files: _*)
      .select("url")
      .filter($"url".contains("zonajobs"))
      .dropDuplicates("url")

    println(df.count())  
    */
      /**

      val df_old = getSelectedKeywords(spark, 15 , 42 )
      .groupBy("domain")
      .agg(approx_count_distinct(col("url_raw"), 0.03).as("count_old"))

      val df_new = getSelectedKeywords(spark,  15 , 22 )
      .groupBy("domain")
      .agg(approx_count_distinct(col("url_raw"), 0.03).as("count_new"))
    
      df_old.join(df_new,Seq("domain"),"outer").na.fill(0)
      .write.format("csv").option("header","true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/domains_count_selectedkws")  

      */


      /**
      val df_old_dump =  spark.read
          .format("csv")
          .option("header", "True")
          .option("sep", "\t")
          .load("/datascience/scraper/temp_dump2/.csv")
          .select("url")
          .filter("url!='url'")
          .selectExpr("*", "parse_url(url, 'HOST') as domain")
          .groupBy("domain")
          .agg(approx_count_distinct(col("url"), 0.03).as("count_old_dump"))

      val df_old_sk = getSelectedKeywords(spark, 15 , 23 )
      .select("url_raw")
      .selectExpr("*", "parse_url(url_raw, 'HOST') as domain")
      .groupBy("domain")
      .agg(approx_count_distinct(col("url_raw"), 0.03).as("count_old_sk"))
    
      df_old_dump.join(df_old_sk,Seq("domain"),"outer").na.fill(0)
      .write.format("csv").option("header","true")
      .mode(SaveMode.Overwrite)
      .save("/datascience/misc/domains_count_comparison")  
      */


      /**
      val dir = "/datascience/reports/custom/client_688/"
      val dir2 = "/datascience/reports/custom/client_688_2/"

      spark.read
        .format("parquet")
        .load(dir)
        .repartition(1)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .save(dir2)

      */  

          /**
          val intervals = "2020-01-27,2020-01-28,2020-01-29,2020-01-30,2020-01-31,2020-02-01,2020-02-02,2020-02-03,2020-02-04,2020-02-05,2020-02-06,2020-02-07,2020-02-08,2020-02-09,2020-02-10".split(",").toList

          var path = "/datascience"
          var destpath = "/datascience"

          for (i <- intervals) {
              path = "/datascience/scraper/temp_dump/%s_daily.csv*".format(i)
              destpath = "/datascience/scraper/temp_dump2/%s_daily.csv".format(i)

              var df = spark.read.option("sep", "\t")
                      .option("header", "true")
                      .format("csv")
                      .load(path)
              df.write
                .format("csv")
                .option("header", "true")
                .option("sep", "\t")
                .mode(SaveMode.Overwrite)
                .save(destpath)
      }  

      **/

        /**
          //val df_old = getDataURLS(spark, "AR", 15 , 36 )
          val df_old = getSelectedKeywords(spark, 15 , 36 )
          .filter("domain=='zonajobs'")

          //println(df_old.groupBy("domain").agg(sum(col("count")) as "total_hits").show())

          println(df_old.drop("count","kw").dropDuplicates().count())


          //val df_new = getDataURLS(spark, "AR", 15 , 16 )
          val df_new = getSelectedKeywords(spark,  15 , 16 )
          .filter("domain=='zonajobs'")

          //println(df_new.groupBy("domain").agg(sum(col("count")) as "total_hits").show())

          println(df_new.drop("count","kw").dropDuplicates().count())

      */


      /**

      def getString =
          udf((array: Seq[String]) => array.map(_.toString).mkString(","))

      val df_keys = spark.read
      .format("csv")
      .option("header", "true")
      .load("/datascience/misc/all_kws_tojoin.csv")

      val df_old = getDataKeywords(spark,"AR",15,31,0)

      val data_old = getJointKeys(df_keys, df_old, false)
      .withColumn("kws",getString(col("kws")))
      .withColumn("domain",getString(col("domain")))   
      
      data_old.write
            .format("csv")
            .option("header",true)
            .mode(SaveMode.Overwrite)
            .save("/datascience/misc/df_old_all")

      val df_new = getDataKeywords(spark,"AR",15,11,0)

      val data_new = getJointKeys(df_keys, df_new, false)
      .withColumn("kws",getString(col("kws")))
      .withColumn("domain",getString(col("domain")))   
      
      data_new.write
            .format("csv")
            .option("header",true)
            .mode(SaveMode.Overwrite)
            .save("/datascience/misc/df_new_all")

      */

      /**

      //103984
      val keywords = "base|dato,c++,data|base,develop,golang,java,json,linux,php,programacion|lenguaje,sdk,simple|text,sql"
          
      val domain_filter =  "domain IN ('bumeran', 'konzerta', 'laborum', 'multitrabajos', 'zonajobs')"

      /** Format all keywords from queries to join */
      import spark.implicits._
      val trimmedList: List[String] = keywords.split(",").map(_.trim).toList
      val df_keys = trimmedList.toDF().withColumnRenamed("value", "content_keywords")

      val df_old = getDataKeywords(spark,"AR",15,31,0)
      //.filter(domain_filter)

      val data_old = getJointKeys(df_keys, df_old, false)
      .withColumn("kws",getString(col("kws")))
      .withColumn("domain",getString(col("domain")))   
      
      data_old.write
            .format("csv")
            .option("header",true)
            .mode(SaveMode.Overwrite)
            .save("/datascience/misc/df_old_103984_NOF")

      val df_new = getDataKeywords(spark,"AR",15,11,0)
      //.filter(domain_filter)

      val data_new = getJointKeys(df_keys, df_new, false)
      .withColumn("kws",getString(col("kws")))
      .withColumn("domain",getString(col("domain")))   
      
      data_new.write
            .format("csv")
            .option("header",true)
            .mode(SaveMode.Overwrite)
            .save("/datascience/misc/df_new_103984_NOF")

      */

      /**

      //val domain_filter =  "domain IN ('autocosmos', 'autoscerokm', 'demotores', 'olx')"

      val domain_filter = "domain IN ('bumeran', 'konzerta', 'laborum', 'multitrabajos', 'perfil', 'taringa', 'upsocl', 'zonajobs')"

      //val query ="((array_contains(kw, 'hybrid') OR array_contains(kw, 'rimac') OR array_contains(kw, 'tesla')) or ((array_contains(kw, 'bmw') and array_contains(kw, 'i3')) or ((array_contains(kw, 'nissan') and array_contains(kw, 'leaf')) or ((array_contains(kw, 'renault') and array_contains(kw, 'twizy')) or ((array_contains(kw, 'tesla') and array_contains(kw, 'model3')) or ((array_contains(kw, 'tesla') and array_contains(kw, 'models')) or ((array_contains(kw, 'tesla') and array_contains(kw, 'modelx')) or ((array_contains(kw, 'tesla') and array_contains(kw, 'p900')) or ((array_contains(kw, 'tesla') and array_contains(kw, 'spider')) or ((array_contains(kw, 'toyota') and array_contains(kw, 'prius')) or ((array_contains(kw, 'auto') and array_contains(kw, 'electrico')) or (array_contains(kw, 'vehiculo') and array_contains(kw, 'electrico')))))))))))))"

      val query = "(array_contains(kw, 'biogeografia') OR array_contains(kw, 'biologa') OR array_contains(kw, 'biologia') OR array_contains(kw, 'biologica') OR array_contains(kw, 'biologicas') OR array_contains(kw, 'biologico') OR array_contains(kw, 'biologicos') OR array_contains(kw, 'biologo') OR array_contains(kw, 'bioquimica') OR array_contains(kw, 'embriologia') OR array_contains(kw, 'etologia') OR array_contains(kw, 'fisiologia') OR array_contains(kw, 'microbiologia') OR array_contains(kw, 'neurociencia') OR array_contains(kw, 'primatologia') OR array_contains(kw, 'protozoologia') OR array_contains(kw, 'virologia'))"

      //val df_old = getSelectedKeywords(spark,15,29)
      val df_old = getSelectedKeywords(spark,15,31)    
      .filter(domain_filter)
      .withColumn("kw", split(col("kw"), " "))
      .filter(query)
      .withColumn("kw",getString(col("kw")))
      
      df_old.write
            .format("csv")
            .option("header",true)
            .mode(SaveMode.Overwrite)
            .save("/datascience/misc/df_old_query_103921")

      //val df_new = getSelectedKeywords(spark,15,2)
      val df_new = getSelectedKeywords(spark,15,4)    
      .filter(domain_filter)
      .withColumn("kw", split(col("kw"), " "))
      .filter(query)
      .withColumn("kw",getString(col("kw")))          

      df_new.write
            .format("csv")
            .option("header",true)
            .mode(SaveMode.Overwrite)
            .save("/datascience/misc/df_new_query_103921")

      */

      /**
      val df_old = spark.read.format("csv")
      .option("delimiter","\t")
      .option("header",false)
      .load("/datascience/keywiser/test/AR_big_scrapper_test_15Dsince27*")
      .toDF("device_type", "device_id","segment")
      .groupBy("device_id").agg(countDistinct("segment") as "segment_count")

      println("old overlap mean:")
      println(df_old.select(mean(col("segment_count"))).show())
      

      val df_new = spark.read.format("csv")
      .option("delimiter","\t")
      .option("header",false)
      .load("/datascience/keywiser/test/AR_big_scrapper_test_15Dsince1*")
      .toDF("device_type", "device_id","segment")
      .groupBy("device_id").agg(countDistinct("segment") as "segment_count")

      println("new overlap mean:")
      println(df_new.select(mean(col("segment_count"))).show())

      df_old.write
          .format("csv")
          .option("delimiter","\t")
          .option("header",true)
          .mode(SaveMode.Overwrite)
          .save("/datascience/misc/scrapper_overlap_old")

      df_new.write
          .format("csv")
          .option("delimiter","\t")
          .option("header",true)
          .mode(SaveMode.Overwrite)
          .save("/datascience/misc/scrapper_overlap_new")    


    */

    /**
    val country = "AR"
    val df = df_old.join(df_new,Seq("segment_id"))
    df.withColumn("country", lit(country))
    .write
    .format("csv")
    .option("delimiter","\t")
    .option("header",true)
    .mode("append") 
    .save("/datascience/misc/stem_test_results2")
    */
    
    //val countries = "AR,BR,CL,CO,EC,MX,PE,US".split(",").toList
    /*
    val countries = "AR".split(",").toList

    for (country <- countries) {

     var df_old = spark.read.format("csv")
    .option("delimiter","\t")
    .option("header",false)
    .load("/datascience/keywiser/test/%s_scrapper_test_15Dsince24*".format(country))
    .withColumnRenamed("_c1", "device_id")
    .withColumnRenamed("_c2", "segment_id")
    .groupBy("segment_id")
    .agg(countDistinct("device_id").as("count_old"))

    var df_new = spark.read.format("csv")
    .option("delimiter","\t")
    .option("header",false)
    .load("/datascience/keywiser/test/%s_scrapper_test_15Dsince1*".format(country))
    .withColumnRenamed("_c1", "device_id")
    .withColumnRenamed("_c2", "segment_id")
    .groupBy("segment_id")
    .agg(countDistinct("device_id").as("count_new"))

    var df = df_old.join(df_new,Seq("segment_id"))
    df.withColumn("country", lit(country))
    .write
    .format("csv")
    .option("delimiter","\t")
    .option("header",true)
    .mode("append") 
    .save("/datascience/misc/scrapper_test_results")
    }
    **/
    
    /**  
    val audiences = """MX_71172_2020-01-30T14-29-02-220256,MX_71172_2020-01-30T14-29-14-744166,MX_71172_2020-01-30T14-29-33-106219,MX_71172_2020-01-30T14-29-02-220256,MX_71172_2020-01-30T14-29-23-107754,MX_71172_2020-01-30T14-29-35-550514,MX_71172_2020-01-30T14-29-38-074317,MX_71172_2020-01-30T14-28-57-423908,MX_71172_2020-01-30T14-29-40-379240""".split(",").toList
    val ids = """124641,124643,124645,124647,124649,124651,124653,124655,124657""".split(",").toList

    // Total por audiencia
    for ((file, i) <- (audiences zip ids)){
        spark.read.format("csv")
        .option("sep", "\t")
        .load("/datascience/devicer/processed/%s".format(file))
        .filter("_c2 LIKE '%"+i+"%'")
        .write.format("csv")
        .option("sep", "\t")
        .mode("append")
        .save("/datascience/misc/amex_leo_feb6.csv")
    }

  

    def getString =
  udf((array: Seq[Integer]) => array.map(_.toString).mkString(","))
          
    val df = spark.read.format("csv")
        .option("sep", "\t")
        .load("/datascience/misc/amex_leo_feb6.csv")
        .toDF("device_type","device_id","segment")
        .groupBy("device_type","device_id").agg(collect_list("segment").as("segment"))
        .withColumn("segment",getString(col("segment")))
        .write.format("csv")
        .option("sep", "\t")
        .mode(SaveMode.Overwrite)
        .save("/datascience/misc/amex_leo_feb6_total")      

  */


  }
}