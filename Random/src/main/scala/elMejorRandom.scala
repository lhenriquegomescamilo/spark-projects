package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object elMejorRandom {
  def get_tapad_home_cluster(spark:SparkSession){

/*
val code = spark.read.format("csv").option("header",true).option("delimiter","\t")
.load("/datascience/geo/argentina_365d_home_21-8-2019-0h").withColumn("device_id",upper(col("ad_id")))


val poly = spark.read.format("csv").option("header",true).option("delimiter","\t")
.load("/datascience/geo/radios_argentina_2010_geodevicer_5d_argentina_14-8-2019-17h").withColumn("device_id",upper(col("device_id")))
*/
val codepoly = spark.read.format("csv").option("header",true).option("delimiter","\t")
.load("/datascience/geo/geospark_debugging/homes_AR_180_code_and_poly_for_crossdevice")

val home_index = spark.read.format("csv").option("delimiter","\t").load("/data/crossdevice/2019-09-10/")
.withColumn("tmp",split(col("_c2"),"="))
.select(col("_c0"),col("tmp").getItem(1).as("_c2")).drop("tmp").filter(col("_c2").isNotNull).toDF("house_cluster","device_id").withColumn("device_id",upper(col("device_id")))


codepoly.join(home_index,Seq("device_id"))
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/geospark_debugging/homes_AR_180_code_and_poly_home_cluster")



  }

def getDataPipeline(
      spark: SparkSession,
      path: String,
      nDays: String,
      since: String) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    //specifying country
    val country_iso = "MX"
      
        // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day,country_iso)) //
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    df
  }

def get_ua_segments(spark:SparkSession) = {

//
//val ua = spark.read.format("parquet")
//        .load("/datascience/data_useragents/day=*/country=AR")
 //       .filter("model != ''") //con esto filtramos los desktop
  //      .withColumn("device_id",upper(col("device_id")))
   //     .drop("user_agent","event_type","url")
    //    .dropDuplicates("device_id")



val ua = getDataPipeline(spark,"/datascience/data_useragents/","30","1")
        .filter("model != ''") //con esto filtramos los desktop
        .withColumn("device_id",upper(col("device_id")))
        .drop("user_agent","event_type","url")
        .dropDuplicates("device_id")        
        //.filter("(country== 'AR') OR (country== 'CL') OR (country== 'MX')")

val segments = getDataPipeline(spark,"/datascience/data_triplets/segments/","30","1")
              .withColumn("device_id",upper(col("device_id")))
              .groupBy("device_id").agg(concat_ws(",",collect_set("feature")) as "segments")

val joined = ua.join(segments,Seq("device_id"))
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/misc/ua_w_segments_30d_MX_II")

                                          }

def get_safegraph_data(
      spark: SparkSession,
      nDays: String,
      since: String,
      country: String
     
  ) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

   
    // Get the days to be loaded
    val format = "yyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt)
      .map(end.minusDays(_))
      .map(_.toString(format))
      

    // Now we obtain the list of hdfs files to be read
    val path = "/datascience/geo/safegraph_pipeline/"
    val hdfs_files = days
      .map(day => path +  "day=0%s/country=%s/".format(day,country))
      .filter(
        path => fs.exists(new org.apache.hadoop.fs.Path(path))
      )
      .map(day => day + "*.snappy.parquet")


    // Finally we read, filter by country, rename the columns and return the data
    val df_safegraph = spark.read
      .option("header", "true")
      .parquet(hdfs_files: _*)
      .dropDuplicates("ad_id", "latitude", "longitude")
      .select("ad_id", "id_type", "latitude", "longitude", "utc_timestamp")
      .withColumnRenamed("ad_id","device_id")
      .withColumnRenamed("id_type","device_type")
      .withColumn("device_id",upper(col("device_id")))

     df_safegraph                    
    
  }


  /*

Funciones  para telecentro

    val uas = spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/misc/ua_w_segments_5d")  
    
    uas.withColumn("segments",explode(split(col("segments"),","))).groupBy("brand","model","segments").agg(countDistinct("device_id"))
    .write.format("csv")
    .option("header",true)
    .option("delimiter","\t")
    .mode(SaveMode.Overwrite)
    .save("/datascience/misc/ua_agg_segments_5d")



//Hay que matchear con los PII para obtener los hashes

val telecentro_isp =  spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/audiences/crossdeviced/Telecentro_Test_ISP_to_push").withColumn("device_id",upper(col("device_id")))
val pii = spark.read.format("parquet").load("/datascience/pii_matching/pii_tuples/").withColumn("device_id",upper(col("device_id"))).filter("country = 'AR'").drop("device_type")

//telecentro_isp.show(2)
//pii.show(2)


val telecentro_hash = telecentro_isp.join(pii,Seq("device_id"))

     val ispMap = Map(
      "120885"->"Arnet",
       "120884"->"Speedy",
        "120883" ->"Fibertel",
          "120882"->"Telecentro")
          
          
          val audienceUDF = udf((dev_type: String) => ispMap(dev_type))
          
telecentro_hash.withColumn("ISP_Name",audienceUDF(col("ISP"))).write.format("csv").option("header",true).option("delimiter","\t").mode(SaveMode.Overwrite).save("/datascience/audiences/crossdeviced/Telecentro_Hash") 


    val hash_loaded = spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/audiences/crossdeviced/Telecentro_Hash")

hash_loaded.select("ml_sh2","mb_sh2","nid_sh2","ISP_Name").distinct().write.format("csv").option("header",true).option("delimiter","\t").mode(SaveMode.Overwrite).save("/datascience/audiences/crossdeviced/Telecentro_Hash_Unique") 



spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/audiences/crossdeviced/Telecentro_Hash_Unique").filter("ISP_Name == 'Arnet'").write.format("csv").option("header",true).option("delimiter","\t").mode(SaveMode.Overwrite).save("/datascience/audiences/crossdeviced/Telecentro_Hash_Arnet") 

spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/audiences/crossdeviced/Telecentro_Hash_Unique").filter("ISP_Name == 'Speedy'").write.format("csv").option("header",true).option("delimiter","\t").mode(SaveMode.Overwrite).save("/datascience/audiences/crossdeviced/Telecentro_Hash_Speedy") 

spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/audiences/crossdeviced/Telecentro_Hash_Unique").filter("ISP_Name == 'Fibertel'").write.format("csv").option("header",true).option("delimiter","\t").mode(SaveMode.Overwrite).save("/datascience/audiences/crossdeviced/Telecentro_Hash_Fibertel") 

spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/audiences/crossdeviced/Telecentro_Hash_Unique").filter("ISP_Name == 'Telecentro'").write.format("csv").option("header",true).option("delimiter","\t").mode(SaveMode.Overwrite).save("/datascience/audiences/crossdeviced/Telecentro_Hash_Telecentro") 
  }


val HourFrom = 19
val HourTo = 7

val raw = spark.read.format("csv").option("delimiter","\t").option("header",true).load("/datascience/geo/radios_argentina_2010_geodevicer_30d_argentina_30-8-2019-14h")

 val geo_hour = raw.select("device_id","device_type", "latitude", "longitude","utc_timestamp","name").withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp")))).withColumn("Hour", date_format(col("Time"), "HH")).filter(col("Hour") >= HourFrom || col("Hour") <= HourTo)
                                                                 
                                                    
val geo_counts = geo_hour.groupBy("device_id","device_type").agg(collect_list("name") as "radios_censales").withColumn("radios_censales", concat_ws(",", col("radios_censales")))

  geo_counts.write.format("csv").option("header",true).option("delimiter","\t").mode(SaveMode.Overwrite).save("/datascience/geo/geo_processed/radios_argentina_2010_geodevicer_30d_argentina_30-8-2019-14h_agg") 


// Esto es para el proceso de jcdaux
val safegraph_data = get_safegraph_data(spark,"60","1","mexico")
val all_audience_xd = spark.read.format("csv")
    .load("/datascience/audiences/crossdeviced/all_audience_a_k_s_h_a_xd")
    .select("_c1")
    .withColumnRenamed("_c1","device_id")
    .withColumn("device_id",upper(col("device_id")))

val joined = all_audience_xd.join(safegraph_data,Seq("device_id"))

joined.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/MX/JCDecaux/all_audience_xd_safegraph_60")

*/

/*

//tomo la audiencia de usuarios rankeados, madid y web
val audience_ranked = spark.read.format("csv").option("header",true).option("delimiter",",")
  .load("/datascience/geo/MX/JCDecaux/all_audience_ranked.csv")
  .withColumn("device_id",upper(col("device_id")))
  .select("device_id","audience","confidence")

//levanto tabla de equivlencia
val equivalence_table = spark.read.format("csv").load("/datascience/audiences/crossdeviced/all_audience_a_k_s_h_a_xd")
.select("_c0","_c1").toDF("device_id","device_id_xd")

//uso la tabla de equivalencia del XD para quedarme con los XD, ojo, acá me quedo sólo con el XD
val madid_w_category = equivalence_table.join(audience_ranked,Seq("device_id"))
.orderBy(asc("confidence"))
.drop("device_id")
.withColumnRenamed("device_id_xd","device_id")
.withColumn("device_id",upper(col("device_id")))
.dropDuplicates("device_id")

//levanto lo geo que había generado para esta audiencia los últimos 10 días. esto es todo méxico
val the_people_100 = spark.read.format("csv").option("header",true)
.option("delimiter","\t")
.load("/datascience/geo/MX/JCDecaux/all_audience_xd_safegraph_100")
.distinct()

//me quedo con los homes que estan en distrito federal
val homes_in_df = spark.read.format("csv")
.option("header",true)
.option("delimiter","\t")
.load("/datascience/geo/mexico_300d_home_6-9-2019-12h_w_NSE")
.withColumn("ENT", substring(col("CVEGEO"), 1, 2)).filter("ENT == '09'")
.withColumn("device_id",upper(col("ad_id"))).select("device_id")

val the_people_in_CITY = homes_in_df.join(the_people_100,Seq("device_id"))

val category_locations = the_people_in_CITY.join(madid_w_category,Seq("device_id"))


category_locations.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/MX/JCDecaux/category_locations_100")
*/

def get_homes_from_radius( spark: SparkSession) {
val df_users = spark.read.format("csv").option("delimiter","\t").load("/datascience/geo/geospark_debugging/sample_w_rdd_30_points_first_RDD_part*").dropDuplicates().toDF("device_id","utc_timestamp","radio")

  val value_dictionary: Map[String, String] = Map(
      "country" -> "argentina",
      "HourFrom" -> "19",
      "HourTo" -> "7",
      "UseType" -> "home",
      "minFreq" -> "0")


//dictionary for timezones
val timezone = Map("argentina" -> "GMT-3", "mexico" -> "GMT-5")
    
//setting timezone depending on country
spark.conf.set("spark.sql.session.timeZone", timezone(value_dictionary("country")))

val geo_hour = df_users     .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
                                            .withColumn("Hour", date_format(col("Time"), "HH"))
                                                .filter(
                                                    if (value_dictionary("UseType")=="home") { 
                                                                col("Hour") >= value_dictionary("HourFrom") || col("Hour") <= value_dictionary("HourTo") 
                                                                            } 
                                                    else {
                                                          (col("Hour") <= value_dictionary("HourFrom") && col("Hour") >= value_dictionary("HourTo")) && 
                                                                !date_format(col("Time"), "EEEE").isin(List("Saturday", "Sunday"):_*) })


val df_count  = geo_hour.groupBy(col("device_id"),col("radio"))
                        .agg(count(col("utc_timestamp")).as("freq"))

df_count
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/geospark_debugging/homes_from_polygons_AR_180")

                }




def aggregations_ua ( spark: SparkSession){

  val ua_ar = spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/misc/ua_w_segments_30d")


  ua_ar.withColumn("segments",explode(split(col("segments"),",")))
    .groupBy("brand","segments")
    .agg(countDistinct("device_id") as "segment_country") 
    .write.format("csv")    
    .option("header",true)    
    .option("delimiter","\t")    
    .mode(SaveMode.Overwrite)    
    .save("/datascience/misc/ua_agg_segments_BRAND_30d_AR")


 val ua_cl = spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/misc/ua_w_segments_30d_CL")


  ua_cl.withColumn("segments",explode(split(col("segments"),",")))
    .groupBy("brand","segments")
    .agg(countDistinct("device_id") as "segment_country") 
    .write.format("csv")    
    .option("header",true)    
    .option("delimiter","\t")    
    .mode(SaveMode.Overwrite)    
    .save("/datascience/misc/ua_agg_segments_BRAND_30d_CL")   


 val ua_mx = spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/misc/ua_w_segments_30d_MX")


  ua_mx.withColumn("segments",explode(split(col("segments"),",")))
    .groupBy("brand","segments")
    .agg(countDistinct("device_id") as "segment_country") 
    .write.format("csv")    
    .option("header",true)    
    .option("delimiter","\t")    
    .mode(SaveMode.Overwrite)    
    .save("/datascience/misc/ua_agg_segments_BRAND_30d_MX")
 
}
def equifax_count ( spark: SparkSession){

  val segments_new = getDataPipeline(spark,"/datascience/data_triplets/segments/","1","30")

val theNSE_new = segments_new.filter(col("feature") isin (35360,35361,35362, 35363))

theNSE_new.groupBy("feature").agg(countDistinct("device_id") as "unique_devices") 
.write.format("csv")    .option("header",true)    .option("delimiter","\t")    
.mode(SaveMode.Overwrite)    
.save("/datascience/misc/equifax_count_AR_new")


val segments_old = getDataPipeline(spark,"/datascience/data_triplets/segments/","30","30")

val theNSE_old = segments_old.filter(col("feature") isin (35360,35361,35362, 35363))

theNSE_old.groupBy("feature").agg(countDistinct("device_id") as "unique_devices") 
.write.format("csv")    .option("header",true)    .option("delimiter","\t")    
.mode(SaveMode.Overwrite)    
.save("/datascience/misc/equifax_count_AR_old")
}

 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("Spark devicer").config("spark.sql.files.ignoreCorruptFiles", "true").getOrCreate()


  /*
  val safegraph_data = get_safegraph_data(spark,"2","10","mexico")

  
*/
//get_homes_from_radius(spark)



//get_tapad_home_cluster(spark)
get_ua_segments(spark)
  }
}