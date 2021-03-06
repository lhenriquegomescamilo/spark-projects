package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{round, broadcast, col, abs, to_date, to_timestamp, hour, date_format, from_unixtime,count, avg}
import org.apache.spark.sql.SaveMode
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import java.time.format.{ DateTimeFormatter}

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object unBuenRandom {
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
      since: String,
      country: String) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    //specifying country
    //val country_iso = "MX"
      
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


  def getDataPipelineMarkII(
      spark: SparkSession,
      path: String,
      nDays: String,
      since: String) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    //specifying country
    //val country_iso = "MX"
      
        // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/".format(day)) //
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



val ua = getDataPipeline(spark,"/datascience/data_useragents/","30","1","MX")
        .filter("model != ''") //con esto filtramos los desktop
        .withColumn("device_id",upper(col("device_id")))
        .drop("user_agent","event_type","url")
        .dropDuplicates("device_id")        
        //.filter("(country== 'AR') OR (country== 'CL') OR (country== 'MX')")

val segments = getDataPipeline(spark,"/datascience/data_triplets/segments/","15","1","MX")
              .withColumn("device_id",upper(col("device_id")))
              .groupBy("device_id").agg(concat_ws(",",collect_set("feature")) as "segments")

val joined = ua.join(segments,Seq("device_id"))
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/misc/ua_w_segments_30d_MX_II")

                                          }



def get_ua_urls(spark:SparkSession) = {

for (country_iso <- List("AR","CL","MX")) {

val ua = getDataPipeline(spark,"/datascience/data_useragents/","30","1",country_iso)
        .filter("model != ''") //con esto filtramos los desktop
        .withColumn("device_id",upper(col("device_id")))
        .drop("user_agent","event_type","url")
        .dropDuplicates("device_id")        
        //./datascience/data_triplets/urls/country=/"filter("(country== 'AR') OR (country== 'CL') OR (country== 'MX')")

val urls = spark.read.format("parquet").load("/datascience/data_triplets/urls/country=%s".format(country_iso))
              .withColumn("device_id",upper(col("device_id")))
              .groupBy("device_id").agg(concat_ws(",",collect_set("url")) as "urls",concat_ws(",",collect_set("domain")) as "domains")

val joined = ua.join(urls,Seq("device_id"))
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/misc/ua_30d_w_url_%s".format(country_iso))

val result = spark.read.format("csv")
.option("header",true)
.option("delimiter","\t")
.load("/datascience/misc/ua_30d_w_url_%s".format(country_iso))

result
    .withColumn("url",explode(split(col("urls"),",")))
    .groupBy("brand","url")
    .agg(countDistinct("device_id") as "url_count") 
    .write.format("csv")    
    .option("header",true)    
    .option("delimiter","\t")    
    .mode(SaveMode.Overwrite)    
    .save("/datascience/misc/ua_agg_url_%s".format(country_iso))

result
    .withColumn("domain",explode(split(col("domains"),",")))
    .groupBy("brand","domain")
    .agg(countDistinct("device_id") as "domains_count") 
    .write.format("csv")    
    .option("header",true)    
    .option("delimiter","\t")    
    .mode(SaveMode.Overwrite)    
    .save("/datascience/misc/ua_agg_domain_%s".format(country_iso))    
        }
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
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt)
      .map(end.minusDays(_))
      .map(_.toString(format))
      

    // Now we obtain the list of hdfs files to be read
    val path = "/datascience/geo/safegraph/"
    val hdfs_files = days
      .map(day => path +  "day=%s/country=%s/".format(day,country))
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

 val geo_hour = raw.select("device_id","device_type", "latitude", "longitude","utc_timestamp","name").withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
 .withColumn("Hour", date_format(col("Time"), "HH")).filter(col("Hour") >= HourFrom || col("Hour") <= HourTo)
                                                                 
                                                    
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

  val ua_ar = spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/misc/ua_w_segments_30d_AR_II")


  ua_ar.withColumn("segments",explode(split(col("segments"),",")))
    .groupBy("brand","segments")
    .agg(countDistinct("device_id") as "segment_country") 
    .write.format("csv")    
    .option("header",true)    
    .option("delimiter","\t")    
    .mode(SaveMode.Overwrite)    
    .save("/datascience/misc/ua_agg_segments_BRAND_30d_AR_II")


 val ua_cl = spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/misc/ua_w_segments_30d_CL_II")


  ua_cl.withColumn("segments",explode(split(col("segments"),",")))
    .groupBy("brand","segments")
    .agg(countDistinct("device_id") as "segment_country") 
    .write.format("csv")    
    .option("header",true)    
    .option("delimiter","\t")    
    .mode(SaveMode.Overwrite)    
    .save("/datascience/misc/ua_agg_segments_BRAND_30d_CL_II")   


 val ua_mx = spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/misc/ua_w_segments_30d_MX_II")


  ua_mx.withColumn("segments",explode(split(col("segments"),",")))
    .groupBy("brand","segments")
    .agg(countDistinct("device_id") as "segment_country") 
    .write.format("csv")    
    .option("header",true)    
    .option("delimiter","\t")    
    .mode(SaveMode.Overwrite)    
    .save("/datascience/misc/ua_agg_segments_BRAND_30d_MX_II")
 
}
def equifax_count ( spark: SparkSession){

  val segments_new = getDataPipeline(spark,"/datascience/data_triplets/segments/","1","30","AR")

val theNSE_new = segments_new.filter(col("feature") isin (35360,35361,35362, 35363))

theNSE_new.groupBy("feature").agg(countDistinct("device_id") as "unique_devices") 
.write.format("csv")    .option("header",true)    .option("delimiter","\t")    
.mode(SaveMode.Overwrite)    
.save("/datascience/misc/equifax_count_AR_new")


val segments_old = getDataPipeline(spark,"/datascience/data_triplets/segments/","30","30","AR")

val theNSE_old = segments_old.filter(col("feature") isin (35360,35361,35362, 35363))

theNSE_old.groupBy("feature").agg(countDistinct("device_id") as "unique_devices") 
.write.format("csv")    .option("header",true)    .option("delimiter","\t")    
.mode(SaveMode.Overwrite)    
.save("/datascience/misc/equifax_count_AR_old")
}

def metrics_geo_gcba ( spark: SparkSession) {
  //Geo Data


val geo = spark.read.format("parquet").option("sep","\t").option("header",true)
.load("/datascience/geo/safegraph/day=*/country=argentina/").filter("geo_hash == 'gcba'")

val count_miss = geo
.withColumn("compare",when(col("latitude")===col("longitude"),1)
  .otherwise(0))
.withColumn("day", to_timestamp(from_unixtime(col("utc_timestamp"))))
.withColumn("day", date_format(col("day"), "YYYYMMdd"))

val summary = count_miss.groupBy("day")
              .agg(count("ad_id") as "total_gcba",sum("compare") as "errors")


}

def reconstruct_equifax( spark: SparkSession) {
val typeMap = Map(
      "ABC1" -> "35360",
      "C2" -> "35361",
      "C3" -> "35362",
      "D1" -> "35362",
      "D2" -> "35362",
      "E" -> "35363") 
val mapUDF = udf((dev_type: String) => typeMap(dev_type))

val llave = spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/geo/Equifax/argentina_365d_home_1-10-2019-16h_hashed_key")
val enviado = spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/geo/Equifax/argentina_365d_home_1-10-2019-16h")
val devuelta = spark.read.format("csv").option("header",true).load("/datascience/geo/Equifax/cookies_geo_base_201910_enriquecidas.csv").withColumn("geonse",mapUDF(col("geonse")))


def addValue = udf( (firstcolumn: Seq[String],secondcolumn: Seq[String])=> firstcolumn ++ secondcolumn)

val devuelta_format = devuelta.na.fill("").withColumn("geonse",split(col("geonse")," ")).withColumn("audience",split(col("id_aud"),";")).withColumn("audience",addValue(col("geonse"),col("audience"))).withColumn("audience",concat_ws(",",col("audience"))).select("JCDindex","audience")

val typeMap2 = Map(
      "aaid" -> "android",
      "idfa" -> "ios",
      "unknown" -> "unknown") 
val mapUDF2 = udf((dev_type: String) => typeMap2(dev_type))

val to_xd = devuelta_format.join(llave,Seq("JCDindex"))
.join(enviado.select("ad_id","id_type"),Seq("ad_id"))
.drop("JCDindex")
.select("id_type","ad_id","audience").withColumn("id_type",mapUDF2(col("id_type")))


to_xd
.write.format("csv")    
.option("header",true)    
.option("delimiter","\t")    
.mode(SaveMode.Overwrite)  
.save("/datascience/geo/Equifax/argentina_365d_home_1-10-2019-16h_to_xd")
}

def get_segments_from_triplets_from_xd(
      spark: SparkSession,
      path_w_cookies: String
  ) = {

        val segments_raw = getDataPipeline(spark,"/datascience/data_triplets/segments/","10","1","CO")
                        

        val segments = segments_raw.groupBy("device_id","feature").agg(sum("count") as "count_in_days")
                        .withColumn("device_id",upper(col("device_id")))
                       
            segments.show(5)

        val data = spark.read
        .format("csv")
        .option("header", "false") // OJO ACA SI QUERES CAMBIAR EL CODIGO
        .option("sep", ",")
        .load(path_w_cookies)//.filter("device_type == 'web'")
        .select("_c1","_c2")
        .filter("_c2 == 'coo'")
        .drop("_c2")
        .toDF("device_id")
        .withColumn("device_id",upper(col("device_id")))

        data.show(5)

        val joint = data.join(segments, Seq("device_id"))
            
            joint.show(5)                  //.agg(count(col("device_id")) as "unique_count")  

      val output_path_segments = "/datascience/geo/geo_processed/%s_w_segments".format(path_w_cookies.split("/").last)

       joint.write.format("csv")
                    .option("header", "true")
                    .mode(SaveMode.Overwrite)
                    .save(output_path_segments)

                
                     
  }


def get_segments_from_triplets_for_geo_users(
      spark: SparkSession
  ) = {

        //Levantamos los segmentos
        val segments_raw = getDataPipeline(spark,"/datascience/data_triplets/segments/","30","1","MX")
                        

        val segments = segments_raw.filter(col("feature").isin(List("99593", "5022","920","275","2660","302","48174"):_*))
                                    .select("device_id","feature")
                                    .withColumn("device_id",upper(col("device_id")))

                       
       //LEvantamos lo GEO

       //direct
      val direct = spark.read.format("csv").option("header",true).option("delimiter","\t")
      .load("/datascience/geo/geo_processed/LuxoticaRadiosCiudades_geodevicer_mexico_sjoin_polygon")
      .withColumn("origin",lit("direct"))

      //madid
      val xd = spark.read.format("csv").option("header",false).option("delimiter",",")
                .load("/datascience/audiences/crossdeviced/LuxoticaRadiosCiudades_geodevicer_mexico_sjoin_polygon_xd")
                .select("_c1","_c2","_c5")
                .toDF("ad_id","id_type","name")
                .withColumn("origin",lit("xd"))

      
      val audience = List(direct,xd).reduce(_.unionByName (_))
                      .distinct()
                      .withColumn("device_id",upper(col("ad_id")))
                      .drop("ad_id")


      val joined = audience.join(segments,Seq("device_id"))
                    

      joined.write.format("csv")
      .option("header",true)
      .option("delimiter","\t")
      .mode(SaveMode.Overwrite)
      .save("/datascience/geo/geo_processed/Luxottica_raw")



                
                     
  }

def startapp_geo_metrics (spark: SparkSession) {


val geo = spark.read.format("csv")
.option("header",false)
.option("delimiter","\t")
.load("/datascience/audiences/crossdeviced/sample_startapp_xd")


println ("Devices by country")
geo.groupBy("country")
.agg(countDistinct("device_id") as "unique users",count("device_id") as "detections")
.write.format("csv")
.option("header",false)
.option("delimiter",",")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/geo_processed/startapp_geo_metrics")

//println ("Min,Max Date")
//geo.agg(min("timestamp"), max("timestamp")).show()




}





def get_mex_data( spark: SparkSession) 
{
/*
  val w_seg_users = spark.read.format("csv")
  .option("header",true)
  .option("delimiter",",")
  .load("/datascience/geo/geo_processed/mex_alcohol_60d_mexico_30-10-2019-15h_output_path_users_data")

val pois = spark.read.format("csv")
  .option("header",true)
  .load("/datascience/geo/POIs/mex_alcohol.csv")
  .select("type","common_name","osm_id")

val named = w_seg_users.join(pois,Seq("osm_id"))

val url = spark.read.format("parquet").option("header",true).option("delimiter","\t")
          .load("/datascience/data_triplets/urls/country=MX")

val domain = url.withColumn("domain",split(col("url"),"/")(0)).drop("url")

val domain_users = named.join(domain,Seq("device_id")).groupBy("domain","type").agg(countDistinct("device_id") as "unique_device")

domain_users
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/geo_processed/mex_alcohol_60d_mexico_user_domain")


  val raw_data_full_frequency = raw_xd.join(raw_data_full,Seq("device_id","osm_id"))

val chupi = List ("103928","103929","103928","166","103929","103930","103931","4776","85","103966","103967","5298")
  
val alcohol_user = raw_data_full.filter(col("feature").isin(chupi:_*))
val count_alcohol = alcohol_user.groupBy("type").agg(countDistinct("device_id") as "uniques")
  
val no_birra = raw_data_full
   .join(alcohol_user.select("device_id"), Seq("device_id"),"left_anti")
   
val count_no_birra = no_birra.groupBy("type").agg(countDistinct("device_id") as "uniques")

//println("con_alcohol",alcohol_user.select("device_id").distinct().count())
//println("sin_alcohol",no_birra.select("device_id").distinct().count())

count_alcohol
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/geo_processed/mex_alcohol_60d_mexico_birra_type")

count_no_birra.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/geo_processed/mex_alcohol_60d_mexico_no_birra_type")

*/

/*

//Esto es para agregarle la taxonomia a la data 27-12
val taxonomy = Seq(2, 3, 4, 5, 6, 7, 8, 9, 26, 32, 36, 59, 61, 82, 85, 92,
      104, 118, 129, 131, 141, 144, 145, 147, 149, 150, 152, 154, 155, 158, 160,
      165, 166, 177, 178, 210, 213, 218, 224, 225, 226, 230, 245, 247, 250, 264,
      265, 270, 275, 276, 302, 305, 311, 313, 314, 315, 316, 317, 318, 322, 323,
      325, 326, 352, 353, 354, 356, 357, 358, 359, 363, 366, 367, 374, 377, 378,
      379, 380, 384, 385, 386, 389, 395, 396, 397, 398, 399, 401, 402, 403, 404,
      405, 409, 410, 411, 412, 413, 418, 420, 421, 422, 429, 430, 432, 433, 434,
      440, 441, 446, 447, 450, 451, 453, 454, 456, 457, 458, 459, 460, 462, 463,
      464, 465, 467, 895, 898, 899, 909, 912, 914, 915, 916, 917, 919, 920, 922,
      923, 928, 929, 930, 931, 932, 933, 934, 935, 937, 938, 939, 940, 942, 947,
      948, 949, 950, 951, 952, 953, 955, 956, 957, 1005, 1116, 1159, 1160, 1166,
      2623, 2635, 2636, 2660, 2719, 2720, 2721, 2722, 2723, 2724, 2725, 2726,
      2727, 2733, 2734, 2735, 2736, 2737, 2743, 3010, 3011, 3012, 3013, 3014,
      3015, 3016, 3017, 3018, 3019, 3020, 3021, 3022, 3023, 3024, 3025, 3026,
      3027, 3028, 3029, 3030, 3031, 3032, 3033, 3034, 3035, 3036, 3037, 3038,
      3039, 3040, 3041, 3055, 3076, 3077, 3084, 3085, 3086, 3087, 3302, 3303,
      3308, 3309, 3310, 3388, 3389, 3418, 3420, 3421, 3422, 3423, 3470, 3472,
      3473, 3564, 3565, 3566, 3567, 3568, 3569, 3570, 3571, 3572, 3573, 3574,
      3575, 3576, 3577, 3578, 3579, 3580, 3581, 3582, 3583, 3584, 3585, 3586,
      3587, 3588, 3589, 3590, 3591, 3592, 3593, 3594, 3595, 3596, 3597, 3598,
      3599, 3600, 3779, 3782, 3913, 3914, 3915, 4097, 104014, 104015, 104016,
      104017, 104018, 104019)

//2) Acá están los usuarios y sus segmentos asociados de triplets
val audience_segments = spark.read.format("csv").option("header",true).option("delimiter",",")
.load("/datascience/misc/Luxottica/in_store_audiences_xd_luxottica_w_segments")
.withColumn("device_id",upper(col("device_id")))
.select("device_id","segment")
.withColumnRenamed("segment","interest")
.filter(col("interest").isin(taxonomy: _*))


val una_base = spark.read.format("csv").option("header",true).option("delimiter",",")
.load("/datascience/misc/Luxottica/in_store_audiences_w_group_taxo_gral")
.select("device_id")
.withColumn("device_id",upper(col("device_id")))
.distinct()

una_base.join(audience_segments,Seq("device_id"))
.write
.mode(SaveMode.Overwrite)
.format("csv")
.option("header",true)
.option("delimiter",",")
.save("/datascience/misc/Luxottica/in_store_audiences_w_group_taxo_gral_expanded")


*/



}




def getDataTriplets(
      spark: SparkSession,
      country: String,
      nDays: Int = -1,
      from: Int = 1
  ): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val path = "/datascience/data_triplets/segments/"

    val df: DataFrame = if (nDays > 0) {
      // read files from dates
      val format = "yyyyMMdd"
      val endDate = DateTime.now.minusDays(from)
      val days =
        (0 until nDays.toInt).map(endDate.minusDays(_)).map(_.toString(format))
      // Now we obtain the list of hdfs folders to be read
      val hdfs_files = days
        .map(day => path + "/day=%s/country=%s".format(day, country))
        .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

      val dfs = hdfs_files.map(
        f =>
          spark.read
            .parquet(f)
            .select("device_id", "feature")
            .withColumn("count", lit(1))
            .withColumnRenamed("feature", "segment")
      )
      dfs.reduce((df1, df2) => df1.unionAll(df2))
    } else {
      // read all date files
      spark.read.load(path + "/day=*/country=%s/".format(country))
    }
    df
  }

  def getDataTripletsCSVNSE(spark: SparkSession, country:String, nDays: Int, from: Int) = {
    val segments = List(136529,35360,136531,35361,136533,35362,136535,144741,35363,136537,210275,20107,20108,20109,20110,210265)
        .map(_.toInt)
        .toSeq

    

    val triplets = getDataTriplets(spark, country, nDays, from)
      triplets
        .filter(col("segment").isin(segments: _*))
        .withColumn("device_id",lower(col("device_id")))
        .select("device_id", "segment")
        .distinct()
        .write
        .format("csv")
        .mode("overwrite")
        .save("/datascience/geo/NSEHomes/NSE_GT_Equifax_%s_%s".format(nDays,country))
    }

 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("Spark devicer").config("spark.sql.files.ignoreCorruptFiles", "true").getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

val descriptor = "JCDecuax_Expanded_Ready"
val today = (java.time.LocalDate.now).toString

val useg  = spark.read.format("csv").option("header",true).option("delimiter",",")
.load("/datascience/geo/geo_processed/JCDecuax_Expanded_Ready_09-03_POIs_30d_mexico_9-3-2020-14h_users_data")
.select("device_id","feature")
.toDF("device_id","segmentId")
.withColumn("device_id",lower(col("device_id")))
//Estos usuarios tienen las features, y en base a estas features queremos meterles un nombre de audiencia. Queremos taggear los usuarios en base a su info web. Un user puede pertenecer a múltiples clusters. Después vamos a tener que ir a los carteles y contar, pero vamos a eso después. Primero el taggeo

//Aca levantamos un dataset que nos indica a que cluster pertenece según el feature

val cluster =  spark.read.format("csv").option("header",true).option("delimiter",",")
.load("/datascience/geo/Reports/JCDecaux/all_clusters_updated_13_02_20_II.csv")
.select("segmentId","longname").distinct()

//Acá tageamos a los usuarios, ya no necesitamos el segmentId original, lo tiramos y distinct
val tagged = useg.join(cluster,Seq("segmentId")).drop("segmentId").distinct()
//Con esto ya podríamos hacer conteo de usuarios por cartel, pero necesitamos por horario, así que vamos a des-crosdevicear, vamos a taggear a lo geo en base a esto
//Levantamos la tabla de equivalencias
val equiv = spark.read.format("csv").option("header",true).option("delimiter","\t")
.load("/datascience/geo/crossdeviced/JCDecuax_Expanded_Ready_09-03_POIs_30d_mexico_9-3-2020-14h_xd_equivalence_table")
.withColumn("device_id_origin",lower(col("device_id_origin")))
.withColumn("device_id_xd",lower(col("device_id_xd")))
.drop("device_type_origin","device_type_xd")

//Unimos a los tagged la tabla de equivalencias para recuperar el device original geo
val geo_tagged = tagged.withColumnRenamed("device_id","device_id_xd")
                .join(equiv,Seq("device_id_xd"))
                .drop("device_id_xd")
                .distinct()
                .withColumnRenamed("device_id_origin","device_id")
//geo_tagged.show(5)
//Ahora cada dispositivo geo lo tenemos taggeado, podemos ir a buscarlos al raw y ahí contar cuántos hay de cada cluster
//Ahora levantamos la raw_data. La necesitamos porque nos piden desagregación por franja horaria
//extraemos los tiempos
val raw = spark.read.format("csv").option("header",true).option("delimiter","\t")
.load("/datascience/geo/raw_output/JCDecuax_Expanded_Ready_09-03_POIs_30d_mexico_9-3-2020-14h")
.withColumn("device_id", lower(col("device_id")))
.withColumn("Time", to_timestamp(from_unixtime(col("timestamp"))))
 .withColumn("Hour", date_format(col("Time"), "HH"))
 .withColumn("Date", date_format(col("Time"), "MMM dd"))
 .withColumn("Day", date_format(col("Time"), "EEEE"))
 .withColumn("Month", date_format(col("Time"), "MMM"))
 .na.fill("0")
 .withColumn("WeekDay", when(col("Day").isin(List("Saturday", "Sunday"):_*), "WeekEnd").otherwise("WeekDay"))
 .withColumn("DayPeriod", when(col("Hour")>=0 && col("Hour")<6, "0 - EarlyMorning")
     .otherwise(when(col("Hour")>=6 && col("Hour")<11, "1 - Morning")
     .otherwise(when(col("Hour")>=11 && col("Hour")<14, "2 - Noon")
     .otherwise(when(col("Hour")>=14 && col("Hour")<18, "3 - Evening")
     .otherwise(when(col("Hour")>=18, "4 - Night"))))))

spark.conf.set("spark.sql.session.timeZone",  "GMT-5")
//Unimos el geotagged al raw 
val tagged_timed = geo_tagged.join(raw,Seq("device_id"),"right_outer").na.fill("No Cluster")

tagged_timed
.write
.mode(SaveMode.Overwrite)
.format("csv")
.option("header",true)
.option("delimiter","\t")
.save("/datascience/geo/Reports/JCDecaux/tagged_timed_%s".format(descriptor))

val tagged_time_up = spark.read.format("csv").option("header",true).option("delimiter","\t")
.load("/datascience/geo/Reports/JCDecaux/tagged_timed_%s".format(descriptor))

val cluster_time_count = tagged_time_up.groupBy("WeekDay","DayPeriod","JCDindex","longname")
.agg(countDistinct("device_id") as "uniques",count("device_id") as "detections")

val all_day_long_count = tagged_time_up.groupBy("WeekDay","JCDindex","longname")
.agg(countDistinct("device_id") as "uniques",count("device_id") as "detections")
.withColumn("DayPeriod",lit("24hs"))

val total_time_count = raw.groupBy("WeekDay","DayPeriod","JCDindex")
.agg(countDistinct("device_id") as "total_uniques",count("timestamp") as "total_detections")

val all_day_total_time_count = raw.groupBy("WeekDay","JCDindex")
.agg(countDistinct("device_id") as "total_uniques",count("timestamp") as "total_detections")
.withColumn("DayPeriod",lit("24hs"))

cluster_time_count
.repartition(1)
.write
.mode(SaveMode.Overwrite)
.format("csv")
.option("header",true)
.option("delimiter","\t")
.save("/datascience/geo/Reports/JCDecaux/cluster_time_count_%s_%s".format(descriptor,today))

all_day_long_count
.repartition(1)
.write
.mode(SaveMode.Overwrite)
.format("csv")
.option("header",true)
.option("delimiter","\t")
.save("/datascience/geo/Reports/JCDecaux/all_day_long_count_%s_%s".format(descriptor,today))

total_time_count
.repartition(1)
.write
.mode(SaveMode.Overwrite)
.format("csv")
.option("header",true)
.option("delimiter","\t")
.save("/datascience/geo/Reports/JCDecaux/total_time_count_%s_%s".format(descriptor,today))

all_day_total_time_count
.repartition(1)
.write
.mode(SaveMode.Overwrite)
.format("csv")
.option("header",true)
.option("delimiter","\t")
.save("/datascience/geo/Reports/JCDecaux/all_day_total_time_count_%s_%s".format(descriptor,today))


}


  
}
