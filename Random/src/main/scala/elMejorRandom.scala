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

val devuelta_format = devuelta.na.fill("").withColumn("geonse",split(col("geonse")," ")).withColumn("audience",split(col("id_aud"),";")).withColumn("audience",addValue(col("geonse"),col("audience"))).withColumn("audience",concat_ws(",",col("audience"))).select("id","audience")

val typeMap2 = Map(
      "aaid" -> "android",
      "idfa" -> "ios",
      "unknown" -> "unknown") 
val mapUDF2 = udf((dev_type: String) => typeMap2(dev_type))

val to_xd = devuelta_format.join(llave,Seq("id"))
.join(enviado.select("ad_id","id_type"),Seq("ad_id"))
.drop("id")
.select("id_type","ad_id","audience").withColumn("id_type",mapUDF2(col("id_type")))


to_xd
.write.format("csv")    
.option("header",true)    
.option("delimiter","\t")    
.mode(SaveMode.Overwrite)  
.save("/datascience/geo/Equifax/argentina_365d_home_1-10-2019-16h_to_xd")
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
}

 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("Spark devicer").config("spark.sql.files.ignoreCorruptFiles", "true").getOrCreate()


//Usuarios que fueron a un strip club. Esta es la web cookie
/*
    val raw_data_full =  spark.read.format("csv")
  .option("header",true)
  .option("delimiter","\t")
  .load("/datascience/geo/geo_processed/mex_alcohol_60d_mexico_named_poi_feature")

val strip_users = raw_data_full.filter("type == 'stripclub'").dropDuplicates("device_id")

val raw_xd = spark.read.format("csv")
  .option("header",false)
  .option("delimiter",",")
  .load("/datascience/audiences/crossdeviced/mex_alcohol_60d_mexico_30-10-2019-15h_aggregated_xd").select("_c0","_c1").distinct().toDF("madid","device_id")
  
val filter_strip_users = strip_users.join(raw_xd,Seq("device_id","osm_id", "common_name", "type")).withColumn("madid",upper(col("madid")))

val raw = spark.read.format("csv").option("header",true).option("delimiter","\t")
.load("/datascience/geo/raw_output/mex_alcohol_60d_mexico_30-10-2019-12h")
.withColumnRenamed("device_id","madid")
.withColumn("madid",upper(col("madid")))



filter_strip_users.join(raw,Seq("madid")).write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/geo_processed/mex_alcohol_60d_mexico_strip_club")
*/

  val raw = spark.read.format("csv").option("header",true).option("delimiter","\t")
  .load("/datascience/geo/raw_output/mex_alcohol_60d_mexico_30-10-2019-12h")
  .withColumnRenamed("device_id","madid").withColumn("madid",upper(col("madid")))
val freq_high = spark.read.format("csv")
  .option("header",true)
  .option("delimiter","\t")
  .load("/datascience/geo/geo_processed/mex_alcohol_60d_mexico_frequency")  .filter(col("freq") >= 20 || col("validUser") == true)
  .groupBy("feature").agg(countDistinct("device_id" )as "uniques")
  
val freq_low = spark.read.format("csv")
  .option("header",true)
  .option("delimiter","\t")
  .load("/datascience/geo/geo_processed/mex_alcohol_60d_mexico_frequency")  .filter(col("freq") < 20 || col("validUser") == false)
  .groupBy("feature").agg(countDistinct("device_id" ) as "uniques")
  
freq_high.join(raw,Seq("madid")).write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/geo_processed/mex_alcohol_60d_mexico_freq_high")

freq_low.join(raw,Seq("madid")).write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/geo_processed/mex_alcohol_60d_mexico_freq_low")


}

  
}