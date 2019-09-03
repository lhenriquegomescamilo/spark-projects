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

val code = spark.read.format("csv").option("header",true).option("delimiter","\t")
.load("/datascience/geo/argentina_365d_home_21-8-2019-0h").withColumn("device_id",upper(col("ad_id")))


val poly = spark.read.format("csv").option("header",true).option("delimiter","\t")
.load("/datascience/geo/radios_argentina_2010_geodevicer_5d_argentina_14-8-2019-17h").withColumn("device_id",upper(col("device_id")))

val index = spark.read.format("csv").option("delimiter","\t").load("/data/crossdevice/2019-07-14/Retargetly_ids_full_20190714_193703_part_00.gz")
val home_index = index.withColumn("tmp",split(col("_c2"),"=")).select(col("_c0"),col("tmp").getItem(1).as("_c2")).drop("tmp").filter(col("_c2").isNotNull).toDF("house_cluster","device_id").withColumn("device_id",upper(col("device_id")))


poly.join(home_index,Seq("device_id"))
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/AR/tapad_w_polygon")

code.join(home_index,Seq("device_id"))
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/AR/tapad_w_geocode")

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
    val country_iso = "AR"
      
        // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since.toInt)
    val days = (0 until nDays.toInt).map(end.minusDays(_)).map(_.toString(format))

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day,country_iso))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

    df
  }

def get_ua_segments(spark:SparkSession) = {

val ua = spark.read.format("parquet")
        .load("/datascience/data_useragents/day=*/country=AR")
        .filter("model != ''") //con esto filtramos los desktop
        .withColumn("device_id",upper(col("device_id")))
        .drop("user_agent","event_type","url")
        .dropDuplicates("device_id")

val segments = getDataPipeline(spark,"/datascience/data_triplets/segments/","5","10")
              .withColumn("device_id",upper(col("device_id")))
              .groupBy("device_id").agg(concat_ws(",",collect_set("feature")) as "segments")

val joined = ua.join(segments,Seq("device_id"))
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/misc/ua_w_segments_5d")

                                          }

def get_safegraph_data(
      spark: SparkSession,
      nDays: String,
      since: String
     
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
      .map(day => path +  "day=0%s/country=%s/".format(day,value_dictionary("country")))
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
      .withColumnRenamed("ad_id","device_type")
      .withColumnRenamed("id_type","device_id")


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

*/


 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("Spark devicer").config("spark.sql.files.ignoreCorruptFiles", "true").getOrCreate()

  

   val safegraph_data = get_safegraph_data(spark,"5","1")
  

    val all_audience_xd = spark.read.format("csv")
    .load("/datascience/audiences/crossdeviced/all_audience_a_k_s_h_a_xd")
    .select("_c0","_c1","_c2")
    .toDF("device_id","device_xd","device_type_xd")

val joined = all_audience_xd.join(safegraph_data,Seq("device_id"))

joined.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.mode(SaveMode.Overwrite)
.save("/datascience/geo/MX/JCDecaux/all_audience_xd_safegraph")

  }
}