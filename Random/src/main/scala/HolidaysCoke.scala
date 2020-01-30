package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{round, broadcast, col, abs, to_date, to_timestamp, hour, date_format, from_unixtime,count, avg}
import org.apache.spark.sql.SaveMode
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.apache.spark.sql.expressions.Window


/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */

 
object HolidaysCoke {
  
//Acá el código para correr Peru
def get_safegraph_data(
      spark: SparkSession,
      nDays: String,
      since: String,
      decimals: String,
      country: String) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
​ 
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
      .withColumnRenamed("ad_id", "device_id")
      .withColumnRenamed("id_type", "device_type")
      .withColumn( "lat_user",((col("latitude").cast("float"))))
      .withColumn( "lon_user",((col("longitude").cast("float"))))
      .withColumn("geocode",((abs(col("lat_user")) * decimals).cast("int") * decimals * 100) + (abs(col("lon_user")) * decimals).cast("int"))
      .withColumn("Time", to_timestamp(from_unixtime(col("utc_timestamp"))))
      .withColumn("Date", date_format(col("Time"),"M-dd"))
      .select("device_id","device_type","lat_user","lon_user","geocode","Date")
    df_safegraph } 
          
 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark = SparkSession.builder
    .appName("HolidaysCoke")
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .getOrCreate()
    .conf.set("spark.sql.session.timeZone","PE")

    //Acá usamos la función para levantar la data de safegraph y crearle las columnas necesarias          
    //OJO QUE DEPENDIENDO DEL PAIS HAY QUE CAMBIARLO***********************************************
    val safegraph_data = get_safegraph_data(spark,"30","1","10","PE")
    ​
    ​
    //Acá generamos un conteo de geocodes por usuario por dia
    val devices_geocode_counts = safegraph_data.groupBy("device_id","Date","geocode").agg(count("lat_user") as "geocode_count_by_day")
    ​
    //Ahora nos quedamos con el mayor geocode count para un determinado día
    val w = Window.partitionBy(col("device_id"),col("Date")).orderBy(col("geocode_count_by_day").desc) //esto es como una máscara, ordenamos descentendente
    val devices_single_top_geocode = devices_geocode_counts.withColumn("rn", row_number.over(w)).where(col("rn") === 1).drop("rn") //acá le inventas una columna y te quedás con la primer ocurrencia
    ​
    // Ahora nos queremos quedar con un lat long de verdad donde haya estado ese usuario
    //Para eso eliminamos duplicados de la data de safegraph 
    //Ahora tenemos una latitud y longitud por día, representativa del geocode de mayor frecuencia.
    val top_geocode_by_user_by_day_w_coordinates = devices_single_top_geocode.join(safegraph_data.dropDuplicates("device_id","Date","geocode"),Seq("device_id","Date","geocode"))
    ​
    //Acá levantamos el dataset de homes
    //OJO QUE DEPENDIENDO DEL PAIS HAY QUE CAMBIARLO***********************************************
    val path = "/datascience/geo/PE_90d_home_14-1-2020-19h"
    val df_homes = spark.read
    .format("csv")
    .option("sep","\t")
    .option("header",true)
    .load(path)
    .toDF("device_id","pii_type","freq","else","lat_home","lon_home")
    .filter("lat_home != lon_home")
    .withColumn( "lat_home",((col("lat_home").cast("float"))))
    .withColumn( "lon_home",((col("lon_home").cast("float"))))
    .select("device_id","lat_home","lon_home")
    ​​
    val km_limit = 200*1000
    //val km_limit = 50
    ​
    //Aramos el vs dataset
    val device_vs_device = df_homes.join(top_geocode_by_user_by_day_w_coordinates,Seq("device_id"))
    ​
    // Using vincenty formula to calculate distance between user/device location and ITSELF.
    device_vs_device.createOrReplaceTempView("joint")
    ​
    val columns = device_vs_device.columns
    ​
    val query =
    """SELECT lat_user,
                lon_user,
                Date,
                lat_home,
                lon_home,
                device_id,
                device_type,
                distance
            FROM (
            SELECT *,((1000*111.045)*DEGREES(ACOS(COS(RADIANS(lat_user)) * COS(RADIANS(lat_home)) *
            COS(RADIANS(lon_user) - RADIANS(lon_home)) +
            SIN(RADIANS(lat_user)) * SIN(RADIANS(lat_home))))) as distance
            FROM joint 
            )
            WHERE distance > %s""".format(km_limit)      

    // Storing result
    val sqlDF = spark.sql(query)
    .withColumn( "distance",(col("distance")/ 1000)).orderBy(desc("distance")).na.fill(0).filter("distance>0")
    ​
    sqlDF
    .write
    .mode(SaveMode.Overwrite)
    .format("csv")
    .option("delimiter","\t")
    .option("header",true)
    .save("/datascience/geo/misc/travelers_from_home_PE_JAN_30_2020")
    ​   ​
    //una vez que tenemos la audiencia VACACionantes, se la restamos a los homes para obtener los no vacacionantes
    ​
    //Peru
    //Primero juntamos madid y XD
    ​
    val home_madid_path = "/datascience/geo/CL_90d_home_14-1-2020-16h"
    val home_xd_path = "/datascience/audiences/crossdeviced/PE_90d_home_14-1-2020-19h_xd"
    ​
    val homes_madid = spark.read.format("csv")
    .option("delimiter","\t")
    .option("header",true)
    .load(home_madid_path)
    .select("ad_id","id_type")
    .toDF("device_id","device_type")
    ​
    val homes_xd = spark.read.format("csv")
    .option("delimiter",",")
    .load(home_xd_path)
    .select("_c1","_c2")
    .toDF("device_id","device_type")
    ​
    val typeMap = Map(
        "coo" -> "web",
        "and" -> "android",
        "aaid" -> "android",
            "android" -> "android",
        "unknown" -> "android",
        "ios" -> "ios",
        "idfa"->"ios")
        
    val mapUDF = udf((aud: String) => typeMap(aud))
    ​
    val homes = List(homes_madid,homes_xd).reduce(_.unionByName (_)).withColumn("device_type",mapUDF(col("device_type")))

    val country = "PE"
    val vacacion_path = "/datascience/geo/misc/travelers_from_home_PE_JAN_30_2020"
    ​
    ​
    val vacaciones = spark.read.format("csv")
    .option("delimiter","\t")
    .option("header",true)
    .load(vacacion_path)
    .select("device_type","device_id")
    ​
    val no_vacacion =  homes.join(vacaciones,Seq("device_id"),"left_anti")
    ​
    no_vacacion.write
    .mode(SaveMode.Overwrite)
    .format("csv")
    .option("delimiter","\t")
    .option("header",true)
    .save("/datascience/geo/misc/stay_at_home_full_audience_%s_JAN_30_2020".format(country))

       }
  }

