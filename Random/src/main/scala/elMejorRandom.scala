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

val code = spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/geo/argentina_5d_home_14-8-2019-17h").withColumn("device_id",upper(col("ad_id")))


val poly = spark.read.format("csv").option("header",true).option("delimiter","\t").load("/datascience/geo/radios_argentina_2010_geodevicer_5d_argentina_14-8-2019-17h").withColumn("device_id",upper(col("device_id")))

val index = spark.read.format("csv").option("delimiter","\t").load("/data/crossdevice/2019-07-14/Retargetly_ids_full_20190714_193703_part_00.gz")
val home_index = index.withColumn("tmp",split(col("_c2"),"=")).select(col("_c0"),col("tmp").getItem(1).as("_c2")).drop("tmp").filter(col("_c2").isNotNull).toDF("house_cluster","device_id").withColumn("device_id",upper(col("device_id")))


poly.join(home_index,Seq("device_id"),"left")
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.save("/datascience/geo/AR/tapad_w_polygon")

code.join(home_index,Seq("device_id"),"left")
.write.format("csv")
.option("header",true)
.option("delimiter","\t")
.save("/datascience/geo/AR/tapad_w_geocode")

  }


  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder.appName("Spark devicer")
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)
    
    
    get_tapad_home_cluster(spark)
     
  }
}