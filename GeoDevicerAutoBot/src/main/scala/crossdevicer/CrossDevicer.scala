package main.scala.crossdevicer

import main.scala.Geodevicer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{upper, col, coalesce, udf}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object CrossDevicer {

  /**
    * This method generates the cross device of a given Geo audience. It takes the audience given
    * by parameters, then loads the Cross-Device Index and performs a join. It only obtains cookies
    * out of the cross-device.
    *
    * @param spark: Spark session object that will be used to load and store the DataFrames.
    * @param value_dictionary: Map that contains all the necessary information to run the match. The following fields are required:
    *        - poi_output_file: name of the output file.
    *
    * As a result, this method stores the cross-deviced audience in /datascience/audiences/crossdeviced
    * using the same name as the one sent by parameter.
    */
  def cross_device(
      spark: SparkSession,
      value_dictionary: Map[String, String],
      path: String =  "/datascience/geo/geodevicer_bot/outputs/%s/aggregated",
      sep: String = "\t",
      column_name: String = "_c1",
      columns_to_save: Seq[String], 
      header: String = "false"
  ) {
    // First we get the audience. Also, we transform the device id to be upper case.
    val audience = spark.read
      .format("csv")
      .option("sep", sep)
      .option("header", header)
      .load(path.format(value_dictionary("poi_output_file")))
      .withColumn("device_id", upper(col("device_id")))
    
    /*
    val columns_to_select = audience.columns.filter(
      !"timestamp,latitude_user,longitude_user,latitude_poi,longitude_poi,distance"
        .split(",")
        .toList
        .contains(_)
    )*/

    // Useful function to transform the naming from CrossDevice index to Rely naming.
    val typeMap = Map(
      "coo" -> "web",
      "and" -> "android",
      "android"->"android",
      "ios" -> "ios",
      "con" -> "TV",
      "dra" -> "drawbridge",
      "idfa" -> "ios",
      "aaid"->"android",
      "unknown"->"unknown") 
    
    val mapUDF = udf((dev_type: String) => typeMap(dev_type))

    // Get DrawBridge Index. Here we transform the device id to upper case too.
    // BIG ASSUMPTION: we only want the cookies out of the cross-device.

    val colNames = columns_to_save.map(name => col(name))

    val db_data = spark.read
      .format("parquet")
      .load("/datascience/crossdevice/double_index")
      .filter("index_type IN ('and', 'ios') AND device_type IN ('coo', 'ios', 'and')")
      .withColumn("index", upper(col("index")))
      .select("index", "device", "device_type")
      .withColumnRenamed("index", "device_id")
      .withColumnRenamed("device_type", "device_type_db")
      .withColumn("device_id", upper(col("device_id")))

   
    val cross_deviced_proto = db_data      
      .join(        
        audience    
           //,     value_dictionary("audience_column_name"),"validUser","frequency",     "device_id","device_type",           value_dictionary("poi_column_name"),
        .distinct(),        
        Seq("device_id"),
            "right_outer")
      .na.drop()


    val cross_deviced = cross_deviced_proto
      .withColumn("device_id", coalesce(col("device"), col("device_id")))      
      .withColumn("device_type",coalesce(col("device_type_db"), col("device_type")))
      .drop(col("device"))
      .drop(col("device_type_db"))
      .withColumn("device_type", mapUDF(col("device_type")))     
      
      val equivalence_table = cross_deviced_proto
      .select("device_id","device_type","device","device_type_db")
      .toDF("device_id_origin","device_type_origin","device_id_xd","device_type_xd")
      .withColumn("device_type_origin",mapUDF(col("device_type_origin")))
      .withColumn("device_type_xd",mapUDF(col("device_type_xd")))


      val cross_deviced_agg = cross_deviced.groupBy(colNames.filter(y => y.toString !=  value_dictionary("poi_column_name").toString):_*) // ,"validUser","frequency"
      .agg(collect_set(value_dictionary("poi_column_name")) as value_dictionary("poi_column_name"))
      .withColumn(value_dictionary("poi_column_name"), concat_ws(",", col(value_dictionary("poi_column_name"))))
      .select(colNames:_*)//.select("device_type","device_id",value_dictionary("poi_column_name")) //,"validUser","frequency" antes se seleccionaban estas para filtar luego, pero si se dejan el archivo no se empuja

    // We want information about the process
    cross_deviced_agg.explain(extended = true)

    // Finally, we store the result obtained.

    //This is the data so we can filter it
    val output_path = "/datascience/geo/geodevicer_bot/outputs/%s/xd".format(
      value_dictionary("poi_output_file")
    )

    cross_deviced_agg.write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(output_path)




 


//This is an equivalence table with the users and its expansion
val output_path_eq_table = "/datascience/geo/geodevicer_bot/outputs/%s/xd_equivalence_table".format(
      value_dictionary("poi_output_file")
    )
    equivalence_table.write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(output_path_eq_table)  


//We also generate a table with volume counts
 val allUserCount = already_saved
.withColumn(value_dictionary("poi_column_name"),split(col(value_dictionary("poi_column_name")),","))
.withColumn(value_dictionary("poi_column_name"),explode(col(value_dictionary("poi_column_name"))))
.groupBy(value_dictionary("poi_column_name")).agg(approx_count_distinct("device_id", rsd = 0.03) as "total_devices")

val validUserCount = already_saved
.filter("validUser == true")
.withColumn(value_dictionary("poi_column_name"),split(col(value_dictionary("poi_column_name")),","))
.withColumn(value_dictionary("poi_column_name"),explode(col(value_dictionary("poi_column_name"))))
.groupBy(value_dictionary("poi_column_name")).agg(approx_count_distinct("device_id", rsd = 0.03) as "valid_user_devices")

val output_path_volume_table = "/datascience/geo/geodevicer_bot/outputs/volume_count/%s".format(value_dictionary("poi_output_file"))

val volume = allUserCount
.join(validUserCount,Seq(value_dictionary("poi_column_name")))
.repartition(1)

volume
.repartition(1)
.write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(output_path_volume_table) 


  }

def ready_to_push(
      spark: SparkSession,
      value_dictionary: Map[String, String]) {
//Now that the crossdeviced table is already saved, we use it for the following proceses
val already_saved = spark.read.format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load(output_path)


//This is the same data, but ready to push
    val output_path_push = "/datascience/geo/geodevicer_bot/outputs/push/%s".format(value_dictionary("poi_output_file")
    )
    already_saved
    .select("device_type","device_id",value_dictionary("poi_column_name")) //.filter(col("frequency")>=value_dictionary("min_frequency_of_detection").toInt)
    .repartition(10)
    .write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(output_path_push)
}


}
