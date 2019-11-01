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
      path: String =  "/datascience/geo/geo_processed/%s_aggregated",
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

   
      val cross_deviced = db_data      
      .join(        
        audience    
        .select(colNames:_*)      //,     value_dictionary("audience_column_name"),"validUser","frequency",     "device_id","device_type",           value_dictionary("poi_column_name"),
        .distinct(),        
        Seq("device_id"),
            "right_outer")      
      .withColumn("device_id", coalesce(col("device"), col("device_id")))      
      .withColumn("device_type",coalesce(col("device_type_db"), col("device_type")))
      .drop(col("device"))
      .drop(col("device_type_db"))
      .withColumn("device_type", mapUDF(col("device_type")))

      val cross_deviced_agg = cross_deviced.groupBy(colNames.filter(y => y.toString !=  value_dictionary("poi_column_name").toString):_*) // ,"validUser","frequency"
      .agg(collect_set(value_dictionary("poi_column_name")) as value_dictionary("poi_column_name"))
      .withColumn(value_dictionary("poi_column_name"), concat_ws(",", col(value_dictionary("poi_column_name"))))
      .select(colNames:_*)//.select("device_type","device_id",value_dictionary("poi_column_name")) //,"validUser","frequency" antes se seleccionaban estas para filtar luego, pero si se dejan el archivo no se empuja

    // We want information about the process
    cross_deviced_agg.explain(extended = true)

    // Finally, we store the result obtained.
    val output_path = "/datascience/geo/crossdeviced/%s_xd".format(
      value_dictionary("poi_output_file")
    )
    cross_deviced_agg.write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(output_path)
  }
}
