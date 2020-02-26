package main.scala.equifaxhomes

import main.scala.NSEFromHomes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime
import java.time.format.{ DateTimeFormatter}

object EquifaxHomes {

  /**
    * This method generates two datasets, one will be the hashed 
    */
  def create_hash_for_madids(
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) {
    


    //Levantamos los homes que crea este proceso. Estos homes NO tienen XD.
    val homes = spark.read.format("csv")
          .option("delimiter","\t")
          .load(("/datascience/geo/NSEHomes/%s".format(value_dictionary("output_file"))))
          .toDF("device_id","device_type","freq","geocode","latitude","longitude")
          .drop("geocode","device_type")


    //Nos aseguramos de quedarnos con el geocode de mayor frecuencia para el usuario
    val w = Window.partitionBy(col("device_id")).orderBy(col("freq").desc)
    val Top_homes = homes.withColumn("rn", row_number.over(w)).where(col("rn") === 1).drop("rn")

    //Acá nos quedamos con una tabla de equivalencias que después tenemos que usar para recomponer los datos, vamos a pasar sólo el hash
    val homes_are_hashed = homes.drop("freq")
    .withColumn("device_id_hash", sha2(col("device_id"),256))
    

    homes_are_hashed
    .select("device_id","device_id_hash")
    .write.format("csv")
    .option("header",true)
    .option("delimiter","\t") 
    .mode(SaveMode.Overwrite)
    .save("/datascience/geo/NSEHomes/monthly/equifax/keys/%s_hashed_key".format(value_dictionary("output_file"))) 

    //Aca levantamos lo que acabamos de crear y nos quedamos sólo con el device_id hash. 

    val country_output = Map
    ("argentina" -> "AR",
      "CL" -> "CL", 
      "mexico" -> "MX",
      "CO"-> "CO",
      "PE"-> "PE")

 
    val today = (java.time.LocalDate.now)
    val date = today.format(DateTimeFormatter.ofPattern("yyyy-MM")).toString

    val homes_equifax = 
      homes_are_hashed
    .select("device_id_hash","latitude","longitude")
    .repartition(1)
    .write      .format("csv")
    .option("header",true)
    .option("delimiter","\t") 
    .mode(SaveMode.Overwrite)
    .save("/datascience/geo/NSEHomes/monthly/equifax/to_push/%s/%s".format(date,country_output(value_dictionary("country"))))
  }
}
