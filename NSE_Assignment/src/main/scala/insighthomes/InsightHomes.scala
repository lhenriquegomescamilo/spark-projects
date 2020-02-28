package main.scala.insighthomes

import main.scala.NSEFromHomes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime
import java.time.format.{ DateTimeFormatter}

object InsightHomes {

  /**
    * This method generates two datasets, one will be the hashed 
    */
  def create_homes_pipeline(
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) {
    
       
//Mapeo otra vez para estandarizar los device types. Es medio innecesario. Es un cleansing extra que en realidad debería venir limpito de antes. 
    val typeMap = Map(
      "coo" -> "web",
      "web" -> "web",
      "and" -> "android",
      "android"->"android",
      "ios" -> "ios",
      "idfa" -> "ios",
      "aaid"->"android",
      "unknown"->"unknown") 
    
    val mapUDF = udf((dev_type: String) => typeMap(dev_type))
    
//Acá obtenemos la fecha para después guardar la data
val country_output_dict = Map("argentina" -> "AR",
      "CL" -> "CL", 
      "mexico" -> "MX",
      "CO"-> "CO",
      "PE"-> "PE")
      
val actual_country = country_output_dict(value_dictionary("country")).toString
val today = (java.time.LocalDate.now)
val date = today.format(DateTimeFormatter.ofPattern("yyyy-MM")).toString


//Levantamos los homes expandidos. Hace falta que estén expandidos porque esto hay que enchufarlo con el resto del pipeline de insights
val homes = spark.read.format("csv")
 .option("delimiter","\t")
.option("header",true)
          .load(("/datascience/geo/NSEHomes/%s_xd".format(value_dictionary("output_file"))))
.select("device_type","device_id","GEOID","frequency")
.withColumn("ESTATE",substring(col("GEOID"), 0, 2)) //Esto es un hardcodeo medio feo, va a tomar los dos primeros dígitos del GEOID. En caso de Perú y Colombia, son los únicos que hay
.withColumn("device_type",mapUDF(col("device_type")))
.withColumn("country",lit(actual_country))
.withColumn("day",lit(date))


homes
.write
.format("parquet")
.mode("append")
.partitionBy("day","country")
.save("/datascience/data_insights/homes/")
  }
}
