package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{upper, col,abs,udf,regexp_replace,split,lit}
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path

object GenerateTriplets {
    /**
   * Este metodo se encarga de generar una lista de triplets de la pinta <device_id, segment, count>
   * utilizando la data ubicada en data_keywords_p. Una vez generado el dataframe se lo guarda en formato
   * parquet dentro de /datascience/data_demo/triplets_segments
   * Los parametros que recibe son:
   * 
   * @param spark: Spark session object que sera utilizado para cargar los DataFrames.
   * @param ndays: cantidad de dias que se utilizaran para generar los triplets.
   * 
   */
    def generate_triplets_segments(spark:SparkSession,ndays:Int){
        /// Obtenemos la data de los ultimos ndays
        val format = "yyyyMMdd"
        val start = DateTime.now.minusDays(ndays)
        val end   = DateTime.now.minusDays(0)

        val daysCount = Days.daysBetween(start, end).getDays()
        val days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))
        
        val dfs = days
                    .filter(day => fs.exists(new org.apache.hadoop.fs.Path("/datascience/data_keywords_p/day=%s".format(day))))
                    .map(x => spark.read.parquet("/datascience/data_keywords_p/day=%s".format(x))
                                    .select("device_id","all_segments")
                                    .withColumn("all_segments",explode(col("all_segments")))
                                    .withColumnRenamed("all_segments","feature")
                                    .withColumn("count",lit(1)))

        val df = dfs.reduce((df1,df2) => df1.union(df2))
        
        df.write.format("parquet")
                .mode(SaveMode.Overwrite)
                .save("/datascience/data_demo/triplets_segments")
    }
    /**
   * Este metodo se encarga de generar una lista de triplets de la pinta <device_id, keyword, count>
   * utilizando la data ubicada en data_keywords_p. Para generar los triplets se utilizaran tanto las keywords
   * provenientes de las urls visitadas por los usuarios como las keywords provienentes del contenido de las urls (scrapping)-
   * Una vez generado el dataframe se lo guarda en formato parquet dentro de /datascience/data_demo/triplets_keywords
   * Los parametros que recibe son:
   * 
   * @param spark: Spark session object que sera utilizado para cargar los DataFrames.
   * @param ndays: cantidad de dias que se utilizaran para generar los triplets.
   * 
   */
    def generate_triplets_keywords(spark:SparkSession,ndays:Int){
        /// Obtenemos la data de los ultimos ndays
        val format = "yyyyMMdd"
        val start = DateTime.now.minusDays(ndays)
        val end   = DateTime.now.minusDays(0)

        val daysCount = Days.daysBetween(start, end).getDays()
        val days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))
        
        val dfs = days
                    .filter(day => fs.exists(new org.apache.hadoop.fs.Path("/datascience/data_keywords_p/day=%s".format(day))))
                    .map(x => spark.read.parquet("/datascience/data_keywords_p/day=%s".format(x))
                                    .select("device_id","url_keys","content_keys"))

        val df = dfs.reduce((df1,df2) => df1.union(df2))

        /// Obtenemos las keywords del contenido de la url 
        val df_content_keys = df.withColumn("content_keys",explode(col("content_keys")))
                                .withColumnRenamed("content_keys","feature")
                                .withColumn("count",lit(1))
        
        /// Obtenemos las keywords provenientes de la url                             
        val df_url_keys = df.withColumn("url_keys",explode(col("url_keys")))
                            .withColumnRenamed("url_keys","feature")
                            .withColumn("count",lit(1))
        
        /// Unimos ambas keywords y las guardamos
        df_content_keys.unionAll(df_url_keys)
                        .write.format("parquet")
                        .option("header",true)
                        .mode(SaveMode.Overwrite)
                        .save("/datascience/data_demo/triplets_keywords")
      
    }
    def main(args: Array[String]) {
        /// Configuracion spark
        val spark = SparkSession.builder.appName("Get triplets: keywords and segments").getOrCreate()
        generate_triplets_segments(spark)
        generate_triplets_keywords(spark)
    
    }
  }
