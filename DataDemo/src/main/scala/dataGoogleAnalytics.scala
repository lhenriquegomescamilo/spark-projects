package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{upper, col,abs,udf,regexp_replace,split,lit,explode,length,count,mean,stddev}
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path

object DataGoogleAnalytics {
    /**
   * Este metodo se encarga de generar un dataframe de la pinta < device_id, mean(segment 2), mean(segment 3), ..., std(2), std(3), ..., count(2)>
   * utilizando la data ubicada en data_audiences_p y la distribucion de google analytics.
   * Una vez generado el dataframe se lo guarda en formato
   * parquet dentro de /datascience/data_demo/data_google_analytics
   * Los parametros que recibe son:
   * 
   * @param spark: Spark session object que sera utilizado para cargar los DataFrames.
   * @param ndays: cantidad de dias que se utilizaran para generar los triplets.
   * 
   */
    def get_data_google_analytics(spark:SparkSession,ndays:Int){
        /// Configuraciones de spark
        val sc = spark.sparkContext
        val conf = sc.hadoopConfiguration
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)
        
        /// Obtenemos la data de los ultimos ndays
        val format = "yyyyMMdd"
        val start = DateTime.now.minusDays(ndays)
        val end   = DateTime.now.minusDays(0)

        val daysCount = Days.daysBetween(start, end).getDays()
        val days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))
        
        val dfs = days
                    .filter(day => fs.exists(new org.apache.hadoop.fs.Path("/datascience/data_audiences_p/day=%s".format(day))))
                    .map(x => spark.read.format("parquet").load("/datascience/data_audiences_p/day=%s".format(x)).select("device_id","url"))
                    
        /// Concatenamos los dataframes y nos quedamos solamente con el dominio de la url
        val df = dfs.reduce((df1,df2) => df1.union(df2))
                    .withColumn("url",regexp_replace(col("url"), """http*://""", ""))
                    .withColumn("url",regexp_replace(col("url"), """https*://""", ""))
        
        /// Leemos el archivo que tiene las distribuciones para cada URL
        val distributions = spark.read.format("csv").option("header","true")
                                 .load("/datascience/data_demo/sites_distribution.csv")
                                 .drop("_c0")
                                 .withColumnRenamed("index","url")

        /// Hacemos el join de ambos dataframes para obtener la distribucion de cada device_id
        val joint = df.join(broadcast(distributions),Seq("url"))

        /// Agrupamos por device_id y sacamos algunas estadisticas para cada segmento (promedio, desvio estandar y cantidad)
        ///  Aclaracion: Se renombran las columnas al final para que no haya errores al gurdar en formato parquet.
        val statistics = joint.groupBy("device_id")
                                .agg(mean("2"), mean("3"), mean("4"), mean("5"), mean("6"), mean("7"), mean("8"), mean("9"),
                                    stddev("2"), stddev("3"), stddev("4"), stddev("5"), stddev("6"), stddev("7"), stddev("8"), stddev("9"),
                                    count("2"), count("3"), count("4"), count("5"), count("6"), count("7"), count("8"), count("9"))
                                .na.fill(0)
                                .toDF("device_id","mean_2","mean_3","mean_4","mean_5","mean_6","mean_7","mean_8","mean_9", 
                                        "std_2","std_3","std_4","std_5","std_6","std_7","std_8","std_9",
                                        "count_2","count_3","count_4","count_5","count_6","count_7","count_8","count_9") 
                                
        /// Las guardamos en formato parquet
        statistics.write.format("parquet")
                .mode(SaveMode.Overwrite)
                .save("/datascience/data_demo/data_google_analytics")
    }

    def main(args: Array[String]) {
        /// Configuracion spark
        val spark = SparkSession.builder.appName("Data Demo: Google Analytics").getOrCreate()

        // Parseo de parametros
        val ndays = if (args.length > 0) args(0).toInt else 30
        
        get_data_google_analytics(spark,ndays)
    }
  }