package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions.{explode,desc,lit,size,concat,col,concat_ws,collect_list,udf,broadcast}
import org.joda.time.{Days,DateTime}
import org.apache.hadoop.fs.Path

object generateOrganic {
    def generate_organic(spark:SparkSession,ndays:Int){
        val sc = spark.sparkContext
        val conf = sc.hadoopConfiguration
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)

        /// Lista de dias con los que se generara el sample
        val format = "yyyyMMdd"
        val start = DateTime.now.minusDays(ndays)
        val end   = DateTime.now.minusDays(0)

        val daysCount = Days.daysBetween(start, end).getDays()
        val days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))
        
        
        /// Leemos la data de data_audiences_p con la cual generaremos el sample.
        val dfs = days.reverse
                        .filter(day => fs.exists(new org.apache.hadoop.fs.Path("/datascience/data_audiences_p/day=%s".format(day))))
                        .map(x => spark.read.parquet("/datascience/data_audiences_p/day=%s".format(x))
                        .filter("country = 'MX'")
                        .withColumn("day",lit(x))
                        .select("device_id","day","third_party"))

        val df = dfs.reduce((df1,df2) => df1.union(df2))

        /// Leemos los archivos con los segmentos para la taxo general y la taxo geo
        val taxo_general = spark.read.format("csv").option("sep","\t")
                                                    .option("header","True")
                                                    .load("/datascience/taxo_general.csv")
                                                    .select("Segment ID")
                                                    .rdd.map(row=>row(0)).collect()

        
        /// Hacemos un broadcast de estas listas ya que son chicas
        val taxo_general_b = sc.broadcast(taxo_general)

        /// Esta UDF recibe una lista de segmentos y se queda con los pertenecientes a la taxo general
        val udfGralSegments = udf((segments: Seq[String]) => segments.filter(segment => taxo_general_b.value.contains(segment)))
        /// Esta UDF recibe una lista de segmentos y un dia, y genera una tupla (segmento, dia)
        val udfAddDay = udf((segments: Seq[String], day: String) => segments.map(segment => (segment, day)))
        /// Esta UDF recibe una lista de listas de tuplas y lo que hace es convertir todo a string y aplanar estas listas
        val udfFlattenLists = udf((listOfLists: Seq[Seq[Row]]) => listOfLists.flatMap(list => list.map(row => (row(0).asInstanceOf[String], row(1).asInstanceOf[String]))))
        /// Esta UDF recibe una lista de tuplas y descarta las duplicadas quedandose con la de fecha mas reciente 
        /// (dado un mismo segmento se queda con el mas reciente)
        val udfDropDuplicates = udf((segments: Seq[Row]) => segments.map(row => (row(0).asInstanceOf[String], row(1).asInstanceOf[String]))
                                                                    .groupBy(row => row._1)
                                                                    .map(row => row._2.sorted.last).toList
                                                                    .map(tuple => "%s:%s".format(tuple._1, tuple._2))
                                                                    .mkString(","))

        /**
        Los pasos a seguir son los siguientes:
        1) Nos quedamos con los segmentos de la taxo general y de la taxo geo
        2) Armamos una lista de tuplas (segmento, dia)
        3) De la lista del punto anterior descartamos los duplicados y nos quedamos con los segmentos que fueron asignados
            mas recientemente.
        **/
        val userSegments = df.withColumn("gral_segments", udfGralSegments(col("third_party")))
                            .withColumn("gral_segments", udfAddDay(col("gral_segments"), col("day")))
                            .groupBy("device_id")
                            .agg(collect_list("gral_segments") as "gral_segments")
                            .withColumn("gral_segments", udfFlattenLists(col("gral_segments")))
                            .withColumn("gral_segments", udfDropDuplicates(col("gral_segments")))

        userSegments.write.format("csv").option("sep", "\t")
                            .option("header",true)
                            .mode(SaveMode.Overwrite)
                            .save("/datascience/data_mediabrands/organic")

    }
    def main(args: Array[String]) {
        /// Configuracion spark
        val spark = SparkSession.builder.appName("Generate Organic Data").getOrCreate()
        
        /// Parseo de parametros
        val ndays = if (args.length > 0) args(0).toInt else 5

        generate_organic(spark,ndays)

    }
}
