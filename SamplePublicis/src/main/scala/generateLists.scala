package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode,desc,lit,size,concat,col,concat_ws,collect_list}
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Column



object generateLists {
    def stringify(c: Column) = concat(lit("["), concat_ws(",", c), lit("]"))

    def main(args: Array[String]) {
        /// Configuracion spark
        val spark = SparkSession.builder.appName("keyword ingestion").getOrCreate()
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)

        ////////////////////// ACTUAL EXECUTION ////////////////////////

        /// Este job se encarga de armar un dataframe con < dev_id, [s1:ts1, s2:ts2, ...], [g1:ts1, g2:ts2, ...] > donde los s son segmentos de la taxo general
        /// y los g son segmentos geo.

        val triplets = spark.read.format("csv").option("header","True")
                                              .load("/datascience/data_publicis_organica_triplets")

        val taxo_general = spark.read.format("csv").option("sep","\t")
                                                    .option("header","True")
                                                    .load("/datascience/taxo_general.csv")
                                                    .withColumnRenamed("Segment ID","segments")
        val taxo_geo = spark.read.format("csv").option("header","True")
                                              .load("/datascience/taxo_geo.csv")
                                              .withColumnRenamed("Segment ID","segments")

        val join_geo = triplets.join(broadcast(taxo_geo), Seq("segments"))
                                .withColumn("geo_segments",concat(col("segments"),lit(":"),col("day")))
                                .groupBy("device_id")
                                .agg(collect_list("geo_segments"))

        val join_general = triplets.join(broadcast(taxo_general), Seq("segments"))
                                    .withColumn("general_segments",concat(col("segments"),lit(":"),col("day")))
                                    .groupBy("device_id")
                                    .agg(collect_list("general_segments"))

        /// Guardo ambos join en disco para que despues spark haga un hashJoin en lugar de un broadcast join (Son dataframes muy grandes)
        join_general.withColumnRenamed("collect_list(general_segments)","general_segments")
                    .withColumn("general_segments", stringify(col("general_segments")))
                    .write.format("csv").option("header","True").mode(SaveMode.Overwrite).save("/datascience/data_publicis_organica_join_general")
        join_geo.withColumnRenamed("collect_list(geo_segments)","geo_segments")
                    .withColumn("geo_segments", stringify(col("geo_segments")))
                    .write.format("csv").option("header","True").mode(SaveMode.Overwrite).save("/datascience/data_publicis_organica_join_geo")


        val join_final = join_general.join(join_geo,Seq("device_id"),"left_outer")
                                      .withColumnRenamed("collect_list(general_segments)","general_segments")
                                      .withColumnRenamed("collect_list(geo_segments)","geo_segments")
                                      .withColumn("geo_segments", concat_ws(",", col("geo_segments")))
                                      .withColumn("general_segments", concat_ws(",", col("general_segments")))

        join_final.write.format("csv").option("header","True").mode(SaveMode.Overwrite).save("/datascience/data_publicis_organica_join_final")

    }
  }
