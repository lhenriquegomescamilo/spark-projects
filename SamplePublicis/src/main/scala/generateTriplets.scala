package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode,desc,lit,size,concat,col,concat_ws,collect_list}
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path

object generateTriplets {
    def main(args: Array[String]) {
        /// Configuracion spark
        val spark = SparkSession.builder.appName("keyword ingestion").getOrCreate()
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)

        ////////////////////// ACTUAL EXECUTION ////////////////////////

        /// Este job se encarga de armar triplets de la pinta < dev_id, day, segment > ordenados por fecha y sin duplicados (segment y device_id
        val format = "yyyyMMdd"
        val start = DateTime.now.minusDays(30)
        val end   = DateTime.now.minusDays(0)

        val daysCount = Days.daysBetween(start, end).getDays()
        val days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

        val dfs = (0 until daysCount).map(start.plusDays(_))
                                      .map(_.toString(format)).reverse
                                      .filter(day => fs.exists(new org.apache.hadoop.fs.Path("/datascience/data_audiences_p/day=%s".format(day))))
                                      .map(x => spark.read.parquet("/datascience/data_audiences_p/day=%s".format(x))
                                          .filter(col("country")==="MX")
                                          .withColumn("day",lit(x))
                                          .select("device_id","day","segments"))

        val df = dfs.reduce((df1,df2) => df1.union(df2))

        val triplets = df.withColumn("segments",explode(col("segments"))).sort(desc("day")).dropDuplicates(Seq("device_id", "segments"))

        triplets.write.format("csv").mode(SaveMode.Overwrite).save("/datascience/data_publicis_organica_triplets")

    }
  }
