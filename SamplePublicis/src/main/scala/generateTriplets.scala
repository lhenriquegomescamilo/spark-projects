package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode,desc,lit,size,concat,col,concat_ws,collect_list,udf}
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
        val start = DateTime.now.minusDays(5)
        val end   = DateTime.now.minusDays(0)

        val daysCount = Days.daysBetween(start, end).getDays()
        val days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

        val dfs = (0 until daysCount).map(start.plusDays(_))
                                      .map(_.toString(format)).reverse
                                      .filter(day => fs.exists(new org.apache.hadoop.fs.Path("/datascience/data_audiences_p/day=%s".format(day))))
                                      .map(x => spark.read.parquet("/datascience/data_audiences_p/day=%s".format(x))
                                          .filter("country = 'MX' AND event_type <> 'xp'")
                                          .withColumn("day",lit(x))
                                          .select("device_id","day","segments"))

        val df = dfs.reduce((df1,df2) => df1.union(df2))
        /**
        val df = spark.read.format("parquet").load("/datascience/data_audiences_p/")
                                             .filter("day > %s AND country = 'MX' AND event_type <> 'xp'".format(start.toString(format)))
                                             .select("device_id","day","segments")
        **/
        val taxo_general = spark.read.format("csv").option("sep","\t")
                                                    .option("header","True")
                                                    .load("/datascience/taxo_general.csv")
                                                    .select("Segment ID")
                                                    .rdd.map(row=>row(0)).collect()

        val taxo_geo = spark.read.format("csv").option("header","True")
                                              .load("/datascience/taxo_geo.csv")
                                              .select("Segment ID")
                                              .rdd.map(row=>row(0)).collect()

        val sc = spark.sparkContext
        val taxo_general_b = sc.broadcast(taxo_general)
        val taxo_geo_b = sc.broadcast(taxo_geo)

        val udfGralSegments = udf((segments: Seq[String]) => segments.filter(segment => taxo_general_b.value.contains(segment)))
        val udfGeoSegments = udf((segments: Seq[String]) => segments.filter(segment => taxo_geo_b.value.contains(segment)))
        val udfAddDay = udf((segments: Seq[String], day: String) => segments.map(segment => (segment, day)))
        val udfFlattenLists = udf((listOfLists: Seq[Seq[Row]]) => listOfLists.flatMap(list => list)) 
        vval udfDropDuplicates = udf((segments: Seq[Row]) => segments.map(row => (row(0).asInstanceOf[String], row(1).asInstanceOf[String]))
                                                                     .groupBy(row => row._1)
                                                                     .map(row => row._2.sorted.last).toList
                                                                     .map(tuple => "%s:%s".format(tuple._1, tuple._2))
                                                                     .mkString(","))

        val userSegments = df.withColumn("gral_segments", udfGralSegments(col("segments")))
                             .withColumn("geo_segments", udfGeoSegments(col("segments")))
                             .withColumn("gral_segments", udfAddDay(col("gral_segments"), col("day")))
                             .withColumn("geo_segments", udfAddDay(col("geo_segments"), col("day")))
                             .groupBy("device_id")
                             .agg(collect_list("gral_segments") as "gral_segments",
                                  collect_list("geo_segments") as "geo_segments")
                             .withColumn("gral_segments", udfFlattenLists(col("gral_segments")))
                             .withColumn("geo_segments", udfFlattenLists(col("geo_segments")))
                             .withColumn("gral_segments", udfDropDuplicates(col("gral_segments")))
                             .withColumn("geo_segments", udfDropDuplicates(col("geo_segments")))
        //val triplets = df.withColumn("segments",explode(col("segments"))).sort(desc("day")).dropDuplicates(Seq("device_id", "segments"))

        userSegments.write.format("csv").option("sep", "\t").mode(SaveMode.Overwrite).save("/datascience/data_publicis/organic")

    }
}