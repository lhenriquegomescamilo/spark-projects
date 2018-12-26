package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{upper, col,abs,udf,regexp_replace,split}
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path

object keywordIngestion {
    def main(args: Array[String]) {
        /// Configuracion spark
        val spark = SparkSession.builder.appName("keyword ingestion").getOrCreate()
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)

        ////////////////////// ACTUAL EXECUTION ////////////////////////
        // Levantamos la data de las urls con sus keywords asociadas provenientes del scrapping
        val format = "yyyy-MM-dd"
        val start = DateTime.now.minusDays(10)
        val end   = DateTime.now.minusDays(0)

        val daysCount = Days.daysBetween(start, end).getDays()
        val days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))
        val today = DateTime.now().toString("yyyyMMdd")

        val dfs = (0 until daysCount).map(start.plusDays(_))
                                     .map(_.toString(format))
                                     .filter(day => fs.exists(new Path("/datascience/data_keyword_ingestion/%s.csv".format(day))))
                                     .map(x => spark.read.format("csv").load("/datascience/data_keyword_ingestion/%s.csv".format(x)))

        val df = dfs.reduce(_ union _).withColumnRenamed("_c0", "url")
                                      .withColumnRenamed("_c1", "content_keys")
                                      .withColumnRenamed("_c2", "count")
                                      .withColumnRenamed("_c3", "country")
                                      .drop("country")

        // Levantamos el dataframe que tiene toda la data de los usuarios con sus urls
        val udfFilter = udf((segments: Seq[String]) => segments.filter(token => !(token.matches("\\d*"))))

        val df_audiences = spark.read.parquet("/datascience/data_audiences_p/day=%s".format(today))
                                      .select("device_id","event_type","all_segments","url","device_type","country")
                                      .withColumn("url_keys", regexp_replace(col("url"), """https*://""", ""))
                                      .withColumn("url_keys", regexp_replace(col("url_keys"), """[/,=&\.\(\) \|]""", " , "))
                                      .withColumn("url_keys", regexp_replace(col("url_keys"), """%..""", " , "))
                                      .withColumn("url_keys", split(col("url_keys")," , "))
                                      .withColumn("url_keys", udfFilter(col("url_keys")))
        // Hacemos el join entre nuestra data y la data de las urls con keywords.
        val joint = df_audiences.join(broadcast(df),Seq("url"),"left_outer")
        // Guardamos la data en formato parquet
        joint.write.format("parquet").mode(SaveMode.Overwrite).save("/datascience/data_keywords_p/%s".format(today))

        // Armamos el archivo que se utilizara para ingestar en elastic
        val udfAs = udf((segments: Seq[String]) => segments.map(token => "as_"+token))
        val udfCountry = udf((country: String) =>  "c_"+country)
        val udfXp = udf((segments: Seq[String], et:String) => if (et == "xp") segments :+ "xp"
                                                                else segments)
        val udfJoin = udf((L: Seq[String]) => L.reduce((seg1, seg2) => seg1+","+seg2))

        val to_csv = joint.select("device_id","url_keys","content_keys","all_segments","event_type","country")
                          .withColumn("all_segments", udfAs(col("all_segments")))
                          .withColumn("all_segments",udfXp(col("all_segments"),col("event_type")))
                          .withColumn("country",udfCountry(col("country")))
                          .withColumn("all_segments",udfJoin(col("all_segments")))
                          .withColumn("url_keys",udfJoin(col("url_keys")))
                          .select("device_id","url_keys","content_keys","all_segments","country")

        to_csv.repartition(100)
              .write.mode(SaveMode.Overwrite)
              .format("csv")
              .option("sep", "\t")
              .save("/datascience/data_keywords_elastic/%s".format(today))
    }
  }
