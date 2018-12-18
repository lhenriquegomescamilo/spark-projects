import org.apache.spark.sql.SparkSession
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path

object keywordIngestion {
    def main(args: Array[String]) {
        /// Configuracion spark
        val spark = SparkSession.builder.appName("keyword ingestion").getOrCreate()
        val conf = sc.hadoopConfiguration
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)

        ////////////////////// ACTUAL EXECUTION ////////////////////////
        // Levantamos la data de las urls con sus keywords asociadas provenientes del scrapping
        val format = "yyyy-MM-dd"
        val start = DateTime.now.minusDays(10)
        val end   = DateTime.now.minusDays(0)

        val daysCount = Days.daysBetween(start, end).getDays()
        val days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

        val dfs = (0 until daysCount).map(start.plusDays(_))
                                     .map(_.toString(format))
                                     .filter(day => fs.exists(new Path("/datascience/keyword_ingestion/%s.csv".format(day))))
                                     .map(x => spark.read.format("csv").load("/datascience/keyword_ingestion/%s.csv".format(x)))

        val df = dfs.reduce(_ union _).withColumnRenamed("_c0", "url")
                                      .withColumnRenamed("_c1", "content_keys")
                                      .withColumnRenamed("_c2", "count")
                                      .withColumnRenamed("_c3", "country")

        // Levantamos el dataframe que tiene toda la data de los usuarios con sus urls
        val udfFilter = udf((segments: Seq[String]) => segments.filter(token => !(token.matches("\\d*"))))

        val today = DateTime.now().toString("yyyyMMdd")

        val df_audiences = spark.read.parquet("/datascience/data_audiences_p/day=%s".format(today))
                                      .select("device_id","event_type","all_segments","url","device_type","country")
                                      .withColumn("url_keys", regexp_replace($"url", """https*://""", ""))
                                      .withColumn("url_keys", regexp_replace($"url_keys", """[/,=&\.\(\) \|]""", " , "))
                                      .withColumn("url_keys", regexp_replace($"url_keys", """%..""", " , "))
                                      .withColumn("url_keys", split($"url_keys"," , "))
                                      .withColumn("url_keys", udfFilter($"url_keys"))
        // Hacemos el join entre nuestra data y la data de las urls con keywords.
        val joint = df_audiences.join(broadcast(df),Seq("url"),"left_outer")
        // Guardamos la data en formato parquet
        joint.write.format("parquet").mode(SaveMode.Overwrite).save("/datascience/keyword_ingestion_p/%s".format(today))
