package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{upper, col,abs,udf,regexp_replace,split}
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path



object keywordIngestionMain {
    def main(args: Array[String]) {
      val spark = SparkSession.builder.appName("keyword ingestion").getOrCreate()
      val conf = spark.sparkContext.hadoopConfiguration
      val fs = org.apache.hadoop.fs.FileSystem.get(conf)

      ////////////////////// ACTUAL EXECUTION ////////////////////////
      // First we parse the parameters
      val query = if (args.length > 0) args(0) else "Error"
      val name = if (args.length > 1) args(1) else "Error"

      // Levantamos la data de las urls con sus keywords asociadas provenientes del scrapping
      // usando 10 dias para atras.
      val format = "yyyy-MM-dd"
      val start = DateTime.now.minusDays(10)
      val end   = DateTime.now.minusDays(1)

      val daysCount = Days.daysBetween(start, end).getDays()
      val days = (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))
      val today = DateTime.now().toString("yyyyMMdd")

      val dfs = (0 until daysCount).map(start.plusDays(_))
                                   .map(_.toString(format))
                                   .filter(day => fs.exists(new Path("/datascience/data_keywords_p/%s.parquet".format(day))))
                                   .map(x => spark.read.format("parquet").load("/datascience/data_keywords_p/%s.parquet".format(x)))

      val df = dfs.reduce(_ union _).withColumnRenamed("_c0", "url")
                                    .withColumnRenamed("_c1", "content_keys")
                                    .withColumnRenamed("_c2", "count")
                                    .withColumnRenamed("_c3", "country")
                                    .drop("country")

      df.createOrReplaceTempView("data")

      val sqlDF = spark.sql(query)
      sqlDF.write.format("csv").mode(SaveMode.Overwrite)
                  .save("/datascience/%s.csv".format(name))

    }
  }
