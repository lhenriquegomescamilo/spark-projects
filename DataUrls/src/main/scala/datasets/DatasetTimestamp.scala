package main.scala.datasets
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}

object DatasetTimestamp {

  def get_url_timestamp(spark: SparkSession,ndays: Int,since: Int,country: String,gtDF: DataFrame,
                        joinType:String,df_urls: DataFrame, name:String): DataFrame =  {
    
    // First we get the data from urls (<url, time>)
    val data_urls = df_urls.select("url","time")
                                  
    // Join with the GT dataframe
    val joint = gtDF.join(data_urls,Seq("url"),joinType)
                    .select("url","time")
                    .withColumn("country",lit(country))
                    .dropDuplicates()
                    
    // Generate dataset with columns weekday and hour
    val myUDF = udf((weekday: String, hour: String) => if (weekday == "Sunday" || weekday == "Saturday") "weekend" else "week")

    val myUDFTime = udf((hour: String) =>   if (List("09","10","11","12","13").contains(hour)) "morning" 
                                            else if (List("14","15","16","17","18","19","20","21").contains(hour)) "afternoon"
                                                  else "night")

    val UDFFinal = udf((daytime: String, wd:String) => "%s_%s".format(daytime,wd))

    val df = joint.withColumn("Hour", date_format(col("time"), "HH"))
                  .withColumn("Weekday", date_format(col("time"), "EEEE"))
                  .withColumn("wd", myUDF(col("Weekday"), col("Hour")))
                  .withColumn("daytime", myUDFTime(col("Hour")))
                  .withColumn("feature",UDFFinal(col("daytime"),col("wd")))
                  .select("url","feature")

  
    df.groupBy("url","feature").count()
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .partitionBy("country")
      .save("/datascience/data_url_classifier/%s".format(name))
      
    df
  }

  def main(args: Array[String]) {
    // Spark configuration
    val spark = SparkSession.builder
      .appName("Data URLs: Dataset Timestamp")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .getOrCreate()

    // Parseo de parametros
    val ndays = if (args.length > 0) args(0).toInt else 10
    val since = if (args.length > 1) args(1).toInt else 1
    val country = if (args.length > 2) args(2).toString else ""
    val segments = List(129, 59, 61, 250, 396, 150, 26, 32, 247, 3013, 3017)

    //val data_urls = get_data_urls(spark, ndays, since, country)
    //val gtDF = spark.read.load("/datascience/data_url_classifier/dataset_referer/country=AR")
    //get_url_timestamp(spark, country = country, since = since, ndays = ndays, gtDF = gtDF, joinType = "inner", df_urls = data_urls)
  }
}
