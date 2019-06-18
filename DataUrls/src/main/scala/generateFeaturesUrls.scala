package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{
  upper,
  col,
  abs,
  udf,
  regexp_replace,
  split,
  lit,
  explode,
  length,
  to_timestamp,
  from_unixtime,
  date_format,
  sum,
  count
}
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}

object GenerateFeaturesUrls {

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
    

    def get_datasets_gt(spark: SparkSession, ndays: int, since: int){
        
        val interest_filter = "country = 'AR' and ( array_contains(segments, '36') or array_contains(segments, '59') or array_contains(segments, '61') or array_contains(segments, '129') or array_contains(segments, '144') or array_contains(segments, '145') or array_contains(segments, '165') or array_contains(segments, '224') or array_contains(segments, '247') )"

        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(conf)

        // Get the days to be loaded
        val format = "yyyyMMdd"
        val end = DateTime.now.minusDays(since)
        val days = (0 until ndays).map(end.minusDays(_)).map(_.toString(format))
        val path = "/datascience/data_audiences"

        // Now we obtain the list of hdfs folders to be read
        val hdfs_files = days
            .map(day => path + "/day=%s".format(day))
            .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
        val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

        val df_interest = df.filter(interest_filter)
                            .select("url")
                            .withColumn("label",lit(0))


        val df_intent = df.filter("(country = 'AR' and (array_contains(segments, '352')))")
                        .select("url")
                        .withColumn("label",lit(1))

        df_interest.unionAll(df_intent)
                    .write
                    .format("csv")
                    .option("header","true")
                    .mode(SaveMode.Overwrite)
                    .save("/datascience/data_urls/gt")

    
    }
    
    def get_dataset_timestamps(spark: SparkSession, ndays: Int, since: Int, name: String, country: String){
        val myUDF = udf(
                        (weekday: String, hour: String) =>
                            if (weekday == "Sunday" || weekday == "Saturday") "%s1".format(hour)
                            else "%s0".format(hour)
                        )

        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(conf)

        // Get the days to be loaded
        val format = "yyyyMMdd"
        val end = DateTime.now.minusDays(since)
        val days = (0 until ndays).map(end.minusDays(_)).map(_.toString(format))
        val path = "/datascience/data_audiences"

        // Now we obtain the list of hdfs folders to be read
        val hdfs_files = days
            .map(day => path + "/day=%s".format(day))
            .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
        val df = spark.read.option("basePath", path).parquet(hdfs_files: _*).select("url","timestamp")

        df.withColumn("Time", to_timestamp(from_unixtime(col("timestamp") - (if (country=="AR") 3 else 5) * 3600))) // AR time transformation
            .withColumn("Hour", date_format(col("Time"), "HH"))
            .withColumn("Weekday", date_format(col("Time"), "EEEE"))
            .withColumn("wd", myUDF(col("Weekday"), col("Hour")))
            .groupBy("url", "wd")
            .count()
            .groupBy("url")
            .pivot("wd")
            .agg(sum("count"))
            .write
            .format("csv")
            .option("header", "true")
            .option("sep", "\t")
            .mode(SaveMode.Overwrite)
            .save(
                "/datascience/data_urls/name=%s/country=%s/features_timestamp"
                .format(name, country)
            )
    }
    
    //def get_dataset_keywords(spark: SparkSession, ndays: Int){
    //}
    
    def get_dataset_devices(spark: SparkSession, ndays: Int, since: Int, name: String, country: String){
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(conf)

        // Get the days to be loaded
        val format = "yyyyMMdd"
        val end = DateTime.now.minusDays(since)
        val days = (0 until ndays).map(end.minusDays(_)).map(_.toString(format))
        val path = "/datascience/data_audiences"

        // Now we obtain the list of hdfs folders to be read
        val hdfs_files = days
            .map(day => path + "/day=%s".format(day))
            .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

        val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)

        df.groupBy("url","device_type")
            .agg(count("device_id").as("count"))
            .groupBy("url")
            .pivot("device_type")
            .agg(sum("count"))
            .na.fill(0)
            .write
            .format("csv")
            .option("header", "true")
            .option("sep", "\t")
            .mode(SaveMode.Overwrite)
            .save(
                "/datascience/data_urls/name=%s/country=%s/features_devices"
                .format(name, country)
            )
    }

    def get_datasets_training(spark: SparkSession){
        val features_timestamp = spark.read.format("csv")
                                    .option("header","true")
                                    .option("sep", "\t")
                                    .load("/datascience/data_urls/name=training_AR/country=AR/features_timestamp")

        val gt = spark.read.format("csv")
                        .option("header","true")
                        .load("/datascience/data_urls/gt")

        val features_devices = spark.read.format("csv")
                            .option("header","true")
                            .option("sep", "\t")
                            .load("/datascience/data_urls/name=training_AR/country=AR/features_devices")

        gt.join(features_timestamp,Seq("url")).na.fill(0)
                .write
                .format("csv")
                .option("header", "true")
                .option("sep", "\t")
                .mode(SaveMode.Overwrite)
                .save("/datascience/data_urls/name=training_AR/country=AR/dataset_timestamp")

        gt.join(features_devices,Seq("url")).na.fill(0)   
                .write
                .format("csv")
                .option("header", "true")
                .option("sep", "\t")
                .mode(SaveMode.Overwrite)
                .save("/datascience/data_urls/name=training_AR/country=AR/dataset_devices")

    }


    def main(args: Array[String]) {
        /// Configuracion spark
        val spark = SparkSession.builder.appName("Get Features: Url Classifier")
                                .getOrCreate()

        // Parseo de parametros
        val ndays = if (args.length > 0) args(0).toInt else 3
        val since = if (args.length > 1) args(1).toInt else 1
        val name = if (args.length > 2) args(2).toString else ""
        val country = if (args.length > 3) args(3).toString else ""
        
        get_datasets_gt(spark,ndays,since)
        //get_dataset_timestamps(spark, ndays, since, name, country)
        //get_dataset_devices(spark, ndays, since, name, country)
        get_datasets_training(spark)
    }
}
