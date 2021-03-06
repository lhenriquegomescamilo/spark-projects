package main.scala.datasets
import main.scala.datasets.DatasetTimestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}

object DatasetGA{
/**
  * This method calculates all the datasets for the AR ground truth users, based only on Google Analytics (GA) data.
  * It generates three datasets, each of which are sorted by device id:
        1. ga_url_domains: this dataset has two columns: device id and url domains. The URL domains is a list of 
        URL domains separated by ';'.
        2. ga_dataset_probabilities: this dataset contains a device id along with a probability associated for every 
        age category and gender category. These are the columns generated: device_id, MALE_PROB, FEMALE_PROB, AGE18_PROB, 
        AGE25_PROB, AGE35_PROB, AGE45_PROB, AGE55_PROB, AGE65_PROB.            .select("device_id", "segments", "country",)

        3. ga_timestamp: It contains the data related to the timestamps. It contains 48 columns where each column has the
        following format: [hour][0 or 1]. The hour is the hour of the day extracted from the timestamp, and 0 if it is a
        weekday and 1 if it is a weekend.
  * 
  * @param spark: Spark session object that will be used to load the data.
  * @param gtDF: dataframe where the ground truth users are stored. This dataframe must have a column called 'device_id' 
  * and another called 'label'.
  * @param country: country for which the Google analytics data has to be downloaded.
  * @param joinType: type of join that will be performed. It can be either 'inner' or 'left' or 'left_anti'.
  * @param name: name for the folder where the dataset will be stored.
  */
  
  def getGARelatedData(
      spark: SparkSession,
      gtDF: DataFrame,
      country: String,
      joinType: String,
      name: String,
      format_type: String
  ) = {
    // First we load the GA data from the last 60 days
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(1)

    val days = (0 until 60).map(start.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_demo/google_analytics_domain/"
    val dfs = days.map(day => path + "day=%s/".format(day) + "country=%s".format(country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      .map(
        x =>
          spark.read
            .option("basePath", "/datascience/data_demo/google_analytics_domain/")
            .parquet(x)
            .withColumn("day",lit(x.split("/")(4).split("=").last))
      )

    var ga = dfs.reduce((df1, df2) => df1.union(df2)).dropDuplicates("url", "device_id")

    // Here we filter the users from the last 30 days if we are calculating the expansion set. We get the
    // users from GA data
    if (joinType == "left_anti"){
      val init_day = DateTime.now.minusDays(30).toString("yyyyMMdd")

      ga = ga.filter("day > %s".format(init_day))
    }

    // Here we calculate the data of GA just for the users that do not have ground truth data.
    val joint = ga.join(gtDF, Seq("device_id"), joinType).na.fill(0)

    joint.cache()

    // First of all we store the URLs, separated by ';', for every user.
    joint
      .select("device_id", "url")
      .groupBy("device_id")
      .agg(collect_list(col("url")).as("url"))
      .withColumn("url", concat_ws(";", col("url")))
      .orderBy(asc("device_id"))
      .write
      .format(format_type)
      .mode(SaveMode.Overwrite)
      .save(
        "/datascience/data_demo/name=%s/country=%s/ga_url_domains"
          .format(name, country)
      )

    // In this part we calculate the probabilities for every user.

    joint.registerTempTable("ga")

    val probabilities = spark.sql(
      """SELECT device_id,
                LOG(MALE/total_genero) as MALE_PROB, 
                LOG(FEMALE/total_genero) as FEMALE_PROB,
                LOG(AGE18/total_age) as AGE18_PROB,
                LOG(AGE25/total_age) as AGE25_PROB,
                LOG(AGE35/total_age) as AGE35_PROB,
                LOG(AGE45/total_age) as AGE45_PROB,
                LOG(AGE55/total_age) as AGE55_PROB,
                LOG(AGE65/total_age) as AGE65_PROB
                FROM (SELECT  device_id,
                              IF(MALE is null, 100, MALE+100) as MALE,
                              IF(FEMALE is null, 100, FEMALE+100) as FEMALE,
                              IF(MALE is null, 0, MALE) + IF(FEMALE is null, 0, FEMALE) + 200.0 as total_genero,
                              IF(AGE18 is null, 20, AGE18+20) as AGE18,
                              IF(AGE25 is null, 20, AGE25+20) as AGE25,
                              IF(AGE35 is null, 20, AGE35+20) as AGE35,
                              IF(AGE45 is null, 20, AGE45+20) as AGE45,
                              IF(AGE55 is null, 20, AGE55+20) as AGE55,
                              IF(AGE65 is null, 20, AGE65+20) as AGE65,
                                  IF(AGE18 is null, 0, AGE18) + IF(AGE25 is null, 0, AGE25) + 
                                  IF(AGE35 is null, 0, AGE35) + IF(AGE45 is null, 0, AGE45) +
                                  IF(AGE55 is null, 0, AGE55) + IF(AGE65 is null, 0, AGE65) + 120.0 as total_age
                      FROM ga)"""
    )

    val probabilities_calculated = probabilities
                                    .groupBy("device_id")
                                    .agg(
                                        sum(col("FEMALE_PROB")).as("FEMALE_PROB"),
                                        sum(col("MALE_PROB")).as("MALE_PROB"),
                                        sum(col("AGE18_PROB")).as("AGE18_PROB"),
                                        sum(col("AGE25_PROB")).as("AGE25_PROB"),
                                        sum(col("AGE35_PROB")).as("AGE35_PROB"),
                                        sum(col("AGE45_PROB")).as("AGE45_PROB"),
                                        sum(col("AGE55_PROB")).as("AGE55_PROB"),
                                        sum(col("AGE65_PROB")).as("AGE65_PROB")
                                    )
                                    .withColumn("FEMALE_PROB", exp(col("FEMALE_PROB")))
                                    .withColumn("MALE_PROB", exp(col("MALE_PROB")))
                                    .withColumn("AGE18_PROB", exp(col("AGE18_PROB")))
                                    .withColumn("AGE25_PROB", exp(col("AGE25_PROB")))
                                    .withColumn("AGE35_PROB", exp(col("AGE35_PROB")))
                                    .withColumn("AGE45_PROB", exp(col("AGE45_PROB")))
                                    .withColumn("AGE55_PROB", exp(col("AGE55_PROB")))
                                    .withColumn("AGE65_PROB", exp(col("AGE65_PROB")))
                                    .withColumn("TOTAL_GENDER", col("MALE_PROB") + col("FEMALE_PROB"))
                                    .withColumn("TOTAL_AGE", col("AGE18_PROB") + col("AGE25_PROB") + col("AGE35_PROB") +
                                                            col("AGE45_PROB") + col("AGE55_PROB") + col("AGE65_PROB"))
                                    .withColumn("FEMALE_PROB", col("FEMALE_PROB") / col("TOTAL_GENDER"))
                                    .withColumn("MALE_PROB", col("MALE_PROB") / col("TOTAL_GENDER"))
                                    .withColumn("AGE18_PROB", col("AGE18_PROB") / col("TOTAL_AGE"))
                                    .withColumn("AGE25_PROB", col("AGE25_PROB") / col("TOTAL_AGE"))
                                    .withColumn("AGE35_PROB", col("AGE35_PROB") / col("TOTAL_AGE"))
                                    .withColumn("AGE45_PROB", col("AGE45_PROB") / col("TOTAL_AGE"))
                                    .withColumn("AGE55_PROB", col("AGE55_PROB") / col("TOTAL_AGE"))
                                    .withColumn("AGE65_PROB", col("AGE65_PROB") / col("TOTAL_AGE"))
                                    .drop("TOTAL_GENDER","TOTAL_AGE")
                                    .orderBy(asc("device_id"))
    probabilities_calculated.write
                            .format(format_type)
                            .mode(SaveMode.Overwrite)
                            .save(
                                "/datascience/data_demo/name=%s/country=%s/ga_dataset_probabilities"
                                .format(name, country)
                            )

    // Finally we obtain the data the is related to timestamps coming from GA
    DatasetTimestamp.getDatasetTimestamp(spark,joint,name,country,format_type)

  }

  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder
      .appName("Generate Data urls")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .getOrCreate()

    val ndays = if (args.length > 0) args(0).toInt else 30
    val since = if (args.length > 1) args(1).toInt else 1


  }
}
