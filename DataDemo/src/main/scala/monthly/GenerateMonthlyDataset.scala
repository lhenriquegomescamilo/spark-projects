package main.scala.monthly
import main.scala.datasets.{UrlUtils, DatasetGA, DatasetKeywordsURL, DatasetSegmentTriplets}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.log4j.{Level, Logger}


object GenerateMonthlyDataset{

    /**
   * This function constructs and returns a DataFrame where all the ground truth users are stored. It receives the 
   * path where the users are stored, reads the data, holds it in memory in a DataFrame and returns it.
   * 
   * @param path: path where the ground truth user ids are stored. This dataset has to have these three columns:
                      - _c0: device type (this column will not be used)
                      - _c1: device id
                      - _c2: segment id or label
   * 
   * @return: DataFrame with two columns: device_id and label.
  */
  def getGTDataFrame(spark: SparkSession, path: String): DataFrame = {
    // Now we load the ground truth users
    val users = spark.read
      .format("csv")
      .option("sep", "\t")
      .load(path)
      .withColumnRenamed("_c1", "device_id")
      .withColumnRenamed("_c2", "label")
      .select("device_id", "label")
      .distinct()
    users
  }

  def getExpansionData(spark: SparkSession, path: String, country: String, name:String, ndays:Int) = {
    // Loading the GT dataframe
    val gt = getGTDataFrame(spark,path)

    // Generating the GA data by joining de data from GA and the GT dataframe (left_anti)
    DatasetGA.getGARelatedData(spark, gt, country, "left_anti", name)
    
    // Loading the GA dataset previously generated
    val ga = spark.read
                      .load(
                          "/datascience/data_demo/name=%s/country=%s/ga_dataset_probabilities"
                          .format(name, country)
                      )
    
    // Generating the triplets dataset by joining the triplets with the GA dataset previously generated to mantain the same users
    DatasetSegmentTriplets.generateSegmentTriplets(spark, ga, country, "left", name, ndays)
    
    // Loading the triplets dataset previously generated
    val segments = spark.read
                        .load(
                          "/datascience/data_demo/name=%s/country=%s/segment_triplets"
                            .format(name, country)
                        )

    // Finally we get the Url dataset (device_id, [url1;url2]) from the users that passed the join with the previous dataset
    DatasetKeywordsURL.getDatasetFromURLs(spark, segments, country, "left", name, ndays)
  }

  def getTrainingData(spark: SparkSession, path: String, country: String, name:String, ndays:Int) = {
    // Loading the GT dataframe
    val gt = getGTDataFrame(spark,path)
    gt.cache()
    
    // Generating the GA data by joining de data from GA and the GT dataframe (inner)
    DatasetGA.getGARelatedData(spark, gt, country, "inner", name)
    
    // Loading the GA dataset previously generated
    val ga = spark.read
                  .load(
                      "/datascience/data_demo/name=%s/country=%s/ga_dataset_probabilities"
                      .format(name, country)
                  )
    
    // Generating the GT dataframe (device_id, label) from the users that passed the inner join
    gt.join(ga,Seq("device_id"),"inner")
                .select("device_id", "label")
                .distinct()
                .orderBy(asc("device_id"))
                .write
                .mode(SaveMode.Overwrite)
                .format("parquet")
                .save(
                  "/datascience/data_demo/name=%s/country=%s/gt".format(name, country)
                )
    // Generating the triplets dataset by joining the triplets with the GA dataset previously generated to mantain the same users
    DatasetSegmentTriplets.generateSegmentTriplets(spark, ga, country, "left", name, ndays)
    
    // Loading the triplets dataset previously generated
    val segments = spark.read
                        .load(
                          "/datascience/data_demo/name=%s/country=%s/segment_triplets"
                            .format(name, country)
                        )

    // Finally we get the keywords dataset (device_id, [kw1;kw2]) from the users that passed the join with the previous dataset
    DatasetKeywordsURL.getDatasetFromURLs(spark, segments, country, "left", name, ndays)
  }

  def main(args: Array[String]) {
    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder
      .appName("Monthly Data Download")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .getOrCreate()

    // Parameters
    val ndays = if (args.length > 0) args(0).toInt else 30
    val current_month = DateTime.now().getMonthOfYear.toString

    // AR GENERO
    var training_name = "training_AR_genero_%s".format(current_month)
    var expansion_name = "expansion_AR_genero_%s".format(current_month)
    var country = "AR"
    var path_gt = "/datascience/devicer/processed/AR_genero_%s_grouped".format(current_month)
    println("Generating Training AR Genero ...")
    //getTrainingData(spark, path_gt, country, training_name, ndays)
    println("Generating Expansion AR Genero ...")
    //getExpansionData(spark, path_gt, country, expansion_name, ndays)

    // AR EDAD
    training_name = "training_AR_edad_%s".format(current_month)
    expansion_name = "expansion_AR_edad_%s".format(current_month)
    country = "AR"
    path_gt = "/datascience/devicer/processed/AR_edad_%s_grouped".format(current_month)
    println("Generating Training AR Edad ...")
    //getTrainingData(spark, path_gt, country, training_name, ndays)
    println("Generating Expansion AR Edad ...")
    //getExpansionData(spark, path_gt, country, expansion_name, ndays)

    // MX GENERO
    training_name = "training_MX_genero_%s".format(current_month, ndays)
    expansion_name = "expansion_MX_genero_%s".format(current_month, ndays)
    country = "MX"
    path_gt = "/datascience/devicer/processed/MX_genero_%s_grouped".format(current_month)
    println("Generating Training MX Genero ...")
    //getTrainingData(spark, path_gt, country, training_name, ndays)
    println("Generating Expansion MX Genero ...")
    getExpansionData(spark, path_gt, country, expansion_name, ndays)

    // MX EDAD
    // training_name = "training_MX_edad_%s".format(current_month)
    // expansion_name = "expansion_MX_edad_%s".format(current_month)
    // country = "MX"
    // path_gt = "/datascience/devicer/processed/MX_edad_%s_grouped".format(current_month)
    // println("Generating Training MX Edad ...")
    // getTrainingData(spark, path_gt, country, training_name, ndays)
    // println("Generating Expansion MX Edad ...")
    // getExpansionData(spark, path_gt, country, expansion_name, ndays)
  }
}
