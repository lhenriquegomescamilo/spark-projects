package main.scala.features

import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.hadoop.conf.Configuration

/**
  * The idea of this script is to generate the datasets that will be used to
  * train and predict the demographic attributes. It uses data from URLs and User Agents.
  */
object TrainingFeatures {

  def generate_triplets(
      spark: SparkSession,
      gtPath: String,
      country: String
  ) = {
    // Load the ground truth data
    val gt = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "false")
      .load(gtPath)
      .withColumnRenamed("_c0", "device_type")
      .withColumnRenamed("_c1", "device_id")
      .withColumnRenamed("_c2", "label")

    // Get the last version of the dataset
    val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val dataset_path =
      "/datascience/data_demo/datasets/country=%s".format(country)
    val files = hdfs.listStatus(new Path(dataset_path))
    val last_dataset_path = files
      .map(_.getPath())
      .map(_.toString)
      .toList
      .sorted
      .last

    // Now we load the dataset
    val dataset = spark.read
      .format("parquet")
      .load(last_dataset_path)

    // Now we perform the join
    val dataset_gt = dataset
      .join(gt, Seq("device_id"), "inner")
      .withColumn("country", lit(country))
      .cache()

    // First we store the user-agent
    dataset_gt
      .select("device_id", "user_agent", "country")
      .distinct()
      .write
      .format("parquet")
      .partitionBy("country")
      .mode("overwrite")
      .save("/datascience/data_demo/training/user_agents")

    // Then we add the domain as a new URL for each user and store
    val withDomain = dataset_gt
      .withColumn("url", explode(col("urls")))
      .drop("urls")
      .withColumn(
        "domain",
        regexp_replace(col("url"), "http.*://(.\\.)*(www\\.){0,1}", "")
      )
      .withColumn(
        "url",
        regexp_replace(col("url"), "/$", "")
      )
      .withColumn(
        "url",
        lower(col("url"))
      )
      .withColumn(
        "domain",
        lower(col("domain"))
      )
      .withColumn("domain", regexp_replace(col("domain"), "/.*", ""))
      // add an identifier for each of the two columns
      .withColumn("domain", concat(lit("dom@"), col("domain")))
      .withColumn("url", concat(lit("url@"), col("url")))
      // merge the two columns into a single column named as 'url''
      .withColumn("url", array(col("url"), col("domain")))
      .withColumn("url", explode(col("url")))
      .select("device_id", "url", "country")
      .distinct()

    // Finally we save the data
    withDomain.write
      .format("parquet")
      .partitionBy("country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_demo/training/raw/")

    dataset_gt.unpersist()
  }

  /**
    * This function takes as input a dataframe and returns another dataframe
    * with an id associated to every row. This id is monotonically increasing
    * and with steps of 1 by 1.
    */
  def dfZipWithIndex(
      df: DataFrame,
      offset: Int = 0,
      colName: String = "id",
      inFront: Boolean = true
  ): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(
        ln =>
          Row.fromSeq(
            (if (inFront) Seq(ln._2 + offset) else Seq())
              ++ ln._1.toSeq ++
              (if (inFront) Seq() else Seq(ln._2 + offset))
          )
      ),
      StructType(
        (if (inFront) Array(StructField(colName, LongType, false))
         else Array[StructField]())
          ++ df.schema.fields ++
          (if (inFront) Array[StructField]()
           else Array(StructField(colName, LongType, false)))
      )
    )
  }

  /**
    * This function reads the raw data downloaded previously, and creates
    * two indexes: one for devices and the other one for URLs. Finally, it
    * generates a properly triplets-like dataset only with indexes.
    * In addition, it filters out those URLs that have less than the given
    * min_support users.
    */
  def get_indexes(
      spark: SparkSession,
      country: String,
      minSupport: Integer = 10
  ) = {
    // Load the raw data
    val raw_data = spark.read
      .format("parquet")
      .load("/datascience/data_demo/training/raw/country=%s".format(country))
      .withColumn("country", lit("country"))

    // Obtain an id for every URL. Here we filter out those URLs that have only one person visiting it.
    val windowUrl = Window.partitionBy("country").orderBy(col("url"))
    raw_data
      .groupBy("country", "url")
      .count()
      .filter("count >= %s".format(minSupport))
      .withColumn("url_idx", row_number().over(windowUrl))
      .withColumn("url_idx", col("url_idx") - 1) // Indexing to 0
      .select("url_idx", "url", "country")
      .write
      .format("parquet")
      .partitionBy("country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_demo/training/url_index/")
    val url_idx = spark.read
      .format("parquet")
      .load(
        "/datascience/data_demo/training/url_index/country=%s".format(country)
      )
      .withColumn("country", lit("country"))

    // Get the index for the devices
    val windowUser = Window.partitionBy("country").orderBy(col("device_id"))
    raw_data
      .join(url_idx.select("url", "country"), Seq("url", "country"))
      .select("country", "device_id")
      .distinct()
      .withColumn("device_idx", row_number().over(windowUser))
      .withColumn("device_idx", col("device_idx") - 1) // Indexing to 0
      .select("device_idx", "device_id", "country")
      .write
      .format("parquet")
      .partitionBy("country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_demo/training/device_index/")

    // Finally we use the indexes to store triplets partitioned by country
    val device_idx = spark.read
      .format("parquet")
      .load(
        "/datascience/data_demo/training/device_index/country=%s"
          .format(country)
      )
      .withColumn("country", lit("country"))

    raw_data
      .join(device_idx, Seq("device_id", "country"))
      .join(url_idx, Seq("url", "country"))
      .select("device_idx", "url_idx", "country")
      .write
      .format("parquet")
      .partitionBy("country")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_demo/training/indexed/")
  }

  type OptionMap = Map[Symbol, Any]

  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--from" :: value :: tail =>
        nextOption(map ++ Map('from -> value.toString), tail)
      case "--nDays" :: value :: tail =>
        nextOption(map ++ Map('nDays -> value.toString), tail)
      case "--gt_path" :: value :: tail =>
        nextOption(map ++ Map('gt_path -> value), tail)
      case "--country" :: value :: tail =>
        nextOption(map ++ Map('country -> value), tail)
      case "--support" :: value :: tail =>
        nextOption(map ++ Map('support -> value), tail)
    }
  }

  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val gt_path: String = options('gt_path).toString
    val country: String = options('country).toString
    val support: Integer =
      if (options.contains('support)) options('support).toString.toInt else 10

    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    generate_triplets(spark, gt_path, country)
    get_indexes(spark, country, support)
  }
}
