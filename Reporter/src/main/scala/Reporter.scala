package main.scala

import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._

object Reporter {

  /**
    * This method returns a DataFrame with the data from the partner data pipeline, for the interval
    * of days specified. Basically, this method loads the given path as a base path, then it
    * also loads the every DataFrame for the days specified, and merges them as a single
    * DataFrame that will be returned.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param query: query to filter by id partner, date, and segments.
    *
    * @return a DataFrame with the information coming from the data read.
  **/
  def getDataset(
      spark: SparkSession,
      query: String
  ): DataFrame = {
    println("DEVICER LOG: PIPELINE ID PARTNERS")
    val path =
      "/datascience/data_partner_streaming/"

    val data = spark.read
      .load(path)
      .filter(query)

    // This is the list of columns to be allowed
    val columns =
      """device_id, id_partner, first_party, all_segments"""
        .replace("\n", "")
        .replace(" ", "")
        .split(",")

    val df =
      if (data.columns.length > 0)
        data
          .select(columns.head, columns.tail: _*)
      else
        spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(columns: _*))),
          StructType(columns.map(c => StructField(c, StringType, true)).toArray)
        )

    df
  }

  def getOverlap(
      spark: SparkSession,
      dataset: DataFrame,
      segments: Seq[Int]
  ): DataFrame = {
    // This function is used to add a ficticious segment that will serve
    // as the total per id partner
    val addTotalIdUDF = udf((segments: Seq[Int]) => segments :+ 0)

    // In this part we process the dataset so that we have all the segments per device,
    // the totals and filter to only keep the relevant segments
    val datasetWithSegments = dataset
      .withColumn("all_segments", addTotalIdUDF(col("all_segments")))
      .withColumn("segment", explode(col("all_segments")))
      .filter(col("segment").isin(segments: _*))

    // Now we group by segment and obtain the two relevant metrics: count and device_unique
    val grouped = datasetWithSegments
      .groupBy("segment")
      .agg(
        countDistinct(col("device_id")) as "device_unique",
        count("device_id") as "count"
      )

    grouped
  }

  //TODO agregar documentation
  def getQueryReport(spark: SparkSession, jsonContent: Map[String, String]) = {
    // TODO Agregar try y Catch
    // First of all we read all the parameters that are of interest
    val query = jsonContent("query")
    val segments = jsonContent("datasource").split(",").map(_.toInt).toSeq :+ 0
    val firstParty = jsonContent("segments")
    val split = jsonContent("split")

    // Then we obtain the dataset and the overlap
    val dataset = getDataset(spark, query)
    val overlap = getOverlap(spark, dataset, segments)

    // If there is a split, then we have to add the field firstParty as a column
    val report = if (split == "1") {
      overlap
        .withColumn("first_party", lit(firstParty))
        .select("first_party", "segment", "count", "device_unique")
    } else {
      overlap.select("segment", "count", "device_unique")
    }

    report.createOrReplaceTempView("report")
    val table = spark.table("report")

    table
      .repartition(1)
      .write
      .format("csv")
      .option("sep", ",")
      .mode("append")
      .save("/datascience/reporter/processed/test")
  }

  def main(args: Array[String]) {
    // Setting logger config
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("Spark devicer")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .getOrCreate()

    val testMap = Map(
      "query" -> "id_partner = 328 AND (array_contains(first_party, 8372) OR array_contains(first_party, 5019)) AND (hour >= 2019102300 AND hour <= 2019112123)",
      "datasource" -> List(2, 3, 4, 5, 6, 7, 8, 9, 26, 32, 36, 59, 61, 82, 85, 92,
      104, 118, 129, 131, 141, 144, 145, 147, 149, 150, 152, 154, 155, 158, 160,
      165, 166, 177, 178, 210, 213, 218, 224, 225, 226, 230, 245, 247, 250, 264,
      265, 270, 275, 276, 302, 305, 311, 313, 314, 315, 316, 317, 318, 322, 323,
      325, 326, 352, 353, 354, 356, 357, 358, 359, 363, 366, 367, 374, 377, 378,
      379, 380, 384, 385, 386, 389, 395, 396, 397, 398, 399, 401, 402, 403, 404,
      405, 409, 410, 411, 412, 413, 418, 420, 421, 422, 429, 430, 432, 433, 434,
      440, 441, 446, 447, 450, 451, 453, 454, 456, 457, 458, 459, 460, 462, 463,
      464, 465, 467, 895, 898, 899, 909, 912, 914, 915, 916, 917, 919, 920, 922,
      923, 928, 929, 930, 931, 932, 933, 934, 935, 937, 938, 939, 940, 942, 947,
      948, 949, 950, 951, 952, 953, 955, 956, 957, 1005, 1116, 1159, 1160, 1166,
      2623, 2635, 2636, 2660, 2719, 2720, 2721, 2722, 2723, 2724, 2725, 2726,
      2727, 2733, 2734, 2735, 2736, 2737, 2743, 3010, 3011, 3012, 3013, 3014,
      3015, 3016, 3017, 3018, 3019, 3020, 3021, 3022, 3023, 3024, 3025, 3026,
      3027, 3028, 3029, 3030, 3031, 3032, 3033, 3034, 3035, 3036, 3037, 3038,
      3039, 3040, 3041, 3055, 3076, 3077, 3084, 3085, 3086, 3087, 3302, 3303,
      3308, 3309, 3310, 3388, 3389, 3418, 3420, 3421, 3422, 3423, 3470, 3472,
      3473, 3564, 3565, 3566, 3567, 3568, 3569, 3570, 3571, 3572, 3573, 3574,
      3575, 3576, 3577, 3578, 3579, 3580, 3581, 3582, 3583, 3584, 3585, 3586,
      3587, 3588, 3589, 3590, 3591, 3592, 3593, 3594, 3595, 3596, 3597, 3598,
      3599, 3600, 3779, 3782, 3913, 3914, 3915, 4097).mkString(","),
      "split" -> "1",
      "segments" -> "8372"
    )

    getQueryReport(spark, testMap)
  }
}
