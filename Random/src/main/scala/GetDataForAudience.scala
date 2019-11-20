package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.hadoop.conf.Configuration
import org.apache.spark.ml.classification.{
  RandomForestClassificationModel,
  RandomForestClassifier
}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.classification.{
  GBTClassificationModel,
  GBTClassifier
}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.Row

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object GetDataForAudience {

  /**
    * This method returns a DataFrame with the data from the audiences data pipeline, for the interval
    * of days specified. Basically, this method loads the given path as a base path, then it
    * also loads the every DataFrame for the days specified, and merges them as a single
    * DataFrame that will be returned.
    *
    * @param spark: Spark Session that will be used to load the data from HDFS.
    * @param nDays: number of days that will be read.
    * @param since: number of days ago from where the data is going to be read.
    *
    * @return a DataFrame with the information coming from the data read.
  **/
  def getDataAudiences(
      spark: SparkSession,
      nDays: Int = 30,
      since: Int = 1
  ): DataFrame = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Get the days to be loaded
    val format = "yyyyMMdd"
    val end = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))
    val path = "/datascience/data_audiences_streaming/"

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/hour=%s*".format(day))
    // .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    val df = spark.read.option("basePath", path).parquet(hdfs_files: _*)
    fs.close()

    df
  }

  def getDataTriplets(
      spark: SparkSession,
      country: String,
      nDays: Int = -1,
      path: String = "/datascience/data_triplets/segments/"
  ) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val df = if (nDays > 0) {
      // read files from dates
      val format = "yyyyMMdd"
      val endDate = DateTime.now.minusDays(1)
      val days =
        (0 until nDays.toInt).map(endDate.minusDays(_)).map(_.toString(format))
      // Now we obtain the list of hdfs folders to be read
      val hdfs_files = days
        .map(day => path + "/day=%s/country=%s".format(day, country))
        .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

      val dfs = hdfs_files.map(
        f =>
          spark.read
            .parquet(f)
            .select("device_id", "feature")
            .withColumn("count", lit(1))
            .withColumnRenamed("feature", "segment")
      )
      dfs.reduce((df1, df2) => df1.unionAll(df2))
    } else {
      // read all date files
      spark.read.load(path + "/day=*/country=%s/".format(country))
    }
    df
  }

  def getDataKeywords(
      spark: SparkSession,
      nDays: Int,
      since: Int
  ): DataFrame = {
    val end = DateTime.now.minusDays(since)

    val lista_files = (0 until nDays)
      .map(end.minusDays(_))
      .map(
        day =>
          "/datascience/data_keywords/day=%s"
            .format(day.toString("yyyyMMdd"))
      )

    val keywords = spark.read
      .format("parquet")
      .option("basePath", "/datascience/data_keywords/")
      .load(lista_files: _*)

    keywords
  }

  /**
    *
    *
    *
    *
    *        DATA DE ENCUESTAS DE LAS VOTACIONES
    *
    *
    *
    *
    */
  def getDataVotaciones(spark: SparkSession) = {
    val data_audience = getDataKeywords(spark, 40, 2).filter("country = 'AR'")
    // getDataAudiences(spark, nDays = 10, since = 1)
    //   .filter(
    //     "country = 'AR' and event_type IN ('tk', 'batch', 'data', 'pv')"
    //   )
    //   .select("device_id", "url", "time") //, "all_segments")
    val data_votaciones =
      spark.read
        .format("csv")
        .option("sep", ",")
        // .option("header", "true")
        .load("/datascience/custom/votacion_2019_impacted.csv")
        .withColumnRenamed("_c0", "device_id")
        .distinct()

    val joint = data_audience
      .join(data_votaciones, Seq("device_id"))
    // .withColumn("all_segments", concat_ws(",", col("all_segments")))

    joint.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/votaciones_2019_segments")
  }

  /**
    *
    *
    *
    *
    *        DATA DE ENCUESTAS DE LAS VOTACIONES
    *
    *
    *
    *
    */
  def getDataMaids(spark: SparkSession) = {
    val data_segments = getDataTriplets(spark, "MX")

    val maids =
      spark.read
        .format("csv")
        .option("sep", ",")
        .option("header", "true")
        .load("/datascience/misc/maids_mcdonalds.csv")
        .withColumnRenamed("maid", "device_id")

    val joint = data_segments
      .join(broadcast(maids), Seq("device_id"))

    joint.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/datascience/misc/maids_mcdonalds_segments")
  }

  /**
    *
    *
    *
    *
    *
    *            DATA PARA TEST DE LOOK-ALIKE
    *
    *
    *
    *
    *
    */
  def dfZipWithIndex(
      df: DataFrame,
      offset: Int = 1,
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

  def getDataLookAlike(spark: SparkSession) = {
    val segments_AR =
      List(76208, 98279, 87910, 76203, 75805, 87909, 76209, 76205,
        76286).toArray
    val segments_BR = List(148995, 162433, 148997).toArray
    val segments_CL = List(142083).toArray
    val segments_MX = List(157067).toArray

    val countries = Map(
      "AR" -> segments_AR,
      "BR" -> segments_BR,
      "CL" -> segments_CL,
      "MX" -> segments_MX
    )

    for ((c, segs) <- countries) {
      val triplets = getDataTriplets(spark, nDays = 60, country = c)
        .withColumn("country", lit(c))
        .filter(col("segment").isin(segs: _*))
      triplets.cache()

      for (s <- segs) {
        val bySeg = dfZipWithIndex(
          triplets
            .filter("segment = %s".format(s))
        )
        bySeg.cache()
        val count = bySeg.count()

        val train = bySeg.filter("id < %s".format(count * .7))
        val test = bySeg
          .filter("id >= %s".format(count * .7))
          .withColumn("segment", -col("segment"))

        train
          .unionAll(test)
          .drop("id")
          .withColumnRenamed("segment", "feature")
          .write
          .format("parquet")
          .mode("append")
          .save("/datascience/custom/lookalike_ids/")
        bySeg.unpersist()
      }
      triplets.unpersist()
    }

  }

  /**
    *
    *
    *            AMEX TIMESTAMPS AND URLS
    *
    *
    */
  def getDataAmexURL(spark: SparkSession) = {
    val data_audience =
      getDataAudiences(spark, nDays = 10, since = 1)
        .filter(
          "country = 'MX' and event_type IN ('tk', 'batch', 'data', 'pv')"
        )
        .select("device_id", "url", "time") //, "all_segments")
    val data_amex =
      spark.read
        .format("csv")
        .option("sep", "\t")
        .load("/datascience/custom/approvable_pgp_employed.csv")
        .withColumnRenamed("_c0", "device_id")
        .repartition(20)
    // .withColumnRenamed("_c1", "cluster")

    val joint = data_audience
      .join(data_amex, Seq("device_id"))
    // .withColumn("all_segments", concat_ws(",", col("all_segments")))

    joint.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/amex_con_data_all")
  }

  /**
    *
    *
    *
    *
    *
    *              PEDIDO HAVAS
    *
    *
    *
    *
    *
    */
  def getDataSegmentsHavas(spark: SparkSession) = {
    val data_audiences = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/datascience/custom/havas_411_no_overlap.csv")
      .withColumnRenamed("user", "device_id")
      .repartition(50)

    // val taxonomy = spark.read
    //   .format("csv")
    //   .option("header", "true")
    //   .load("/datascience/data_publicis/taxonomy_publicis.csv")
    //   .select("Segment Id")
    //   .collect()
    //   .map(row => row(0))
    //   .toSeq

    val taxonomy = Seq(2, 3, 4, 5, 6, 7, 8, 9, 26, 32, 36, 59, 61, 82, 85, 92,
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
      3599, 3600, 3779, 3782, 3913, 3914, 3915, 4097)

    val data_segments = getDataTriplets(spark, country = "MX", nDays = 30)
      .filter(col("segment").isin(taxonomy: _*))

    data_audiences
      .join(data_segments, Seq("device_id"))
      .select("device_id", "segment", "label")
      .write
      .format("csv")
      .mode("overwrite")
      .save("/datascience/custom/havas_411_no_overlap_segments")

  }

  /**
    *
    *
    *            AMEX SEGMENTS
    *
    *
    */
  def getDataAmexSegments(spark: SparkSession) = {
    val data_segments =
      spark.read.load("/datascience/data_triplets/segments")
    val data_amex =
      spark.read
        .format("csv")
        .option("sep", "\t")
        .load("/datascience/custom/approvable_pgp_employed.csv")
        .withColumnRenamed("_c0", "device_id")
        .repartition(20)
    // .withColumnRenamed("_c1", "cluster")

    val joint = data_segments
      .join(data_amex, Seq("device_id"))
    // .withColumn("all_segments", concat_ws(",", col("all_segments")))

    joint.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/custom/amex_con_data_segments")
  }

  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Spark devicer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    getDataSegmentsHavas(spark = spark)

  }
}
