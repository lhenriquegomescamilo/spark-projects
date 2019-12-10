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
    *              PEDIDO segmentos para audiencia
    *
    *
    *
    *
    *
    */
  def getDataSegmentsForAudience(spark: SparkSession) = {
    val data_audiences = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load(
        "/datascience/geo/crossdeviced/jcdecaux_test_4_points_120d_mexico_28-11-2019-10h_xd"
      )

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
      3599, 3600, 3779, 3782, 3913, 3914, 3915, 4097, 104014, 104015, 104016,
      104017, 104018, 104019)

    val data_segments = getDataTriplets(spark, country = "MX", nDays = 60)
      .filter(col("segment").isin(taxonomy: _*))
      .select("device_id", "segment")

    data_audiences
      .join(data_segments, Seq("device_id"))
      .write
      .format("csv")
      .mode("overwrite")
      .save("/datascience/custom/jcdecaux_with_segments")

  }

  def getAllDataSegmentsForAudience(spark: SparkSession) = {
    val taxonomy = Seq(182695, 182697, 182699, 182701, 182703, 182705, 182707,
      182709, 182711, 182713, 182715, 182717, 182719, 182721, 182723, 182725,
      182727, 182729, 182731, 182733, 182735, 182737, 182739, 182741, 182743,
      182745, 182747, 182749, 182751, 182753, 182755, 182695, 182759, 182761,
      182763, 182765, 182767, 182769, 182771, 182773, 182775, 182777, 182779,
      182781, 182783, 182785, 182787, 182789, 182791, 182793, 182795, 182797,
      182799, 182801, 182803, 182805, 182807, 182809, 182811, 182813, 182815,
      182817, 182819, 182821, 182823, 182825, 182827, 182829, 182831, 182833,
      182835, 182837, 182839, 182841, 182843, 182845, 182849, 182851, 182853,
      182855, 182857, 182859, 182861, 182863, 182865, 182867, 182869, 182871,
      182873, 182875, 182877, 182879, 182881, 182883, 182885, 182887, 182889,
      182891, 182893, 182895, 182897, 182899, 182901, 182903, 182905, 182907,
      182909, 182911, 182913, 182915, 182917, 182919, 182921, 182923, 182925,
      182927, 182929, 182931, 182933, 182935, 182937, 182939, 182941, 182943,
      182945, 182947, 182949, 182951, 182953, 182955, 182957, 182959, 182961,
      182963, 182965, 182967, 182969, 182971, 182973, 182975, 182977, 182979,
      182981, 182983, 182985, 182987, 182989, 182991, 182993, 182995, 182997,
      182999, 183001, 183003, 183005, 183007, 183009, 183011, 183013, 183015,
      183017, 183019, 183021, 183023, 183025, 183027, 183029, 183031, 183033,
      183035, 183037, 183039, 183041, 183043, 183045, 183047, 183049, 183051,
      183053, 183055, 183057, 183059, 183061, 183063, 183065, 183067, 183069,
      183071, 183073, 183075, 183077, 183079, 183081, 183083, 183085, 183087,
      183089, 183091, 183093, 183095, 183097, 183099, 183101, 183103, 183105,
      183107, 183109, 183111, 183113, 183115, 183117, 183119, 183121, 183123,
      183125, 183127, 183129, 183131, 183133, 183135, 183137, 183139, 183141,
      183143, 183145, 183147, 183149, 183151, 183153, 183155, 183157, 183159,
      185069, 185071, 185073, 185075, 185077, 185079, 185081, 185083, 185085,
      185087, 185089, 185091, 185093, 183161, 183163, 183165, 183167, 183169,
      183171, 183173, 183175, 183177, 183179, 183181, 183183, 183185, 183187,
      183189, 183191, 183193, 183195, 183197, 183199, 183201, 183203, 183205,
      183207, 183209, 183211, 183213, 183215, 183217, 183219, 183221, 183223,
      183225, 183227, 183229, 183231, 183233, 183235, 183237, 183239, 183241,
      183243, 183245, 183247, 183249, 183251, 183253, 183255, 183257, 183259,
      183261, 183263, 183265, 183267, 183269, 183271, 183273, 183275, 183277,
      183279, 183281, 183283, 183285, 185095, 185097, 185099, 185101, 185103,
      185105, 185107, 185109, 185111, 185113, 185115, 185117, 185119, 185121,
      185123, 185125, 185127, 185129, 185131, 185133, 185135, 185137, 185139,
      185141, 185143, 185145, 185147, 185149, 185151, 185153, 185155, 185157,
      185159, 185161, 185163, 185165, 185167, 185169, 185171, 185173, 185175,
      185177, 185179, 185181, 185183, 185185, 185187, 185189, 185191, 183287,
      183289, 183291, 183293, 183295, 183297, 183299, 183301, 183303, 183305,
      183307, 183309, 183311, 183313, 183315, 183317, 183319, 183321, 183323,
      183325, 183327, 183329, 183331, 183333, 183335, 183337, 183339, 183341,
      183343, 183345, 183347, 183349, 183351, 183353, 183355, 183357, 183359,
      183361, 183363, 183365, 183367, 183369, 183371, 183373, 183375, 183377,
      183379, 183381, 183383, 183385, 183387, 183389, 183391, 183393, 183395,
      183397, 183399, 183401, 183403, 183405, 183407, 183409, 183411, 183413,
      183415, 183417, 183419, 183421, 183423, 183425, 183427, 183429, 183431,
      183433, 183435, 183437, 183439, 183441, 183443, 183445, 183447, 183449,
      183451, 183453, 183455, 183457, 183459, 183461, 183463, 183465, 183467,
      183469, 183471, 183473, 183475, 183477, 183479, 183481, 183483, 183485,
      183487, 183489, 183491, 183493, 183495, 183497, 183499, 183501, 183503,
      183505, 183507, 183509, 183511, 183513, 183515, 183517, 183519, 183521,
      183523, 183525, 183527, 183529, 183531, 183533, 183535, 183537, 183539,
      183541, 183543, 183545, 183547, 183549, 183551, 183553, 183555, 183557,
      183559, 183561, 183563, 183565, 183567, 183569, 183571, 183573, 183575,
      183577, 183579, 183581, 183583, 183585, 183587, 183589, 183591, 183593,
      183595, 183597, 183599)

    val data_audiences = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "false")
      .load(
        "/datascience/custom/gt_br_transunion_gender"
      )
      .withColumnRenamed("_c1", "device_id")
      .withColumnRenamed("_c2", "ids")
      .drop("_c0")

    val data_segments = getDataTriplets(spark, country = "BR", nDays = 40)
      .filter(col("segment").isin(taxonomy: _*))
      .select("device_id", "segment")

    data_audiences
      .join(data_segments, Seq("device_id"))
      .write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save("/datascience/custom/gt_br_transunion_gender_startapp")

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

    getAllDataSegmentsForAudience(spark = spark)

  }
}
