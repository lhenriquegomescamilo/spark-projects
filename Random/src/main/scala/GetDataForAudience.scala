package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.hadoop.conf.Configuration

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object GetDataForAudience {

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
      .option("header", "false")
      .load(
        "/datascience/geo/reports/GCBA/carteles_GCBA_devices_type"
      )
      .withColumnRenamed("_c1", "device_id")
      .withColumnRenamed("_c2", "ids")
      .drop("_c0")

    val taxonomy = Seq(26, 32, 36, 59, 61, 82, 85, 92,
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

    val data_segments = getDataTriplets(spark, country = "AR", nDays = 60)
      .filter(col("segment").isin(taxonomy: _*))
      .select("device_id", "segment")

    data_audiences
      .join(data_segments, Seq("device_id"))
      .write
      .format("csv")
      .mode("overwrite")
      .save("/datascience/custom/carteles_GCBA_devices_type_segments")

  }

  def getAllDataSegmentsForAudience(spark: SparkSession) = {
    val data_audiences = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "false")
      .load(
        "/datascience/geo/reports/GCBA/carteles_GCBA_devices_type"
      )
      .withColumnRenamed("_c1", "device_id")
      .withColumnRenamed("_c2", "ids")
      .drop("_c0")

    val taxonomy = Seq(20107, 20108, 20109, 20110, 20111, 20112, 20113, 20114,
      20115, 20116, 20117, 20118, 20119, 20120, 20121, 20122, 20123, 20124,
      20125, 20126, 22474, 22476, 22478, 22480, 22482, 22484, 22486, 22488,
      22490, 22492, 22494, 22496, 22498, 22500, 22502, 22504, 22506, 22508,
      22510, 22512, 34316, 34317, 34318, 34319, 34320, 34321, 34322, 34323,
      34324, 34325, 34326, 34327, 34328, 34329, 34330, 34331, 34332, 34333,
      34334, 34335, 34336, 34337, 34338, 34339, 34340, 34341, 34342, 34343,
      34344, 34345, 34346, 34347, 34348, 34349, 34350, 34351, 34352, 34353,
      34354, 34355, 34356, 34357, 34358, 34359, 34360, 34361, 34362, 34363,
      34364, 34365, 34366, 34367, 34368, 34369, 34370, 34371, 34372, 34373,
      34374, 34375, 34376, 34377, 34378, 34379, 34380, 34381, 34382, 34383,
      34384, 34385, 34386, 34387, 34388, 34389, 34390, 34391, 34392, 34393,
      34394, 34395, 34396, 34397, 34398, 34399, 34400, 34401, 34402, 34403,
      34404, 34405, 34406, 34407, 34408, 34409, 34410, 34411, 34412, 34413,
      34414, 34415, 34416, 34417, 34418, 34419, 34420, 34540, 35360, 35361,
      35362, 35363, 35364, 35365, 35366, 35367, 35949, 35950, 35951, 35952,
      35953, 35954, 35955, 35956, 35957, 35958, 35959, 35960, 35961, 35962,
      35963, 35964, 35965, 35966, 35967, 35968, 35969, 35970, 35971, 35972,
      35973, 35974, 35975, 35976, 35977, 35978, 35979, 35980, 35981, 35982,
      35983, 35984, 35985, 35986, 35987, 35988, 35989, 35990, 35991, 35992,
      35993, 35994, 35995, 35996, 35997, 35998, 35999, 36000, 36001, 36002,
      36003, 36004, 36005, 36006, 36007, 36008, 36009, 36010, 36011, 36012,
      36013, 36014, 36015, 36016, 36017, 36018, 36019, 36020, 36021, 36022,
      36023, 36024, 36025, 36026, 36027, 36028, 36029, 36030, 36031, 36032,
      36033, 36034, 36035, 36036, 36037, 36038, 36039, 36040, 36041, 36042,
      36043, 36044, 36045, 36046, 36047, 36048, 36049, 36050, 36051, 36052,
      36053, 36054)

    val data_segments = getDataTriplets(spark, country = "AR", nDays = 60)
      .filter(col("segment").isin(taxonomy: _*))
      .select("device_id", "segment")

    data_audiences
      .join(data_segments, Seq("device_id"))
      .write
      .format("csv")
      .mode("overwrite")
      .save("/datascience/custom/carteles_GCBA_devices_type_segments_equifax")

  }

  def getDataSegmentsForAudienceWithoutFilterPleaseIneedAllDataMaybeIfilterItLater(
      spark: SparkSession
  ) = {

    val data_audiences = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "false")
      .load(
        "/datascience/audiences/crossdeviced/in_store_audiences_xd/"
      )
      .withColumnRenamed("_c1", "device_id")
      .withColumnRenamed("_c2", "original_id")
      .withColumn("device_id", upper(col("device_id")))

    val data_segments = getDataTriplets(spark, country = "MX", nDays = 1)
      .select("device_id", "segment")
      .withColumn("device_id", upper(col("device_id")))

    data_audiences
      .join(data_segments, Seq("device_id"))
      .write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save("/datascience/custom/in_store_audiences_xd_luxottica_w_segments")

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
