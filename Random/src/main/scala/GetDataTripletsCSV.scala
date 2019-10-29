package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}

/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object GetDataTripletsCSV {

  def getDataTriplets(
      spark: SparkSession,
      country: String,
      nDays: Int = -1,
      path: String = "/datascience/data_triplets/segments/",
      from: Int = 1
  ) = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val df = if (nDays > 0) {
      // read files from dates
      val format = "yyyyMMdd"
      val endDate = DateTime.now.minusDays(from)
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

  def getDataTriplets(spark: SparkSession, nDays: Int, from: Int) = {
    val segments =
      """2,3,4,5,6,7,8,9,26,32,36,59,61,82,85,92,104,118,129,131,141,144,145,147,149,150,152,154,155,158,160,165,166,177,178,210,213,218,224,225,226,230,245,
    247,250,264,265,270,275,276,302,305,311,313,314,315,316,317,318,322,323,325,326,352,353,354,356,357,358,359,363,366,367,374,377,378,379,380,384,385,
    386,389,395,396,397,398,399,401,402,403,404,405,409,410,411,412,413,418,420,421,422,429,430,432,433,434,440,441,446,447,450,451,453,454,456,457,458,
    459,460,462,463,464,465,467,895,898,899,909,912,914,915,916,917,919,920,922,923,928,929,930,931,932,933,934,935,937,938,939,940,942,947,948,949,950,
    951,952,953,955,956,957,1005,1116,1159,1160,1166,2064,2623,2635,2636,2660,2719,2720,2721,2722,2723,2724,2725,2726,2727,2733,2734,2735,2736,2737,2743,
    3010,3011,3012,3013,3014,3015,3016,3017,3018,3019,3020,3021,3022,3023,3024,3025,3026,3027,3028,3029,3030,3031,3032,3033,3034,3035,3036,3037,3038,3039,
    3040,3041,3042,3043,3044,3045,3046,3047,3048,3049,3050,3051,3055,3076,3077,3084,3085,3086,3087,3302,3303,3308,3309,3310,3388,3389,3418,3420,3421,3422,
    3423,3450,3470,3472,3473,3564,3565,3566,3567,3568,3569,3570,3571,3572,3573,3574,3575,3576,3577,3578,3579,3580,3581,3582,3583,3584,3585,3586,3587,3588,
    3589,3590,3591,3592,3593,3594,3595,3596,3597,3598,3599,3600,3730,3731,3732,3733,3779,3782,3843,3844,3913,3914,3915,4097,
    5025,5310,5311,35360,35361,35362,35363"""
        .replace("\n", "")
        .split(",")
        .map(_.toInt)
        .toSeq

    val countries = List("MX", "AR", "CL", "BR")

    for (country <- countries) {
      val triplets = getDataTriplets(spark, country, nDays, from = from)
      triplets
        .filter(col("feature").isin(segments: _*))
        .select("device_id", "feature")
        .distinct()
        .write
        .format("csv")
        .mode("overwrite")
        .save("/datascience/custom/triplets_csv/country=%s".format(country))
    }
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

    getDataLookAlike(spark = spark)

  }
}
