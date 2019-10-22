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

object DatasetSegmentTriplets{
    def generateSegmentTriplets(
      spark: SparkSession,
      gtDF: DataFrame,
      country: String,
      joinType: String,
      name: String,
      ndays:Int = 30
  ) = {
    
    // List of segments that will be considered. The rest of the records are going to be filtered out.
    val segments =
      """26,32,36,59,61,82,85,92,104,118,129,131,141,144,145,147,149,150,152,154,155,158,160,165,166,177,178,210,213,218,224,225,226,230,245,
        247,250,264,265,270,275,276,302,305,311,313,314,315,316,317,318,322,323,325,326,352,353,354,356,357,358,359,363,366,367,374,377,378,379,380,384,385,
        386,389,395,396,397,398,399,401,402,403,404,405,409,410,411,412,413,418,420,421,422,429,430,432,433,434,440,441,446,447,450,451,453,454,456,457,458,
        459,460,462,463,464,465,467,895,898,899,909,912,914,915,916,917,919,920,922,923,928,929,930,931,932,933,934,935,937,938,939,940,942,947,948,949,950,
        951,952,953,955,956,957,1005,1116,1159,1160,1166,2064,2623,2635,2636,2660,2719,2720,2721,2722,2723,2724,2725,2726,2727,2733,2734,2735,2736,2737,2743,
        3010,3011,3012,3013,3014,3015,3016,3017,3018,3019,3020,3021,3022,3023,3024,3025,3026,3027,3028,3029,3030,3031,3032,3033,3034,3035,3036,3037,3038,3039,
        3040,3041,3042,3043,3044,3045,3046,3047,3048,3049,3050,3051,3055,3076,3077,3084,3085,3086,3087,3302,3303,3308,3309,3310,3388,3389,3418,3420,3421,3422,
        3423,3450,3470,3472,3473,3564,3565,3566,3567,3568,3569,3570,3571,3572,3573,3574,3575,3576,3577,3578,3579,3580,3581,3582,3583,3584,3585,3586,3587,3588,
        3589,3590,3591,3592,3593,3594,3595,3596,3597,3598,3599,3600,3730,3731,3732,3733,3779,3782,3843,3844,3913,3914,3915,4097,
        5025,5310,5311,35360,35361,35362,35363""".replace("\n", "").split(",").toList.toSeq
    
    
    // Now we load the triplets, for a particular country. Here we do the group by.
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    val format = "yyyyMMdd"
    val start = DateTime.now.minusDays(ndays)
    val end = DateTime.now.minusDays(0)

    val daysCount = Days.daysBetween(start, end).getDays()
    val days =
      (0 until daysCount).map(start.plusDays(_)).map(_.toString(format))

    val dfs = days
      .filter(
        day =>
          fs.exists(
            new org.apache.hadoop.fs.Path(
              "/datascience/data_triplets/segments/day=%s/country=%s/".format(day,country)
            )
          )
      )
      .map(
        x =>
          spark.read
            .parquet("/datascience/data_triplets/segments/day=%s/country=%s/".format(x,country))
            .select("device_id", "feature")
      )

      val triplets = dfs.reduce((df1, df2) => df1.union(df2))
                        .filter(col("feature").isin(segments: _*))
                        .distinct()
                        .groupBy("device_id")
                        .agg(collect_list(col("feature")).as("feature"))
                        .withColumn("feature", concat_ws(";", col("feature")))

      // Finally we perform the join between the users with no ground truth (left_anti join).
      val join = gtDF.join(triplets, Seq("device_id"), joinType)
                      .select("device_id","feature")
                      .orderBy(asc("device_id"))
      join.write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .save(
            "/datascience/data_demo/name=%s/country=%s/segment_triplets"
              .format(name, country)
          )
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
