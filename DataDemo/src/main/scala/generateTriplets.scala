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
  sum
}
import org.apache.spark.sql.SaveMode
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.spark.sql.functions.broadcast
import org.apache.hadoop.fs.Path

object GenerateTriplets {

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
  def generate_triplets_segments(spark: SparkSession, ndays: Int) {
    /// Configuraciones de spark
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    /// Obtenemos la data de los ultimos ndays
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
              "/datascience/data_audiences/day=%s".format(day)
            )
          )
      )
      .map(
        x =>
          spark.read
            .parquet("/datascience/data_audiences/day=%s".format(x))
            .filter("event_type IN ('batch', 'data', 'tk', 'pv')")
            .select("device_id", "segments", "country")
            .withColumn("segments", explode(col("segments")))
            .withColumnRenamed("segments", "feature")
            .withColumn("count", lit(1))
      )

    val df = dfs.reduce((df1, df2) => df1.union(df2))

    val grouped_data = df
      .groupBy("device_id", "feature", "country")
      .agg(sum("count").as("count"))

    grouped_data.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .partitionBy("country")
      .save("/datascience/data_demo/triplets_segments")
  }

  /**
    * Este metodo se encarga de generar una lista de triplets de la pinta <device_id, keyword, count>
    * utilizando la data ubicada en data_keywords_p. Para generar los triplets se utilizaran tanto las keywords
    * provenientes de las urls visitadas por los usuarios como las keywords provienentes del contenido de las urls (scrapping)-
    * Una vez generado el dataframe se lo guarda en formato parquet dentro de /datascience/data_demo/triplets_keywords
    * Los parametros que recibe son:
    *
    * @param spark: Spark session object que sera utilizado para cargar los DataFrames.
    * @param ndays: cantidad de dias que se utilizaran para generar los triplets.
    *
    */
  def generate_triplets_keywords(spark: SparkSession, ndays: Int) {
    /// Configuraciones de spark
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    /// Obtenemos la data de los ultimos ndays
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
              "/datascience/data_keywords/day=%s".format(day)
            )
          )
      )
      .map(
        x =>
          spark.read
            .parquet("/datascience/data_keywords/day=%s".format(x))
            .select("device_id", "content_keys", "country")
      )

    val df = dfs.reduce((df1, df2) => df1.union(df2))

    /// Obtenemos las keywords del contenido de la url
    val df_content_keys = df
      .select("device_id", "content_keys", "country")
      //.withColumn("content_keys",split(col("content_keys"),","))
      .withColumnRenamed("content_keys", "feature")
      .withColumn("count", lit(1))
      .withColumn("feature", explode(col("feature")))

    val grouped_data = df_content_keys
      .groupBy("device_id", "feature", "country")
      .agg(sum("count").as("count"))

    /// Filtramos las palabras que tiene longitud menor a 3 y guardamos
    grouped_data
      .where(length(col("feature")) > 3)
      .write
      .format("parquet")
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .partitionBy("country")
      .save("/datascience/data_demo/triplets_keywords")

  }

  def generateTripletsForAR(spark: SparkSession) = {
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
      5025,5310,5311""".replace("\n", "").split(",").toList.toSeq

    val gt = spark.read
      .format("csv")
      .option("sep", "\t")
      .load("/datascience/devicer/processed/equifax_demo_AR_grouped/*")
      .withColumnRenamed("_c1", "device_id")
      .withColumnRenamed("_c2", "label")
      .select("device_id", "label")

    val triplets =
      spark.read
        .load("/datascience/data_demo/triplets_segments/country=MX/")
        .filter(col("feature").isin(segments: _*))

    triplets
      .join(gt, Seq("device_id"))
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_demo/expand_triplets_dataset")
  }

  def main(args: Array[String]) {
    /// Configuracion spark
    val spark = SparkSession.builder
      .appName("Get triplets: keywords and segments")
      .getOrCreate()

    // Parseo de parametros
    val ndays = if (args.length > 0) args(0).toInt else 20

    generate_triplets_segments(spark, ndays)
    // generate_triplets_keywords(spark,ndays)
  }
}
