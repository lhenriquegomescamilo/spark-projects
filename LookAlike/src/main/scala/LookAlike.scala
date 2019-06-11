package main.scala

import org.apache.spark.mllib.recommendation.{
  ALS,
  Rating,
  MatrixFactorizationModel
}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions.{sum, col, lit, broadcast}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.spark.rdd.RDD
import com.esotericsoftware.kryo.Kryo
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.serializer.{KryoSerializer, KryoRegistrator}

object LookAlike {
  class ALSRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
      kryo.register(classOf[Rating])
    }
  }

  /**
    * This method returns the data that will be used for the look-alike modelling. Basically,
    * it is a set of triplets with 3 columns:
    *  - device_id
    *  - feature
    *  - count
    *
    * Where feature is a segment initially.
    *
    * @param spark: Spark session that will be used to load the data.
    * @param country: country for which the data will be downloaded.
    *
    * Returns a DataFrame that contains the same three columns, filtered by the given country
    * and using only segments that are standar or custom.
    */
  def getData(spark: SparkSession, country: String): DataFrame = {
    val data: DataFrame = spark.read
      .parquet(
        "/datascience/data_demo/triplets_segments/country=%s/".format(country)
      )
      .filter("feature<550 or feature>1500")

    data
  }

  /**
    * Given a DataFrame, this function returns a new DataFrame with a new column that indicates
    * the index for every row.
    *
    * @param df: DataFrame for which the new column is going to be added.
    * @param offset: integer from which the index will begin.
    * @param colName: name's string that will be used to call the new column.
    * @param inFront: boolean that says if the new column should be added to the front of the
    * DataFrame or not.
    *
    * Returns a new DataFrame with a new column that is the index that has been added.
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

  /**
    * Given a DataFrame with 5 columns, this function returns an RDD that will be used for training
    * and testing the ALS model.
    *
    * @param triplets: DataFrame with 5 columns: device_id, device_index, feature, feature_index, count.
    * @param normalize: string stating the column by which the count should be normalized. Possible values:
    * "device_index", "feature_index", "". If it is empty, then there is no normalization at all.
    *
    */
  def getRatings(triplets: DataFrame, normalize: String): RDD[Rating] = {
    // First of all we decide what kind of normalization is going to be used
    val data = normalize match {
      case "device_index" =>
        triplets
          .groupBy("device_index")
          .agg(sum(col("count")).as("total"))
          .join(triplets, Seq("device_index"))
      case "feature_index" =>
        triplets
          .groupBy("feature_index")
          .agg(sum(col("count")).as("total"))
          .join(triplets, Seq("feature_index"))
      case "1" => triplets.withColumn("total", col("count"))
      case ""  => triplets.withColumn("total", lit(1.0))
    }

    // In this section we perform the normalization and transform the DataFrame in an RDD of Ratings.
    val ratings: RDD[Rating] = data.na
      .drop()
      .select("device_index", "feature_index", "count", "total")
      .rdd
      .map(_ match {
        case Row(user, item, rate, total) =>
          Rating(
            user.toString.toInt,
            item.toString.toInt,
            rate.toString.toDouble / total.toString.toDouble
          )
      })

    ratings //.repartition(1000)
  }

  /**
    *
    */
  def getTripletsWithIndex(spark: SparkSession, country: String = "MX") {
    // Load the triplets as-is
    val triplets = getData(spark, country)

    // First we calculate the index for the device ids
    dfZipWithIndex(
      triplets.select("device_id").distinct(),
      0,
      "device_index",
      false
    ).withColumn("country", lit(country))
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("country")
      .save("/datascience/data_lookalike/device_index")

    // Now we calculate the index for the features
    dfZipWithIndex(
      triplets.select("feature").distinct(),
      0,
      "feature_index",
      false
    ).withColumn("country", lit(country))
      .write
      .mode("append") //SaveMode.Overwrite)
      .partitionBy("country")
      .save("/datascience/data_lookalike/feature_index")

    // Finally, we merge all the information so that we generate the triplets, with the index numbers
    // instead of the regular strings
    val device_index = spark.read.load(
      "/datascience/data_lookalike/device_index/country=%s/".format(country)
    )
    val feature_index = spark.read.load(
      "/datascience/data_lookalike/feature_index/country=%s/".format(country)
    )
    triplets
      .join(broadcast(feature_index), Seq("feature"))
      .join(device_index, Seq("device_id"))
      .withColumn("country", lit(country))
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("country")
      .save("/datascience/data_lookalike/segment_triplets_with_index")
  }

  def train(
      spark: SparkSession,
      training: RDD[Rating],
      rank: Int,
      numIter: Int,
      lambda: Double,
      save: Boolean = true
  ): MatrixFactorizationModel = {
    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setRank(rank)
      .setIterations(numIter)
      .setLambda(lambda)
      .setFinalRDDStorageLevel(StorageLevel.NONE)
      .setIntermediateRDDStorageLevel(StorageLevel.MEMORY_AND_DISK_SER)
      .setBlocks(-1)
      .setCheckpointInterval(10)
    val model = als.run(training)

    if (save)
      model.save(spark.sparkContext, "/datascience/data_lookalike/model")

    model
  }

  /**
    * This function takes an RDD of ratings and uses the stored model to make an evaluation of if.
    * It basically calculates the RMSE for the given test ratings.
    *
    * @param spark: Spark Session to load the data and the models.
    * @param test: RDD of ratings with the actual score given to a feature. That is the value this function tries to estimate.
    */
  def evaluate(
      spark: SparkSession,
      test: RDD[Rating],
      model: MatrixFactorizationModel
  ) = {
    // First we definde the schema that will be used to construct a dataframe.
    val schema = StructType(
      Seq(
        StructField(
          name = "device_index",
          dataType = IntegerType,
          nullable = false
        ),
        StructField(
          name = "feature_index",
          dataType = IntegerType,
          nullable = false
        ),
        StructField(name = "count", dataType = DoubleType, nullable = false)
      )
    )

    // Now we calculate the predictions
    val predictions =
      model.predict(test.map(rating => (rating.user, rating.product)))

    // Now I transform everything into a DataFrame
    val predictions_df = spark
      .createDataFrame(
        predictions.map(p => Row(p.user, p.product, p.rating)),
        schema
      )
      .withColumnRenamed("count", "prediction")

    // I do the same with the test RDD.
    val test_df = spark.createDataFrame(
      test.map(p => Row(p.user, p.product, p.rating)),
      schema
    )

    // Finally, I do a join and store the results obtained
    test_df
      .join(predictions_df, Seq("device_index", "feature_index"))
      .write
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_lookalike/predictions/")
  }

  /**
    * This function takes a set of triplets, transforms them into a dense vector per user, and then contruct
    * a similarity matrix. Once the similarity matrix is created, it returns all the MatrixEntry obtained
    * and stores them into a file as a csv.
    */
  def getSimilarities(spark: SparkSession) {
    // Imports
    import org.apache.spark.mllib.linalg.{Vector, Vectors}
    import spark.implicits._
    import org.apache.spark.mllib.linalg.distributed.RowMatrix
    import org.apache.spark.sql.functions._
    import scala.collection.mutable.WrappedArray

    // Read the data
    val data = spark.read
      .load(
        "/datascience/data_demo/triplets_segments/country=PE"
      )
      .dropDuplicates("feature", "device_id")
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
      5025,5310,5311,35360,35361,35362,35363"""
        .replace("\n", "")
        .split(",")
        .toList
    val segmentsIndex = segments.zipWithIndex.toDF("feature", "index")

    // Here we select the specified segments.
    val joint = data
      .filter(col("feature").isin(segments: _*))
      .join(broadcast(segmentsIndex), Seq("feature"))
      .select("device_id", "index")
      .rdd
      .map(row => (row(0), row(1)))

    // Now we construct a dense vector for every user.
    val grouped = joint.groupByKey().filter(row => row._2.size > 1)
    val rows = grouped.map(
      row =>
        Vectors
          .sparse(
            segments.size,
            row._2.map(_.toString.toInt).toArray,
            Array.fill(row._2.size)(1.0)
          )
          .toDense
          .asInstanceOf[Vector]
    )

    // Now we construct the similarity matrix
    val mat = new RowMatrix(rows)
    val simsPerfect = mat.columnSimilarities(.4)

    // Finally, we store the similarity matrix
    simsPerfect.entries
      .map(entry => List(entry.i, entry.j, entry.value).mkString(","))
      .saveAsTextFile(
        "/datascience/data_lookalike/similarity_matrix/country=PE"
      )
  }

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(
        "ALS look alike"
      )
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[ALSRegistrator].getName)
      .set("spark.kryoserializer.buffer.mb", "8")
      .set("spark.shuffle.memoryFraction", "0.65") //default is 0.2
      .set("spark.storage.memoryFraction", "0.3")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val spark = sqlContext.sparkSession
    import sqlContext.implicits._
    spark.sparkContext.setCheckpointDir(
      "/datascience/data_lookalike/als_checkpoint"
    )

    Logger.getRootLogger.setLevel(Level.WARN)

    // getTripletsWithIndex(spark, "AR")

    // val triplets = spark.read.load(
    //   "/datascience/data_lookalike/segment_triplets_with_index/country=AR/" // part-02*-3023c398-0b95-4e9d-afb5-196e424c15dd.c000.snappy.parquet"
    // )
    // val ratings = getRatings(triplets, "device_index")

    // val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
    // val model = train(
    //   spark,
    //   training.repartition(2000),
    //   16,
    //   3,
    //   0.01
    // )

    // evaluate(spark, test, model)
    getSimilarities(spark)
  }
}
