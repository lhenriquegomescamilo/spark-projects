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



object Item2Item {
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
    * This function takes a set of triplets, transforms them into a dense vector per user, and then contruct
    * a similarity matrix. Once the similarity matrix is created, it returns all the MatrixEntry obtained
    * and stores them into a file as a csv.
    *
    * @param spark: Spark session that will be used to load the data.
    * @param simThreshold
    */
  def getSimilarities(spark: SparkSession, simThreshold: Double = 0.05) {
    // Imports
    import org.apache.spark.mllib.linalg.{Vector, Vectors}
    import spark.implicits._
    import org.apache.spark.mllib.linalg.distributed.RowMatrix
    import org.apache.spark.sql.functions._
    import scala.collection.mutable.WrappedArray
    import org.apache.spark.mllib.linalg.distributed.MatrixEntry
    import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
    import java.io._

    // Read the data
    val data = spark.read
      .load(
        "/datascience/data_demo/triplets_segments/country=PE"
      )
      .dropDuplicates("feature", "device_id")

    // Segments definition
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
    val segmentsIndex = segments.zipWithIndex.toDF("feature", "segment_idx")

    // Here we select the specified segments
    val joint = data
      .filter(col("feature").isin(segments: _*))
      .join(broadcast(segmentsIndex), Seq("feature"))
      .select("device_id", "segment_idx")
      .rdd
      .map(row => (row(0), row(1)))

    // agrupa por deice_id y se queda con los usuarios con más de un segmento asignado
    val rows = joint
      .groupByKey()
      .filter( row => row._2.size > 1 ) // it selects users with more than 1 segment
      .map(
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
    val userSegmentMatrix = new RowMatrix(rows)
    val simMatrix = userSegmentMatrix.columnSimilarities(simThreshold)

    // We store the similarity matrix
    /*simMatrix.entries
      .map(entry => List(entry.i, entry.j, entry.value).mkString(","))
      .saveAsTextFile(
        "/datascience/data_lookalike/similarity_matrix/country=PE"
      )*/

    // it makes the matrix symmetric
    // main diagonal is 0
    var simSymmetric = new CoordinateMatrix(
      simMatrix
      .entries
      .union(simMatrix
             .entries
             .map(me => MatrixEntry(me.j, me.i,me.value))
      )  
      ,simMatrix.numRows(), simMatrix.numCols())

    // it calculates the prediction scores
    // it doesn't use the current segment to make prediction,
    // because the main diagonal of similarity matrix is 0 
    var scoreMatrix = userSegmentMatrix.multiply(simSymmetric.toBlockMatrix().toLocalMatrix())
    // this operation preserves partitioning

    // ---- Evaluation metrics ---------------------
    // it merges scores with segments per user
    var userEvalMatrix = scoreMatrix
    .rows
    .zipWithIndex()
    .map(tup => (tup._2, tup._1))
    .join(
      userSegmentMatrix
      .rows
      .zipWithIndex()
      .map(tup => (tup._2, tup._1))
    )
    .map(tup => (tup._2))
    .cache()

    var nUsers = userEvalMatrix.count()
    // 1) root-mean-square error 
    var rmse = Math.sqrt(userEvalMatrix.map(tup => Vectors.sqdist(tup._1, tup._2) / tup._1.size).sum() / nUsers)

    // generate broadcast with l1
    var rmse_normalized = Math.sqrt(userEvalMatrix.map(tup => Vectors.sqdist(tup._1, tup._2) / tup._1.size).sum() / nUsers)

    //2) information retrieval metrics - recall@k - precision@k - f1@k
    var meanPrecisionAtK = 0.0
    var meanRecallAtK = 0.0
    var meanF1AtK = 0.0
    var k = 1000
    var minSegmentSupport = 100
    var segmentCount = 0

    val segmentSupports = userSegmentMatrix
      .rows
      .map(a => a.toArray)
      .reduce((a, b) => (a, b).zipped.map(_ + _))

    // for each segment
    for (segmentIdx <- 0 until segments.length){
      //  for (segmentIdx <- 0 until 35){
      // number of users assigned to segment
      var nRelevant = segmentSupports.apply(segmentIdx).toInt
      
      if (nRelevant > minSegmentSupport){
        // Number of users to select with highest score
        var nSelected = if (nRelevant>k) k else nRelevant

        var selected = userEvalMatrix
          .map(tup=> (tup._1.apply(segmentIdx), tup._2.apply(segmentIdx)))
          .filter(tup=> tup._1 > 0) // select scores > 0
          .takeOrdered(nSelected)(Ordering[Double].on(tup=> -1 * tup._1))
        var tp = selected.map(tup=>tup._2).sum
        // precision & recall
        var precision = tp / nSelected
        var recall = tp / nRelevant
        var f1 = if (precision + recall > 0)  2* precision * recall / (precision + recall) else 0.0
        meanPrecisionAtK += precision
        meanRecallAtK += recall
        meanF1AtK += f1
        segmentCount += 1
      }

    }
    meanPrecisionAtK /= segmentCount
    meanRecallAtK /= segmentCount
    meanF1AtK /= segmentCount

    println(s"RMSE: $rmse")
    println(s"precision@k: $meanPrecisionAtK")
    println(s"recall@k: $meanRecallAtK")
    println(s"f1@k: $meanF1AtK")
    println(s"k: $k")
    println(s"segments: ${segments.length}")
    //println(s"segments: 35")
    println(s"segmentCount: $segmentCount")

    val metricsDF = Seq(
        ("rmse", rmse),
        ("precision@k", meanPrecisionAtK),
        ("recall@k", meanRecallAtK),
        ("f1@k", meanF1AtK),
        ("k", k.toDouble),
        ("users", nUsers.toDouble),
        ("segments", segments.length.toDouble),
        //("segments", 35.0),
        ("segmentMinSupportCount", segmentCount.toDouble),
        ("segmentMinSupport", minSegmentSupport.toDouble)
      ).toDF("metric", "value")
       .write
       .format("csv")
       .option("sep",",")
       .option("header","true")
       .mode(SaveMode.Overwrite)
       .save("/datascience/data_lookalike/metrics/country=PE")

  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Item2item look alike")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val spark = sqlContext.sparkSession
    import sqlContext.implicits._
    spark.sparkContext.setCheckpointDir(
      "/datascience/data_lookalike/i2i_checkpoint"
    )
    Logger.getRootLogger.setLevel(Level.WARN)

    getSimilarities(spark)
  }
}
