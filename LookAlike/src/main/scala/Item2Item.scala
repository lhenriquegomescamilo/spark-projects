package main.scala


import org.apache.spark.mllib.recommendation.{
  ALS,
  Rating,
  MatrixFactorizationModel
}

import java.io._
import scala.collection.mutable.WrappedArray
import com.esotericsoftware.kryo.Kryo

import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.serializer.{KryoSerializer, KryoRegistrator}


import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.evaluation.RegressionEvaluator

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix


object Item2Item {

  /*
  *
  */
  def testModel(spark: SparkSession,
                country: String,
                simMatrixHits: String = "binary",
                predMatrixHits: String = "binary",
                k: Int = 1000) {
    import spark.implicits._
   // 1) Segments definition
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

    // 2) Read the data
    val data = spark.read
      .load(
        "/datascience/data_demo/triplets_segments/country=%s/".format(country)
      )
    
    // 3) Data aggregation
    val dataTriples = data
      .groupBy("device_id", "feature")
      .agg(sum("count")
      .cast("int")
      .as("count"))

    val usersSegmentsData = dataTriples
      .filter(col("feature").isin(segments: _*))     // segment filtering
      .join(broadcast(segmentsIndex), Seq("feature")) // add segment column index
      .select("device_id", "segment_idx", "count")
      .rdd
      .map(row => (row(0), (row(1), row(2))))
      .groupByKey()  // group by device_id

    // 4) Generate similarities matrix
    val simMatrix = getSimilarities(spark, usersSegmentsData, segments.size, 0.05, simMatrixHits)

    // 5) Predictions
    val predictData = predict(spark,
                              usersSegmentsData,
                              segments.size,
                              simMatrix,
                              predMatrixHits)
    // 6) Metrics
    test(spark, predictData, country, segments, k, 100)

  }

  /**
  * It generates the items to items matrix to make predictions.
  */
  def getSimilarities(spark: SparkSession,
                      data: RDD[(Any, Iterable[(Any, Any)])],
                      nSegments: Int,
                      simThreshold: Double = 0.05,
                      simMatrixHits: String = "binary") : CoordinateMatrix = {

    // it selects users with more than 1 segment
    val filteredData = data.filter( row => row._2.size > 1)

    // Versions of user-segment matrix values:
    var rows: RDD[Vector]  = {
     if (simMatrixHits == "count"){
        println(s"Similarity matrix: counts")
        filteredData
          .filter( row => row._2.size > 1)
          .map(row => Vectors.sparse(
                      nSegments,
                      row._2.map(t => t._1.toString.toInt).toArray,
                      row._2.map(t => t._2.toString.toDouble).toArray)
                    .toDense.asInstanceOf[Vector])
      }
      else if (simMatrixHits == "normalized"){
        println(s"Similarity matrix: normalized counts")
        filteredData
        .map(row => (row._1, row._2, row._2.map(t => t._2.toString.toDouble).toArray.sum)) // sum counts by device id
        .map(row => Vectors.sparse(
                      nSegments,
                      row._2.map(t => t._1.toString.toInt).toArray,
                      row._2.map(t => t._2.toString.toDouble/row._3).toArray)
                    .toDense.asInstanceOf[Vector])
      }
      else{
        println(s"Similarity matrix: binary")
        filteredData 
          .map(row => Vectors.sparse(
                      nSegments,
                      row._2.map(t => t._1.toString.toInt).toArray, 
                      Array.fill(row._2.size)(1.0))
                    .toDense.asInstanceOf[Vector])
      }
    }

    // It generates the similartiy matrix 
    val simMatrix = new RowMatrix(rows).columnSimilarities(simThreshold)
       
    // it makes the matrix symmetric
    // main diagonal must be 0
    var simSymmetric = new CoordinateMatrix(
      simMatrix
      .entries
      .union(simMatrix
             .entries
             .map(me => MatrixEntry(me.j, me.i,me.value))
      )  
      ,simMatrix.numRows(), simMatrix.numCols())

    simSymmetric
  }

  /**
  * For each segment, it calculates a score value for all users.
  * (Greater values indicates higher probabilities to belong to the segment)
  */
  def predict(spark: SparkSession,
              data: RDD[(Any, Iterable[(Any, Any)])],
              nSegments: Int,
              similartyMatrix: CoordinateMatrix,
              predMatrixHits: String = "binary") : RDD[(Any, Array[(Int)], Vector)]  =  {

    var indexedData = data
      .filter( row => row._2.size > 1) // filter users 
      .zipWithIndex() // <device_id, device_idx>
      .map(tup => (tup._2, tup._1._1, tup._1._2)) // <device_idx, device_id, segments>

    //IndexedRow -> new (index: Long, vector: Vector) 
    val indexedRows: RDD[IndexedRow] = {
      if (predMatrixHits == "count"){
        println(s"User matrix: counts")
        indexedData
        .map(row => new IndexedRow 
          (row._1, 
          Vectors.sparse(
            nSegments,
            row._3.map(t => t._1.toString.toInt).toArray,
            row._3.map(t => t._2.toString.toDouble).toArray)
          .toDense.asInstanceOf[Vector]))
      }
      else if (predMatrixHits == "normalized"){
        println(s"User matrix: normalized count")
        indexedData
          .map(row => (row._1, row._3, row._3.map(t => t._2.toString.toDouble).toArray.sum)) // sum counts by device id
          .map(row => new IndexedRow 
            (row._1, 
            Vectors.sparse(
                        nSegments,
                        row._2.map(t => t._1.toString.toInt).toArray,
                        row._2.map(t => t._2.toString.toDouble/row._3).toArray)
                      .toDense.asInstanceOf[Vector]))
      }
      else{
        println(s"User matrix: binary")
        indexedData
        .map(row => new IndexedRow 
          (row._1, 
          Vectors.sparse(
            nSegments,
            row._3.map(t => t._1.toString.toInt).toArray, 
            Array.fill(row._3.size)(1.0))
          .toDense.asInstanceOf[Vector]
          ))
      }
    }

    val userSegmentMatrix = new IndexedRowMatrix(indexedRows)

    // it calculates the prediction scores
    // it doesn't use the current segment to make prediction,
    // because the main diagonal of similarity matrix is 0 
    var scoreMatrix = userSegmentMatrix.multiply(similartyMatrix.toBlockMatrix().toLocalMatrix())
    // this operation preserves partitioning

    var userPredictionMatrix = (scoreMatrix
      .rows
      .map(row => (row.index, row.vector))
      .join(indexedData.map(t => (t._1, (t._2, t._3)))) // (device_idx, (device_id, segments))
      .map(tup => (tup._2._2._1, tup._2._2._2.map(t =>  t._1.toString.toInt).toArray, tup._2._1))
    )// <device_id, array segments index, predictios segments>

    userPredictionMatrix
  }

  def expand(spark: SparkSession, data: RDD[(Any, Array[(Int)], Vector)],
                         selectedSegments: List[Int],
                         segmentLabels: List[String],
                         country: String,
                         k: Int = 1000){
  import spark.implicits._ 
  import org.apache.spark.mllib.rdd.MLPairRDDFunctions.fromPairRDD
  
  // It gets the score thresholds to get at least k elements per segment.
  var minScores = data
    .flatMap(tup => // Select segments - format <segment_idx, score, hasSegment >
      selectedSegments.map(segmentIdx => (segmentIdx, tup._3.apply(segmentIdx), 
                                          tup._2 contains segmentIdx))) 
    .filter(tup => (tup._2 > 0 && !tup._3)) // it selects scores > 0 and devices without the segment
    .map(tup => (tup._1, tup._2))// Format  <segment_idx, score>
    .topByKey(k) // get topK scores values
    .map(t => (t._1, t._2.last)) // get the kth value
    .collect()
    .toMap

  var dataExpansion = data
      .map(
          tup => 
            (tup._1,
             selectedSegments
              .filter(segmentIdx => // select segments with scores > th and don't contain the segment
                (tup._3.apply(segmentIdx) >= minScores(segmentIdx) && !(tup._2 contains segmentIdx)))
              .map(segmentIdx => segmentLabels(segmentIdx)) // segment label
              .mkString(",") // toString
            )              
      )
      .filter(tup => tup._2.length > 0) // exclude devices with empty segments

    dataExpansion
      .toDF("device_id", "segments" )
      .write
      .format("csv")
      .option("sep", "\t")
      .option("header", "false")
      .mode(SaveMode.Overwrite)
      .save(
      "/datascience/data_lookalike/expansion/country=%s/".format(country)
      )
  
  }

  def test(spark: SparkSession, data: RDD[(Any, Array[(Int)], Vector)],
                          country: String,
                          segmentLabels: List[String],
                          k: Int = 1000,
                          minSegmentSupport: Int = 100){
  import spark.implicits._ 
  import org.apache.spark.mllib.rdd.MLPairRDDFunctions.fromPairRDD

  //var nUsers = data.count()
  //println(s"users: $nUsers")

  //var nSegments = data.map(t=>t._3.size).take(1)(0)
  var nSegments =  segmentLabels.length
  val segmentSupports = (data.flatMap(tup => tup._2)).countByValue()

  val selectedSegments = (0 until nSegments).filter(segmentIdx => segmentSupports.getOrElse(segmentIdx, 0).toString().toInt >= minSegmentSupport)

  var scores = data
    .flatMap(tup => selectedSegments.map(segmentIdx => (segmentIdx, tup._3.apply(segmentIdx)) ))
    .filter(tup => (tup._2 > 0)) // select scores > 0
  // (<segment_idx>, score)

  var minScores = scores.topByKey(k).map(t => (t._1, t._2.last)).collect().toMap
  
  // <segmentIdx> -> (nSelected, nTP)
  var relevanCount = data
    .flatMap(
        tup => selectedSegments.map(
          segmentIdx => (segmentIdx,
                          (if(tup._3.apply(segmentIdx) >= minScores(segmentIdx)) 1 else 0,
                          if((tup._2 contains segmentIdx) && (tup._3.apply(segmentIdx) >= minScores(segmentIdx))) 1 else 0))
                          ) 
    )
    .reduceByKey(
      (a, b) => ((a._1 + b._1), (a._2 + b._2))
    )
    .collect
    .toMap

   // (segment_id, count, selected, min_score, prec, recall)
    var metrics = (
        selectedSegments
        .map(segmentIdx => (segmentLabels(segmentIdx).toString,
                            segmentSupports(segmentIdx).toDouble, 
                            relevanCount(segmentIdx)._1.toDouble,  
                            minScores(segmentIdx).toDouble,
                            if(relevanCount(segmentIdx)._2 > 0) relevanCount(segmentIdx)._2.toDouble / relevanCount(segmentIdx)._1.toDouble else 0.0,
                            if(segmentSupports(segmentIdx) > 0) relevanCount(segmentIdx)._2.toDouble / segmentSupports(segmentIdx).toDouble else 0.0)
        )
    )

    var dfMetrics = metrics
      .toDF("segment", "nRelevant", "nSelected", "scoreTh", "precision", "recall" )
      .withColumn("f1", when($"precision" + $"recall" > 0.0, ($"precision" * $"recall") / ( $"precision" + $"recall") * 2.0).otherwise(0.0))
      .sort(desc("precision"))
    
    var dfAvgMetrics = dfMetrics
      .select(
        lit("Avg").as("segmentIdx"),
        avg($"nRelevant").as("nRelevant"),
        avg($"nSelected").as("nSelected"),
        avg($"scoreTh").as("scoreTh"),
        avg($"precision").as("precision"),
        avg($"recall").as("recall"),
        avg($"f1").as("f1")
      )
    var df = dfMetrics.union(dfAvgMetrics)
    
    df
      .write
      .format("csv")
      .option("sep", ",")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(
      "/datascience/data_lookalike/metrics/country=%s/".format(country)
      )
    
    df.show(nSegments + 1, false)
  }

  /*
  * It calculates precision, recall and F1 metrics.
  */
  def calculateRelevanceMetrics(spark: SparkSession,
                        data: RDD[(Any, Array[(Int)], Vector)],
                        country: String,
                        k: Int = 1000,
                        minSegmentSupport: Int = 100) {
      import spark.implicits._              
      data.persist(StorageLevel.MEMORY_AND_DISK)

      var nUsers = data.count()
      var nSegments = data.map(t=>t._3.size).take(1)(0)

      //2) information retrieval metrics - recall@k - precision@k - f1@k
      var meanPrecisionAtK = 0.0
      var meanRecallAtK = 0.0
      var meanF1AtK = 0.0
      var segmentCount = 0
      val segmentSupports = (data.flatMap(tup => tup._2)).countByValue()
      
      // for each segment
      for (segmentIdx <- 0 until nSegments){
        //  for (segmentIdx <- 0 until 35){
        // number of users assigned to segment
        var nRelevant = segmentSupports.getOrElse(segmentIdx, 0).toString().toInt
        
        if (nRelevant > minSegmentSupport){
          // Number of users to select with highest score
          //var nSelected = if (nRelevant>k) k else nRelevant

          var selected = data
            .map(tup=> (tup._2 contains segmentIdx, tup._3.apply(segmentIdx)))
            .filter(tup=> tup._2 > 0) // select scores > 0
            .takeOrdered(k)(Ordering[Double].on(tup=> -1 * tup._2))
          var tp = selected.map(tup=> if (tup._1) 1.0 else 0.0).sum
          // precision & recall
          var precision = tp / k
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

      println(s"precision@k: $meanPrecisionAtK")
      println(s"recall@k: $meanRecallAtK")
      println(s"f1@k: $meanF1AtK")
      println(s"k: $k")
      println(s"users: $nUsers")
      println(s"segments: $nSegments")
      println(s"segmentCount: $segmentCount")
      println(s"minSegmentSupport: $segmentCount")

      val metricsDF = Seq(
        ("precision@k", meanPrecisionAtK),
        ("recall@k", meanRecallAtK),
        ("f1@k", meanF1AtK),
        ("k", k.toDouble),
        ("users", nUsers.toDouble),
        ("segments", nSegments.toDouble),
        ("selectedSegments", segmentCount.toDouble),
        ("segmentMinSupport", minSegmentSupport.toDouble)
      ).toDF("metric", "value")
       .write
       .format("csv")
       .option("sep",",")
       .option("header","true")
       .mode(SaveMode.Overwrite)
       .save(
         "/datascience/data_lookalike/metrics/country=%s/".format(country)
        )
    }


  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Item2item look alike")
      //.set("spark.memory.fraction", "0.7") // default	0.6
      //.set("spark.memory.storageFraction", "0.7") // default	0.5
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val spark = sqlContext.sparkSession
    import sqlContext.implicits._
    spark.sparkContext.setCheckpointDir(
      "/datascience/data_lookalike/i2i_checkpoint"
    )
    Logger.getRootLogger.setLevel(Level.WARN)
    val country = if (args.length > 0) args(0).toString else "PE"
    val k = if (args.length > 1) args(1).toString.toInt else 1000
    val simHits = if (args.length > 2) args(2).toString else "binary"
    val predHits = if (args.length > 3) args(3).toString else "binary"
    testModel(spark, country, simHits, predHits, k)
  }
}
