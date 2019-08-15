package main.scala


import org.apache.spark.mllib.recommendation.{
  ALS,
  Rating,
  MatrixFactorizationModel
}

import java.io._
import org.joda.time.DateTime
import scala.collection.mutable.WrappedArray
import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.fs.{FileSystem, Path}

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

  def runExpand(spark: SparkSession,
                filePath: String,
                nDays: Int = -1,
                simMatrixHits: String = "binary",
                predMatrixHits: String = "binary") {
    import spark.implicits._
    // Read input from file
    val expandInput = getSegmentsToExpand(spark, filePath)
    val baseFeatureSegments = getBaseFeatureSegments()
    val extraFeatureSegments = getExtraFeatureSegments()

    // Expansion for each country
    for (country: String <- expandInput.map( v => v("country").toString).toSet){
      println("Training Model")
      println("Country")
      println(country)
      println("Segments")
      println(expandInput.length)
      val countryExpandInput = expandInput.filter(v => v("country").toString == country)
      val nSegmentToExpand = expandInput.length

      // Read data
      val data = getDataTriplets(spark, country, nDays)

      // Create segment index
      var segments = countryExpandInput.map(row=> row("segment_id").toString) // First: segments to expand
      segments ++= baseFeatureSegments.toSet.diff(segments.toSet).toList // Then: segments used as features
      segments ++= extraFeatureSegments.toSet.diff(segments.toSet).toList 

      val segmentToIndex = segments.zipWithIndex.toMap
      val dfSegmentIndex = segments.zipWithIndex.toDF("feature", "segment_idx")

      val baseSegmentsIdx = baseFeatureSegments.map(seg => segmentToIndex(seg))

      // Data aggregation
      val dataTriples = data
        .groupBy("device_id", "feature")
        .agg(sum("count").cast("int").as("count"))

      val usersSegmentsData = dataTriples
        .filter(col("feature").isin(segments: _*))   // segment filtering
        .join(broadcast(dfSegmentIndex), Seq("feature")) // add segment column index
        .select("device_id", "segment_idx", "count")
        .rdd
        .map(row => (row(0), (row(1), row(2))))
        .groupByKey()  // group by device_id
        .filter(row => row._2.map(t => t._1.toString.toInt).exists(baseSegmentsIdx.contains)) // Filter users who contains any base segments

      // Generate similarities matrix
      val simMatrix = getSimilarities(spark,
                                      usersSegmentsData,
                                      segments.size,
                                      0.05,
                                      simMatrixHits)
      println("Predictions")
      val predictData = predict(spark,
                                usersSegmentsData,
                                nSegmentToExpand,
                                simMatrix,
                                predMatrixHits=predMatrixHits,
                                minUserSegments = 1)
      println("Save output")
      // 6) Expansion
      expand(spark,
             predictData,
             countryExpandInput,
             segmentToIndex,
             country)
    }
  }
  /*
  *
  */
  def runTest(spark: SparkSession,
              country: String,
              nDays: Int = -1,
              simMatrixHits: String = "binary",
              predMatrixHits: String = "binary",
              k: Int = 1000) {
    import spark.implicits._

    // 1) Segments to expand
    val expandSegment =
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
    val nSegmentToExpand = expandSegment.length
    val baseFeatureSegments = getBaseFeatureSegments()
    val extraFeatureSegments = getExtraFeatureSegments()

    // 1) Read the data
    val data = getDataTriplets(spark, country, nDays)

    // Create segment index
    var segments = expandSegment // First: segments to expand
    segments ++= baseFeatureSegments.toSet.diff(segments.toSet).toList // Then: segments used as features
    segments ++= extraFeatureSegments.toSet.diff(segments.toSet).toList 

    val segmentToIndex = segments.zipWithIndex.toMap
    val dfSegmentIndex = segments.zipWithIndex.toDF("feature", "segment_idx")

    val baseSegmentsIdx = baseFeatureSegments.map(seg => segmentToIndex(seg))

    // Data aggregation
    val dataTriples = data
      .groupBy("device_id", "feature")
      .agg(sum("count").cast("int").as("count"))

    val usersSegmentsData = dataTriples
      .filter(col("feature").isin(segments: _*))   // segment filtering
      .join(broadcast(dfSegmentIndex), Seq("feature")) // add segment column index
      .select("device_id", "segment_idx", "count")
      .rdd
      .map(row => (row(0), (row(1), row(2))))
      .groupByKey()  // group by device_id
      .filter(row => row._2.map(t => t._1.toString.toInt).exists(baseSegmentsIdx.contains)) // Filter users who contains any base segments

      println("Data - Users")
      println(usersSegmentsData.count())

      // Generate similarities matrix
      val simMatrix = getSimilarities(spark,
                                      usersSegmentsData,
                                      segments.size,
                                      0.05,
                                      simMatrixHits)
      // 5) Predictions
      val predictData = predict(spark,
                                usersSegmentsData,
                                nSegmentToExpand,
                                simMatrix,
                                predMatrixHits=predMatrixHits,
                                minUserSegments = 2)


    // 6) Metrics
    val selectedSegmentsIdx = expandSegment.map(seg => segmentToIndex(seg))
    test(spark, predictData, selectedSegmentsIdx, segments, country, k, 100)
    
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

  /*
  * It reads data triplets for a given country.
  */
  def getDataTriplets(
      spark: SparkSession,
      country: String,
      nDays: Int = -1,
      path: String = "/datascience/data_triplets/segments/") = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val df = if(nDays > 0){
      // read files from dates
      val format = "yyyyMMdd"
      val endDate = DateTime.now.minusDays(1)
      val days = (0 until nDays.toInt).map(endDate.minusDays(_)).map(_.toString(format))
      // Now we obtain the list of hdfs folders to be read
      val hdfs_files = days
        .map(day => path + "/day=%s/country=%s".format(day, country))
        .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      spark.read.option("basePath", path).parquet(hdfs_files: _*)
    }
    else{
      // read all date files
      spark.read.load(path + "/day=*/country=%s/".format(country))
    }
    df
  }


  /**
  * For each segment, it calculates a score value for all users.
  * (Greater values indicates higher probabilities to belong to the segment)
  */
  def predict(spark: SparkSession,
              data: RDD[(Any, Iterable[(Any, Any)])],
              nSegmentToExpand: Int,
              similartyMatrix: CoordinateMatrix,
              predMatrixHits: String = "binary",
              minUserSegments: Int = 1) : RDD[(Any, Array[(Int)], Vector)]  =  {

    var indexedData = data
      .filter( row => row._2.size >= minUserSegments) // filter users
      .zipWithIndex() // <device_id, device_idx>
      .map(tup => (tup._2, tup._1._1, tup._1._2)) // <device_idx, device_id, segments>

    var nSegments = similartyMatrix.numRows().toInt

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
    /*
    // val indexedRows: RDD[IndexedRow] 
    if (similarity == "cosine"){
      l2 = simMatrix.toRowMatrix.rows.map(row => Math.sqrt(Vectors.norm(row, 2))).collect()
      indexedRows.map
    }
    */

    val userSegmentMatrix = new IndexedRowMatrix(indexedRows)
    
    // Collect the distributed matrix on the driver
    val localSimMatrix =  
      new CoordinateMatrix(
        similartyMatrix.entries 
                .filter(me => me.j.toInt < nSegmentToExpand) // select only columns to predict
        ,similartyMatrix.numRows(), nSegmentToExpand)
      .toBlockMatrix()
      .toLocalMatrix()

    
    // it calculates the prediction scores
    // it doesn't use the current segment to make prediction,
    // because the main diagonal of similarity matrix is 0 
    var scoreMatrix = userSegmentMatrix.multiply(localSimMatrix)
    // this operation preserves partitioning

    var userPredictionMatrix = (scoreMatrix
      .rows
      .map(row => (row.index, row.vector))
      .join(indexedData.map(t => (t._1, (t._2, t._3)))) // (device_idx, (device_id, segments))
      .map(tup => (tup._2._2._1,
                   tup._2._2._2
                    .filter(t => t._1.toString.toInt < nSegmentToExpand)
                    .map(t =>  t._1.toString.toInt).toArray,
                   tup._2._1))
    )// <device_id, array segments index, predictios segments>

    userPredictionMatrix
  }


  def expand(spark: SparkSession,
             data: RDD[(Any, Array[(Int)], Vector)],
             expandInput: List[Map[String, Any]] ,
             segmentToIndex: Map[String, Int],
             country: String){
  import spark.implicits._ 
  import org.apache.spark.mllib.rdd.MLPairRDDFunctions.fromPairRDD
  
  val selSegmentsIdx = expandInput.map(m => segmentToIndex(m("segment_id").toString))
  val kMap = expandInput.map(m => 
                segmentToIndex(m("segment_id").toString) -> m("size").toString.toInt).toMap
  val dstSegmentIdMap = expandInput.map(m => 
                segmentToIndex(m("segment_id").toString) -> m("dst_segment_id").toString).toMap
  val kMax = expandInput.map(m => m("size").toString.toInt).max

  // It gets the score thresholds to get at least k elements per segment.
  val minScoreMap = data
    .flatMap(tup => // Select segments - format <segment_idx, score, hasSegment >
      selSegmentsIdx.map(segmentIdx => (segmentIdx, tup._3.apply(segmentIdx), 
                                        tup._2 contains segmentIdx))) 
    .filter(tup => (tup._2 > 0 && !tup._3)) // it selects scores > 0 and devices without the segment
    .map(tup => (tup._1, tup._2))// Format  <segment_idx, score>
    .topByKey(kMax) // get topK scores values
    .map(t => (t._1, if (t._2.length >= kMap(t._1.toInt)) t._2( kMap(t._1.toInt) - 1 ) else t._2.last )) // get the kth value #
    .collect()
    .toMap


  val dataExpansion = data
      .map(
          tup => 
            (tup._1.toString,
             selSegmentsIdx
              .filter(segmentIdx => // select segments with scores > th and don't contain the segment
                ((minScoreMap contains segmentIdx) && tup._3.apply(segmentIdx) >= minScoreMap(segmentIdx) && !(tup._2 contains segmentIdx) ))
              .map(segmentIdx => dstSegmentIdMap(segmentIdx)) // segment label
              .mkString(",") // toString
            )              
      )
      .filter(tup => tup._2.length > 0) // exclude devices with empty segments

  // save
  spark.createDataFrame(dataExpansion)
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

  /*
  * It calculates precision, recall and F1 metrics.
  */
  def test(spark: SparkSession,
           data: RDD[(Any, Array[(Int)], Vector)],
           selectedSegmentsIdx: List[Int],
           segmentLabels: List[String],
           country: String,          
           k: Int = 1000,
           minSegmentSupport: Int = 100){
  import spark.implicits._ 
  import org.apache.spark.mllib.rdd.MLPairRDDFunctions.fromPairRDD
  var nSegments = segmentLabels.length

  val colIndex = (0 until selectedSegmentsIdx.length)
  var scores = data
    .flatMap(tup => colIndex.map(colIdx => (colIdx, tup._3.apply(colIdx)) ))
    .filter(tup => (tup._2 > 0)) // select scores > 0
  // (<segment_idx>, score)

  var minScores = scores.topByKey(k).map(t => (t._1, t._2.last)).collect().toMap
  
  // <segmentIdx> -> (nP, nSelected, nTP)
  var relevanCount = data
    .flatMap(
        tup => minScores.keys.map(
          colIdx => (colIdx,
                     (if(tup._2 contains colIdx) 1 else 0,
                      if(tup._3.apply(colIdx) >= minScores(colIdx)) 1 else 0,
                      if((tup._2 contains colIdx) && (tup._3.apply(colIdx) >= minScores(colIdx) )) 1 else 0
                     )
                    ))
    )
    .reduceByKey(
      (a, b) => ((a._1 + b._1), (a._2 + b._2), (a._3 + b._3))
    )
    .collect
    .toMap

   // (segment_id, count, selected, min_score, prec, recall)
    var metrics = minScores.keys
        .map(colIdx => (segmentLabels(selectedSegmentsIdx(colIdx)).toString,
                        relevanCount(colIdx)._1.toDouble, 
                        relevanCount(colIdx)._2.toDouble,  
                        minScores(colIdx).toDouble,
                        if(relevanCount(colIdx)._2 > 0) 
                          relevanCount(colIdx)._3.toDouble / relevanCount(colIdx)._2.toDouble
                        else 0.0,
                        if(relevanCount(colIdx)._1 > 0)
                          relevanCount(colIdx)._3.toDouble / relevanCount(colIdx)._1.toDouble
                        else 0.0
                      )
        ).toList

    var dfMetrics = metrics
      .toDF("segment", "nRelevant", "nSelected", "scoreTh", "precision", "recall" )
      .filter($"nRelevant" >= minSegmentSupport)
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

  /***
  Read settings to expand.

  ***/
  def getSegmentsToExpand(
      spark: SparkSession,
      filePath: String
  ): List[Map[String, Any]] = {
    // First of all we obtain all the data from the file
    val df = spark.sqlContext.read.json(filePath)
    val columns = df.columns
    val data = df
      .collect()
      .map(fields => fields.getValuesMap[Any](fields.schema.fieldNames))

    // Now we extract the different values from each row.
    var expandInputs = List[Map[String, Any]]()

    for (line <- data) {
      val segmentId = line("segmentId").toString
      //val dstSegmentId = line("dstSegmentId").toString
      
      val dstSegmentId =
          if (line.contains("dstSegmentId") && Option(line("dstSegmentId"))
            .getOrElse("")
            .toString
            .length > 0) line("dstSegmentId").toString
          else segmentId
      
      val size = line("size").toString.toInt
      val country = line("country")

      val actual_map: Map[String, Any] = Map(
        "segment_id" -> segmentId,
        "dst_segment_id" -> dstSegmentId,
        "country" -> country,
        "size" -> size
      )

      expandInputs = expandInputs ::: List(actual_map)
    }
    expandInputs
  }

  /*
  * It reads the segments used to make predictions.
  * TODO: Now, the segments are hard-coded. But, they should be read from a configuration file.
  */
  def getExtraFeatureSegments(
  ): List[String] = {
    val segments =
      """2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,26,32,36,59,61,82,85,92,104,118,129,131,141,144,145,147,149,150,152,154,155,158,160,165,166,177,178,
      210,213,218,224,225,226,230,245,247,250,264,265,270,275,302,305,311,313,314,315,316,317,318,322,323,325,326,2623,2635,2636,2660,2719,2721,2722,
      2723,2724,2725,2726,2727,2733,2734,2735,2736,2737,3010,3011,3012,3013,3014,3015,3016,3017,3018,3019,3020,3021,3022,3055,3076,3077,3085,3086,
      3087,3913,4097,352,353,354,356,357,358,359,363,366,367,374,377,378,379,380,384,385,386,389,395,396,397,398,399,401,402,403,404,405,410,411,
      412,413,418,420,421,422,429,430,432,433,434,440,441,446,447,450,451,453,454,456,457,458,459,460,462,463,464,465,467,895,898,899,909,912,914,
      915,916,917,919,920,922,923,928,929,930,931,932,933,934,935,937,938,939,940,942,947,948,949,950,951,952,953,955,956,957,1005,1159,1160,1166,
      2720,3023,3024,3025,3026,3027,3028,3029,3030,3031,3032,3033,3034,3035,3036,3037,3038,3039,3040,3041,3302,3303,3308,3309,3388,3389,3418,3420,
      3421,3422,3423,3470,3472,3473,3564,3565,3566,3567,3568,3569,3570,3571,3572,3573,3574,3575,3576,3577,3578,3579,3580,3581,3582,3583,3584,3585,
      3586,3587,3588,3589,3590,3591,3592,3593,3594,3595,3596,3597,3598,3599,3600,3779,3782,3914,3915,560,561,562,563,564,565,566,567,568,569,570,571,
      572,573,574"""
        .replace("\n", "")
        .split(",")
        .toList
    segments
  }

  /*
  * It reads the segments used to make predictions.
  * TODO: Now, the segments are hard-coded. But, they should be read from a configuration file.
  */
  def getBaseFeatureSegments(
  ): List[String] = {
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
        .toList
    segments
  }

  type OptionMap = Map[Symbol, String]
  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--simHits" :: value :: tail =>
        nextOption(map ++ Map('simHits -> value), tail)
      case "--predHits" :: value :: tail =>
        nextOption(map ++ Map('predHits -> value), tail)
      case "--filePath" :: value :: tail =>
        nextOption(map ++ Map('filePath -> value), tail)
       case "--test" :: value :: tail =>
        nextOption(map ++ Map('test -> value), tail)
      case "--testCountry" :: value :: tail =>
        nextOption(map ++ Map('testCountry -> value), tail)
      case "--testSize" :: value :: tail =>
        nextOption(map ++ Map('testSize -> value), tail)
      case "--nDays" :: value :: tail =>
        nextOption(map ++ Map('nDays -> value), tail)
    }
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
    val options = nextOption(Map(), args.toList)

    val filePath =
      if (options.contains('filePath)) options('filePath) else "/datascience/data_lookalike/input/input.json"
    val simHits =
      if (options.contains('simHits)) options('simHits) else "binary"
    val predHits =
      if (options.contains('predHits)) options('predHits) else "binary"
    val isTest =
      if (options.contains('test)) true else false
    val testCountry =
      if (options.contains('testCountry)) options('testCountry) else "PE"
    val testSize =
      if (options.contains('testSize)) options('testSize).toInt else 1000
    val nDays =
      if (options.contains('nDays)) options('nDays).toInt else -1
    if(isTest)
      runTest(spark, testCountry, nDays, simHits, predHits, testSize)
    else
      runExpand(spark, filePath, nDays, simHits, predHits)
  }
}
