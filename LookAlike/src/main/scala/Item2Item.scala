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
import org.apache.hadoop.conf.Configuration
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

import org.apache.spark.mllib.linalg.{Vector, Vectors, Matrix}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix


object Item2Item {

  /**
  It processes all input files from /datascience/data_lookalike/to_process/
  */
  def processPendingJobs(spark: SparkSession,
                         nDays: Int = -1,
                         simMatrixHits: String = "binary",
                         predMatrixHits: String = "binary") {

    val pathToProcess = "/datascience/data_lookalike/to_process/"
    val pathInProcess = "/datascience/data_lookalike/in_process/"
    val pathDone = "/datascience/data_lookalike/done/"
    val pathFailed = "/datascience/data_lookalike/errors/"
    
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    
    // 1 reads to process
    // Now we order the files according to their date (filename, timestamp).
    var filesToProcess = fs
      .listStatus(new Path(pathToProcess))
      .map(
        f =>
          (f.getPath.toString.split("/").last.toString, f.getModificationTime)
      )
      .toList
    // Now we sort the list by the second component (timestamp)
    filesToProcess = scala.util.Sorting.stableSort(
      filesToProcess,
      (e1: (String, Long), e2: (String, Long)) => e1._2 < e2._2
    ).toList

    println("LOOKALIKE LOG: Jobs to process = " + filesToProcess.length.toString)

    for (file <- filesToProcess){
      var filename = file._1
      var fileToProcess = pathToProcess + filename
      var fileInProcess = pathInProcess + filename
      var fileFailed = pathFailed + filename
      var fileDone = pathDone + filename
      var processError = false

      fs.rename(new Path(fileToProcess), new Path(fileInProcess))

      try {
        runExpand(spark, fileInProcess, nDays, simMatrixHits, predMatrixHits)
      } catch {
        case e: Throwable => {
          var errorMessage = e.toString()
          println("LOOKALIKE LOG: The lookalike process failed on " + filename + "\nThe error was: " + errorMessage)
          processError = true
      }
    }
    if (!processError)
      fs.rename(new Path(fileInProcess), new Path(fileDone))
    else
      fs.rename(new Path(fileInProcess), new Path(fileFailed))
    }
  }

  /**
  It runs a expansion using the input file specifications.
  The input file must have the country, and all segments to expand.
  */
  def runExpand(spark: SparkSession,
                filePath: String,
                nDays: Int = -1,
                simMatrixHits: String = "binary",
                predMatrixHits: String = "binary") {
    import spark.implicits._

    println("LOOKALIKE LOG: Input File: " + filePath)
    // Read input from file
    val expandInput = readSegmentsToExpand(spark, filePath)
    val metaInput = readMetaParameters(spark, filePath)

    val isOnDemand = metaInput("job_id").length > 0
    val country = metaInput("country")

    val baseFeatureSegments = getBaseFeatureSegments()
    val extraFeatureSegments = getExtraFeatureSegments()

    val nSegmentToExpand = expandInput.length

    if (isOnDemand)
      println("LOOKALIKE LOG: JobId: " + metaInput("job_id") + " - Country: " + country + " - nSegments: " + nSegmentToExpand.toString)
    else
      println("LOOKALIKE LOG: Country: " + country + " - nSegments: " + nSegmentToExpand.toString)
    

    // Read data
    println("LOOKALIKE LOG: Model training")

    val data = getDataTriplets(spark, country, nDays)

    // Create segment index
    var segments = expandInput.map(row=> row("segment_id").toString) // First: segments to expand
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
                                    nSegmentToExpand,
                                    0.05,
                                    simMatrixHits)

    println("LOOKALIKE LOG: Expansion")
    expand(spark,
           usersSegmentsData,
           expandInput,
           segmentToIndex,
           metaInput,
           simMatrix,
           predMatrixHits)
  
  
  }


  /*
  * Run a test to get precision and recall metrics for the first kth expansions in a country of all standard segments.
  */
  def runTest(spark: SparkSession,
              country: String,
              nDays: Int = -1,
              simMatrixHits: String = "binary",
              predMatrixHits: String = "binary",
              k: Int = 1000) {
    import spark.implicits._
    val expandInput = getSegmentsToTest(k)
    val metaInput: Map[String, String] = Map("country" -> country)

    val nSegmentToExpand = expandInput.length
    val baseFeatureSegments = getBaseFeatureSegments()
    val extraFeatureSegments = getExtraFeatureSegments()

    // 1) Read the data
    val data = getDataTriplets(spark, country, nDays)

    // Create segment index
    var segments = expandInput.map(row=> row("segment_id").toString) // First: segments to expand
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
                                      nSegmentToExpand,
                                      0.05,
                                      simMatrixHits,
                                      true)
      expand(spark,
            usersSegmentsData,
            expandInput,
            segmentToIndex,
            metaInput,
            simMatrix,
            predMatrixHits,
            true)
    
  }

  /**
  * It generates the items to items matrix to make predictions.
  */
  def getSimilarities(spark: SparkSession,
                      data: RDD[(Any, Iterable[(Any, Any)])],
                      nSegments: Int,
                      nSegmentToExpand: Int,
                      simThreshold: Double = 0.05,
                      simMatrixHits: String = "binary",
                      isTest: Boolean = false): Matrix = {

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

    var diagonalValue = if(isTest) 0.0 else Double.MinValue

    var simSymmetric = new CoordinateMatrix(
      simMatrix
      .entries
      .union(simMatrix.entries.map(me => MatrixEntry(me.j, me.i,me.value))) // it makes the matrix symmetric
      .union(spark.sparkContext.parallelize((0 until nSegmentToExpand).map(v => MatrixEntry(v, v, diagonalValue))))
      .filter(me => me.j.toInt < nSegmentToExpand) // select only columns to predict
      ,simMatrix.numRows(), nSegmentToExpand)
                    
    // Collect the distributed matrix on the driver
    var localMartix = simSymmetric.toBlockMatrix().toLocalMatrix()
    localMartix
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
  * For each segment, it calculates a score value for all users and generates the expansion.
  */
  def expand(spark: SparkSession,
              data: RDD[(Any, Iterable[(Any, Any)])],
              expandInput: List[Map[String, Any]] ,
              segmentToIndex: Map[String, Int],
              metaParameters: Map[String, String],
              similartyMatrix: Matrix,
              predMatrixHits: String = "binary",
              isTest: Boolean = false) =  {
    import spark.implicits._
    import org.apache.spark.mllib.rdd.MLPairRDDFunctions.fromPairRDD
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.row_number

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val country = metaParameters("country")
    val jobId = metaParameters("job_id")
    val isOnDemand = jobId.length > 0
    
    val minUserSegments = if(isTest) 2 else 1

    // index data and write in temporal file
    val indexTmpPath = {
      if(isOnDemand)
        "/datascience/data_lookalike/tmp/indexed_devices/jobId=%s/".format(jobId)
      else
       "/datascience/data_lookalike/tmp/indexed_devices/country=%s/".format(country)
    }

    if (!fs.exists(new org.apache.hadoop.fs.Path(indexTmpPath))){
      // generte data to store
      data
        .filter(row => row._2.size >= minUserSegments) // filter users
        .zipWithIndex() // <device_id, device_idx>
        .map(tup => (tup._2.toLong, 
                    tup._1._1.toString,
                    tup._1._2.map(t => t._1.toString.toInt).toArray,
                    tup._1._2.map(t => t._2.toString.toInt).toArray )
        ) // <device_idx, device_id, array segments, array counts>
        .toDF("device_idx", "device_id", "segments", "counts")
            .write
            .mode(SaveMode.Overwrite)
            .format("parquet")
            .save(indexTmpPath)
    } 

    // reload indexed data
    var indexedData = spark.read.load(indexTmpPath).as[(Long, String, Array[Int], Array[Double])].rdd

    var nSegments = similartyMatrix.numRows.toInt

    //IndexedRow -> new (index: Long, vector: Vector) 
    val indexedRows: RDD[IndexedRow] = {
      if (predMatrixHits == "count"){
        println(s"User matrix: counts")
        indexedData
        .map(row => new IndexedRow(row._1, Vectors.sparse(nSegments, row._3, row._4).toDense.asInstanceOf[Vector]))
      }
      else if (predMatrixHits == "normalized"){
        println(s"User matrix: normalized count")
        indexedData
          .map(row => (row._1, row._3, row._4, row._4.sum)) // sum counts by device id
          .map(row => new IndexedRow(row._1,Vectors.sparse(nSegments, row._2, row._3.map(t => t/row._4)).toDense.asInstanceOf[Vector]))
      }
      else{
        println(s"User matrix: binary")
        indexedData
        .map(row => new IndexedRow(row._1,Vectors.sparse(nSegments, row._3, Array.fill(row._3.size)(1.0)).toDense.asInstanceOf[Vector]))
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

    // write scores in temporal file
    val scoresTmpPath = { 
      if(isOnDemand)
        "/datascience/data_lookalike/tmp/scores/jobId=%s/".format(jobId)
      else
       "/datascience/data_lookalike/tmp/scores/country=%s/".format(country)
    }
    if (!fs.exists(new org.apache.hadoop.fs.Path(scoresTmpPath))){
      userSegmentMatrix
        .multiply(similartyMatrix)
        .rows
        .map(row =>  (row.index, row.vector)) 
        .toDF("device_idx", "scores")  
        .write
        .mode(SaveMode.Overwrite)
        .format("parquet")
        .save(scoresTmpPath)
    }

    // reload scores
    val scoreMatrix = spark.read.load(scoresTmpPath).as[(Long, Vector)].rdd

    // It gets the score thresholds to get at least k elements per segment.
    val selSegmentsIdx = expandInput.map(m => segmentToIndex(m("segment_id").toString))
    val sizeMap = expandInput.map(m => segmentToIndex(m("segment_id").toString) -> m("size").toString.toInt).toMap
    val sizeMax = expandInput.map(m => m("size").toString.toInt).max + 1
    val sizeMin = expandInput.map(m => m("size").toString.toInt).min + 1

    /*
    val minScoreMap = scoreMatrix
      .flatMap(tup =>  selSegmentsIdx
                        .map(segmentIdx => (segmentIdx, tup._2.apply(segmentIdx)))
                        .filter(tup => tup._2 >0) // remove scores <= 0
      ) //<segment_idx, score>
      .topByKey(sizeMax)
      .map(t => (t._1, if (t._2.length >= sizeMap(t._1.toInt)) t._2( sizeMap(t._1.toInt) - 1 ) else t._2.last )) // get the kth value #
      .collect()
      .toMap
    */

    var rankedScoreDF = scoreMatrix
      .flatMap(tup =>  selSegmentsIdx
                          .map(segmentIdx => (segmentIdx, tup._2.apply(segmentIdx))) .filter(tup => tup._2 >0) // remove scores <= 0
              ) //<segment_idx, score>
      .toDF("segment_idx", "score")
      .withColumn("rank", row_number.over(Window.partitionBy("segment_idx").orderBy($"score".desc)))
      .filter($"rank" <= sizeMax)
      .filter($"rank" >= sizeMin)
      .cache()

    val minScoreMap = selSegmentsIdx
      .map(segmentIdx => (segmentIdx,
                          rankedScoreDF.filter($"rank" === (sizeMap(segmentIdx) + 1) && $"segment_idx" ===  segmentIdx)
                          .take(1)
                          .map(row => row(1).toString.toDouble)
                         )
      )
      .map(tup => (tup._1, if(!tup._2.isEmpty) tup._2 else 0.0))
      .toMap

    rankedScoreDF.unpersist()


    // It generates masked vectors per indexed user, to indicate segments to expand (scores > threshold)
    /*var maskedScores = scoreMatrix
      .map(tup => (tup._1, selSegmentsIdx.map(segmentIdx => (minScoreMap contains segmentIdx) && (tup._2.apply(segmentIdx) >= minScoreMap(segmentIdx))).toArray) )
      .filter(tup=> tup._2.reduce(_||_))
      // <device_idx, array(boolean))>
    */
    var maskedScores = scoreMatrix
      .map(tup => (tup._1, selSegmentsIdx.map(segmentIdx => tup._2.apply(segmentIdx) > minScoreMap(segmentIdx)).toArray) )
      .filter(tup=> tup._2.reduce(_||_))
      // <device_idx, array(boolean))>

    if(!isTest){
      val devicesId = indexedData.map(t => (t._1, t._2)) // <device_idx, device_id>
      val userPredictions = maskedScores.join(devicesId).map(tup => (tup._2._2, tup._2._1))
      // <device_id, array(boolean)>
      writeOutput(spark, userPredictions, expandInput, segmentToIndex, metaParameters)
    }
    else{ 
      val maskedRelevant = indexedData.map(t => (t._1, selSegmentsIdx.map(segmentIdx => (t._3 contains segmentIdx)).toArray))
      val userPredictions = maskedScores.join(maskedRelevant).map(tup => (tup._2._2, tup._2._1))
      writeTest(spark, userPredictions,  expandInput, segmentToIndex, metaParameters)
    }

    // delete temp files
    fs.delete(new org.apache.hadoop.fs.Path(indexTmpPath), true)
    fs.delete(new org.apache.hadoop.fs.Path(scoresTmpPath), true)
  }

  /**
  * Generate output files
  */
  def writeOutput(spark: SparkSession,
                  data: RDD[(String, Array[(Boolean)])],
                  expandInput: List[Map[String, Any]],
                  segmentToIndex: Map[String, Int],
                  metaParameters: Map[String, String]){
    import spark.implicits._ 
    
    val isOnDemand = metaParameters("job_id").length > 0
    val country = metaParameters("country")

    val selSegmentsIdx = expandInput.map(m => segmentToIndex(m("segment_id").toString))
    val dstSegmentIdMap = expandInput.map(m => 
                  segmentToIndex(m("segment_id").toString) -> m("dst_segment_id").toString).toMap

    if (!isOnDemand){ // monthly expansion
      val dataExpansion = data
        .map(
            tup => 
              (tup._1, // device_id
              selSegmentsIdx
                .filter(segmentIdx => tup._2.apply(segmentIdx)) // select segments to expand
                .map(segmentIdx => dstSegmentIdMap(segmentIdx)) // get segment label
                .mkString(",") // toString
              )              
        )

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
    else{ // on demand expansion
      val jobId = metaParameters("job_id")
      val partnerId = metaParameters("partner_id")

      val dataExpansion = data
      .flatMap(
          tup => 
            (selSegmentsIdx
              .filter(segmentIdx => tup._2.apply(segmentIdx)) // select segments to expand
              .map(segmentIdx => ("web", tup._1.toString, dstSegmentIdMap(segmentIdx))) // <device_type, device_id, segment>
            )   
      )
      val filePath = "/datascience/data_lookalike/expansion/jobId=%s/".format(jobId)
      // save
      spark.createDataFrame(dataExpansion)
        .toDF("device_type", "device_id", "segment")
        .write
        .format("csv")
        .option("sep", "\t")
        .option("header", "false")
        .mode(SaveMode.Overwrite)
        .save(filePath)
      writeOutputMetaFile(filePath, jobId, partnerId)
    }
    
  }

  /**
  * This method generates a new json file that will be used by the Ingester to push the recently
  * downloaded audiences into the corresponding DSPs.
  */
  def writeOutputMetaFile(
      file_path: String,
      jobId: String,
      partnerId: String,
      priority: String = "10",
      queue: String = "datascience"
  ) {
    println("Lookalike LOG:\n\tPushing the audience to the ingester")

    var description = "Lookalike on demand - jobId = %s".format(jobId)

    // Then we generate the content for the json file.
    val json_content = """{"filePath":"%s", "priority":%s, "D":%s,
                          "queue":"%s", "jobId":%s, "description":"%s"}"""
      .format(
        file_path,
        priority,
        partnerId,
        queue,
        jobId,
        description
      )
      .replace("\n", "")
    println("Lookalike LOG:\n\t%s".format(json_content))

    // Finally we store the json.
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://rely-hdfs")
    val fs = FileSystem.get(conf)
    val os = fs.create(
      new Path("/datascience/ingester/ready/lal_%s.meta".format(jobId))
    )
    os.write(json_content.getBytes)
    os.close()
  }

               

  /*
  * It calculates precision, recall and F1 metrics.
  */
  def writeTest(spark: SparkSession,
           data: RDD[(Array[(Boolean)], Array[(Boolean)])],
           expandInput: List[Map[String, Any]] ,
           segmentToIndex: Map[String, Int],
           metaParameters: Map[String, String],         
           minSegmentSupport: Int = 100){
  import spark.implicits._ 

  val country = metaParameters("country")

  val selSegmentsIdx = expandInput.map(m => segmentToIndex(m("segment_id").toString))
  val dstSegmentIdMap = expandInput.map(m => 
                  segmentToIndex(m("segment_id").toString) -> m("dst_segment_id").toString).toMap

  // <segmentIdx> -> (nP, nSelected, nTP)
  var relevanCount = data
    .flatMap(tup => selSegmentsIdx.map(
        colIdx => (colIdx, (if(tup._1.apply(colIdx)) 1 else 0, 
                            if(tup._2.apply(colIdx)) 1 else 0, 
                            if(tup._1.apply(colIdx) && tup._2.apply(colIdx)) 1 else 0)
                  ) 
        )
    )
    .reduceByKey(
      (a, b) => ((a._1 + b._1), (a._2 + b._2), (a._3 + b._3))
    )
    .collect
    .toMap

   // (segment_id, count, selected, prec, recall)
    var metrics = selSegmentsIdx
        .map(colIdx => (dstSegmentIdMap(colIdx).toString,
                        relevanCount(colIdx)._1.toDouble, 
                        relevanCount(colIdx)._2.toDouble,  
                        if(relevanCount(colIdx)._2 > 0) 
                          relevanCount(colIdx)._3.toDouble / relevanCount(colIdx)._2.toDouble
                        else 0.0,
                        if(relevanCount(colIdx)._1 > 0)
                          relevanCount(colIdx)._3.toDouble / relevanCount(colIdx)._1.toDouble
                        else 0.0
                      )
        ).toList

    var dfMetrics = metrics
      .toDF("segment", "nRelevant", "nSelected", "precision", "recall" ) //.filter($"nRelevant" >= minSegmentSupport) TODO ADD all relevants
      .withColumn("f1", when($"precision" + $"recall" > 0.0, ($"precision" * $"recall") / ( $"precision" + $"recall") * 2.0).otherwise(0.0))
      .sort(desc("precision"))
    
    var dfAvgMetrics = dfMetrics
      .select(
        lit("Avg").as("segmentIdx"),
        avg($"nRelevant").as("nRelevant"),
        avg($"nSelected").as("nSelected"),
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
    
    df.show(expandInput.length + 1, false)
  }

  /***
  Read segments from input file.
  ***/
  def readSegmentsToExpand(
      spark: SparkSession,
      filePath: String
  ): List[Map[String, Any]] = {
    // First of all we obtain all the data from the file
    val df = spark.sqlContext.read.json(filePath)
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

      val actual_map: Map[String, Any] = Map(
        "segment_id" -> segmentId,
        "dst_segment_id" -> dstSegmentId,
        "size" -> size
      )

      expandInputs = expandInputs ::: List(actual_map)
    }
    expandInputs
  }


  /***
  Read input meta parameters
  ***/
  def readMetaParameters(
      spark: SparkSession,
      filePath: String
  ): Map[String, String] = {
    // First of all we obtain all the data from the file
    val df = spark.sqlContext.read.json(filePath)
    val data = df
      .collect()
      .map(fields => fields.getValuesMap[Any](fields.schema.fieldNames))
    // Now we extract the different values from each row.
    var jobId = ""
    var partnerId = ""
    var country = ""

    for (line <- data) {
      jobId =
          if (line.contains("jobId") && Option(line("jobId"))
            .getOrElse("")
            .toString
            .length > 0) line("jobId").toString
          else jobId

      partnerId =
          if (line.contains("partnerId") && Option(line("partnerId"))
            .getOrElse("")
            .toString
            .length > 0) line("partnerId").toString
          else partnerId

      country =
          if (line.contains("country") && Option(line("country"))
            .getOrElse("")
            .toString
            .length > 0) line("country").toString.toUpperCase
          else country
          
    }
    val map: Map[String, String] = Map(
        "job_id" -> jobId,
        "partner_id" -> partnerId,
        "country" -> country
    )
    map
  }


  /***
  Get segments to use to expand in runTest.
  ***/
  def getSegmentsToTest(
      size: Int
  ): List[Map[String, Any]] = {

    // 1) Segments to expand
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

    var expandInputs = List[Map[String, Any]]()

    for (segmentId <- segments) {
      val actual_map: Map[String, Any] = Map(
        "segment_id" -> segmentId,
        "dst_segment_id" -> segmentId,
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
      if (options.contains('filePath)) options('filePath) else ""
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
    else if(filePath.length > 0)
      runExpand(spark, filePath, nDays, simHits, predHits)
    else
      processPendingJobs(spark, nDays, simHits, predHits)
  }
}
