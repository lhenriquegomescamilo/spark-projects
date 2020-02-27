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

import org.apache.spark.mllib.linalg.{Vector, Vectors, DenseMatrix, Matrix}
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
                         nDaysSegment: Int = 0,
                         simMatrixHits: String = "binary",
                         simThreshold: Double = 0.05,
                         predMatrixHits: String = "binary",
                         useFactualSegments: Boolean = false,
                         useStartapSegments: Boolean = false) {

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
      var fileDone = pathDone + filename
      var fileFailed = pathFailed + filename
      var processError = false
    
      var hadoopPathInProcess = new Path(fileInProcess)
      var hadoopPathToProcess = new Path(fileToProcess)
      var hadoopPathDone= new Path(fileDone)
      var hadoopPathFailed = new Path(fileFailed)

      if(fs.exists(hadoopPathInProcess))
        fs.delete(hadoopPathInProcess, false)
      if(fs.exists(hadoopPathDone))
        fs.delete(hadoopPathDone, false)
      if(fs.exists(hadoopPathFailed))
        fs.delete(hadoopPathFailed, false)

      fs.rename(hadoopPathToProcess, hadoopPathInProcess)

      try {
        runExpand(spark, fileInProcess, nDays, nDaysSegment, simMatrixHits, simThreshold, predMatrixHits,
                  useFactualSegments, useStartapSegments)
      } catch {
        case e: Throwable => {
          var errorMessage = e.toString()
          println("LOOKALIKE LOG: The lookalike process failed on " + filename + "\nThe error was: " + errorMessage)
          processError = true
      }
    }
    if (!processError)
      fs.rename(hadoopPathInProcess, hadoopPathDone)
    else
      fs.rename(hadoopPathInProcess, hadoopPathFailed)
    }
  }

  /**
  It runs a expansion using the input file specifications.
  The input file must have the country, and all segments to expand.
  */
  def runExpand(spark: SparkSession,
                filePath: String,
                nDays: Int = -1,
                nDaysDataSegment: Int = 0,
                simMatrixHits: String = "binary",
                simThreshold: Double = 0.05,
                predMatrixHits: String = "binary",
                useFactualSegments: Boolean = false,
                useStartapSegments: Boolean = false) {
    import spark.implicits._

    println("LOOKALIKE LOG: Input File: " + filePath)
    // Read input from file
    val metaInput = readMetaParameters(spark, filePath)
    val expandInput = readSegmentsToExpand(spark, filePath)
    val segmentsToExpand = expandInput.map(row=> row("segment_id").toString)

    val isOnDemand = metaInput("job_id").length > 0
    val country = metaInput("country")

    var baseFeatureSegments = getBaseFeatureSegments()

    // We use factual segments in BR to imporve volumes
    if(useFactualSegments || (isOnDemand && country == "BR")){
      val factualSegments = getFactualSegments()
      baseFeatureSegments ++= factualSegments.toSet.diff(baseFeatureSegments.toSet).toList
    }

    if(useStartapSegments){
      // TODO get US segments to country = US
      val startapSegments = getLatamStartappSegments()
      baseFeatureSegments ++= startapSegments.toSet.diff(baseFeatureSegments.toSet).toList
    }

    val extraFeatureSegments = getExtraFeatureSegments()

    val nSegmentToExpand = expandInput.length

    if (isOnDemand)
      println("LOOKALIKE LOG: JobId: " + metaInput("job_id") + " - Country: " + country + " - nSegments: " + nSegmentToExpand.toString + " - Output: " +  metaInput("output_name"))
    else
      println("LOOKALIKE LOG: Country: " + country + " - nSegments: " + nSegmentToExpand.toString + " - Output: " +  metaInput("output_name"))

    var nDaysData = if(nDays != -1 ) nDays else if (List("AR", "MX") contains country) 15 else 30
    
    // Read data
    println("LOOKALIKE LOG: Model training")

    val data = {
      if (nDaysDataSegment <= nDaysData)
        getDataTriplets(spark, country, nDaysData) 
      else
        getDataTriplets(spark, country, nDaysData)
        .union(getDataTripletsSegmentsToExpand(spark, segmentsToExpand, country, nDaysDataSegment - nDaysData, nDaysData))
    }
    
    // Create segment index
    var segments = segmentsToExpand // First: segments to expand
    segments ++= baseFeatureSegments.toSet.diff(segments.toSet).toList // Then: segments used as features
    segments ++= extraFeatureSegments.toSet.diff(segments.toSet).toList 

    val segmentToIndex = segments.zipWithIndex.toMap
    val dfSegmentIndex = segments.zipWithIndex.toDF("feature", "segment_idx")

    val baseSegmentsIdx = baseFeatureSegments.map(seg => segmentToIndex(seg))

    // Data aggregation
    val dataTriples = data
      .groupBy("device_id", "feature")
      .agg(first("device_type").as("device_type"), first("activable").as("activable"), sum("count").cast("int").as("count"))

    var usersSegmentsData = dataTriples
      .filter(col("feature").isin(segments: _*))   // segment filtering
      .join(broadcast(dfSegmentIndex), Seq("feature")) // add segment column index
      .select("device_id", "device_type", "activable", "segment_idx", "count")
      .rdd
      .map(row => ((row(0), row(1), row(2)), (row(3), row(4)))) // < <device_id, device_type, activable>, <segment_idx, count> >
      .groupByKey()  // group by <device_id, device_type, activable>  Values: Iterable(<segment_idx, count>)
      .filter(row => row._2.map(t => t._1.toString.toInt).exists(baseSegmentsIdx.contains)) // Filter users who contains any base segments

    // Generate similarities matrix
    val simMatrix: Matrix = {
      if(!existsTmpFiles(spark, metaInput)("scores"))
        getSimilarities(spark,
          usersSegmentsData,
          segments.size,
          nSegmentToExpand,
          simThreshold,
          simMatrixHits)
      else
        null
      }

    println("LOOKALIKE LOG: Expansion")
    // it applies "activable" filter for BR 
    if((isOnDemand && country == "BR")){
      usersSegmentsData = usersSegmentsData.filter(row => row._1._3  == 1)
    }

    expand(spark,
           usersSegmentsData,
           expandInput,
           segmentToIndex,
           metaInput,
           simMatrix,
           predMatrixHits)
  
  
  }


  /**
  * It generates the items to items matrix to make predictions.
  */
  def getSimilarities(spark: SparkSession,
                      data: RDD[((Any, Any, Any), Iterable[(Any, Any)])], // (<device_id, device_type, activable>, Iterable(<segment_idx, count>)
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
      .filter(me => me.j.toInt < nSegmentToExpand) // select only columns to predict
      ,simMatrix.numRows(), nSegmentToExpand)
                    
    // Collect the distributed matrix on the driver
    var localMartix = simSymmetric.toBlockMatrix().toLocalMatrix()

    // normalize matrix columns (to compute cosine scores in matrix multiplication)
    val colNorms = localMartix.colIter.map(col => Vectors.norm(col, 2)).toArray
    val nRows = localMartix.numRows
    val nCols = localMartix.numCols
    val values = for (j <- 0 until nCols; i <- 0 until nRows) yield {
      var norm = if(colNorms.apply(j) > 0) colNorms.apply(j) else 1.0
      if (i!=j) localMartix.apply(i, j) / norm else diagonalValue
    }
    println(s"Lookalike LOG: Similarity Matrix - elements non-zero per segments to expand: %s".format(localMartix.colIter.map(col => col.numNonzeros ).toList.toString))
    localMartix = new DenseMatrix(nRows, nCols, values.toArray)
    localMartix
  }

  /*
  * It reads data triplets for a given country.
  */
  def getDataTriplets(
      spark: SparkSession,
      country: String,
      nDays: Int = 30,
      from: Int = 1,
      path: String = "/datascience/data_triplets/segments/") = {
    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // read files from dates
    val format = "yyyyMMdd"
    val endDate = DateTime.now.minusDays(from)
    val days = (0 until nDays.toInt).map(endDate.minusDays(_)).map(_.toString(format))
    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = days
      .map(day => path + "/day=%s/country=%s".format(day, country))
      .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
    var df = spark.read.option("basePath", path).parquet(hdfs_files: _*)
    
    // backward compatibility (this can cause duplicate devices)
    if (!df.columns.contains("count"))
      df = df.withColumn("count", lit(1))
    if (!df.columns.contains("activable"))
      df = df.withColumn("activable", lit(1))
    if (!df.columns.contains("device_type"))
      df = df.withColumn("device_type", lit("web"))

    df = df.select("device_id", "device_type", "activable", "feature", "count")

  }


  /*
  * It reads data triplets for the segments to expand.
  * It allows loading devices with the segments to expand using more historical data.
  */
  def getDataTripletsSegmentsToExpand(
      spark: SparkSession,
      segments: List[String], 
      country: String,
      nDays: Int = 30,
      from: Int = 1,
      path: String = "/datascience/data_triplets/segments/") = {

    val df =  getDataTriplets(spark, country, nDays, from, path)
    df.filter(col("feature").isin(segments: _*))
  }

  /**
  * For each segment, it calculates a score value for all users and generates the expansion.
  */
  def expand(spark: SparkSession,
              data: RDD[((Any, Any, Any), Iterable[(Any, Any)])], // (<device_id, device_type, activable>, Iterable(<segment_idx, count>)
              expandInput: List[Map[String, Any]] ,
              segmentToIndex: Map[String, Int],
              metaParameters: Map[String, String],
              similartyMatrix: Matrix,
              predMatrixHits: String = "binary") =  {
    import spark.implicits._
    import org.apache.spark.mllib.rdd.MLPairRDDFunctions.fromPairRDD
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.row_number
    import scala.math.BigDecimal

    val country = metaParameters("country")
    val jobId = metaParameters("job_id")
    val isOnDemand = jobId.length > 0
    
    val minUserSegments = 1

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // index data and write in temporal file
    val indexTmpPath = getTmpPathNames(metaParameters)("indexed")

    if (!existsTmpFiles(spark, metaParameters)("indexed")){

      // generte data to store
      data
        .filter(row => row._2.size >= minUserSegments) // filter users
        .zipWithIndex() // <(<device_id, device_type, activable>, Iterable(<segment_idx, count>), device_idx>
        .map(tup => (tup._2.toLong, // device_idx
                     tup._1._1._1.toString, // device_id
                     tup._1._1._2.toString, // device_type
                     tup._1._2.map(t => t._1.toString.toInt).toArray, // segment_idx array 
                     tup._1._2.map(t => t._2.toString.toInt).toArray ) // counts array
        ) 
        .toDF("device_idx", "device_id", "device_type", "segments", "counts")
            .write
            .mode(SaveMode.Overwrite)
            .format("parquet")
            .save(indexTmpPath)
    } 

    // reload indexed data
    var indexedData = spark.read.load(indexTmpPath).as[(Long, String, String, Array[Int], Array[Double])].rdd
    // <device_idx, device_id, device_type, segment_idx array, counts array>

    var nSegments = segmentToIndex.size

    var indexedRows: RDD[IndexedRow] = {
      if (predMatrixHits == "count"){
        println(s"User matrix: count")
        indexedData  
          .map(row => (row._1, row._4, row._5, Math.sqrt(row._5.map(v => v*v).sum))) //<device_idx, array(segment_idx), array(counts), l2norm >
          .map(row => new IndexedRow(row._1,Vectors.sparse(nSegments, row._2, row._3.map(t => t/row._4)).toDense.asInstanceOf[Vector]))
      }
      else{
        println(s"User matrix: binary")
        indexedData
        .map(row => (row._1, row._4, Array.fill(row._4.size)(1.0), Math.sqrt(row._4.size)))  //<device_idx, array(segment_idx), array(binary), l2norm >
        .map(row => new IndexedRow(row._1,Vectors.sparse(nSegments, row._2, row._3.map(t => t/row._4)).toDense.asInstanceOf[Vector]))
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
    val scoresTmpPath = getTmpPathNames(metaParameters)("scores")

    if (!existsTmpFiles(spark, metaParameters)("scores")){
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
    //<device_idx, scores vector>

    // It gets the score thresholds to get at least k elements per segment.
    val selSegmentsIdx = expandInput.map(m => segmentToIndex(m("segment_id").toString))
    val sizeMap = expandInput.map(m => segmentToIndex(m("segment_id").toString) -> m("size").toString.toInt).toMap

    val minScoreMap = scoreMatrix
      .flatMap(tup =>  selSegmentsIdx
                           .map(segmentIdx => (segmentIdx, tup._2.apply(segmentIdx))).filter(tup => tup._2 >0) // remove scores <= 0
              ) //<segment_idx, score>
      .map(tup => ((tup._1,BigDecimal(tup._2).setScale(4, BigDecimal.RoundingMode.FLOOR).toDouble), 1L)) //<(segment_idx, rounded score) , 1L>
      .reduceByKey(_ + _) // count by <segment_idx, rounded score>
      .map{ case ((segment_idx, score), cnt) => (segment_idx, (score, cnt)) } //<(segment_idx, (rounded score , count) >
      .groupByKey // group scores by segments
      .map{ case (segment_idx, l) => (segment_idx, l.toList.sortWith(_._1 > _._1) ) } //<(segment_idx, List(rounded score , count) sorted by scores >
      .map{ case (segment_idx, l) => 
        (segment_idx, 
         l.zipWithIndex
         .filter({ case (v, i) => l.map(v => v._2).slice(0, i+1).sum > sizeMap(segment_idx) })
         .lift(0).getOrElse((0.0,0), 0)._1._1  ) } // cumulative count sum by th
      .filter{ case (segment_idx, score) => score > 0 }
      .collect
      .toMap

    println("Lookalike LOG: threshold scores = %s".format(minScoreMap))

    var resultDescription = if (minScoreMap.size > 0)
         "%d/%d segments with results".format(minScoreMap.size, selSegmentsIdx.size)
      else
        "no results found"
    println("Lookalike LOG: %s".format(resultDescription))

    var maskedScores = scoreMatrix
      .map(tup => (tup._1, selSegmentsIdx.map(segmentIdx => 
                                ((minScoreMap contains segmentIdx) && tup._2.apply(segmentIdx) >= minScoreMap(segmentIdx)) || 
                                (!(minScoreMap contains segmentIdx) && tup._2.apply(segmentIdx) > 0.0 )).toArray ))
      .filter(tup=> tup._2.reduce(_||_))
      // <device_idx, array(boolean))>

    
    // indexedData: <device_idx, device_id, device_type, segment_idx array, counts array>
    val devicesInfo = indexedData.map(t => (t._1, (t._2, t._3))) // <device_idx, (device_id, "device_type")>
    val userPredictions = maskedScores.join(devicesInfo).map(tup => (tup._2._2, tup._2._1))
      // <(device_id, "device_type"), array(boolean)>

    writeOutput(spark, userPredictions, expandInput, segmentToIndex, metaParameters, resultDescription)
    
    // delete temp files
    fs.delete(new org.apache.hadoop.fs.Path(indexTmpPath), true)
    fs.delete(new org.apache.hadoop.fs.Path(scoresTmpPath), true)
  }

  /*
  * It validates if temporal files alredy exist.
  * There are 2 temporal files:
  *   - indexed: Contains devices indexed and its asociated segments
  *   - scores: Contains prediction scores per devices 
  */
  def existsTmpFiles(spark: SparkSession,
                     metaParameters: Map[String, String]): Map[String, Boolean] = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val pathTmpFiles = getTmpPathNames(metaParameters)
    val existsTmpFiles: Map[String, Boolean] = Map(
        "scores" -> fs.exists(new org.apache.hadoop.fs.Path(pathTmpFiles("scores"))),
        "indexed" -> fs.exists(new org.apache.hadoop.fs.Path(pathTmpFiles("indexed")))
      )
    existsTmpFiles
  }

  /*
  * It returns a map with hdfs path names used to store temporal data.
  * There are 2 temporal files:
  *   - indexed: Contains devices indexed and its asociated segments
  *   - scores: Contains prediction scores per devices 
  */
  def getTmpPathNames(metaParameters: Map[String, String]): Map[String, String] = {
    val outputName = metaParameters("output_name")
    val scoresTmpPath = "/datascience/data_lookalike/tmp/scores/%s/".format(outputName)
    val indexTmpPath = "/datascience/data_lookalike/tmp/indexed_devices/%s/".format(outputName)

    val pathTmpFiles: Map[String, String] = Map(
        "scores" -> scoresTmpPath,
        "indexed" -> indexTmpPath
      )
    pathTmpFiles
  }

  /**
  * Generate output files
  */
  def writeOutput(spark: SparkSession,
                  data: RDD[((String, String), Array[(Boolean)])], // <(device_id, "device_type"), array(boolean)>
                  expandInput: List[Map[String, Any]],
                  segmentToIndex: Map[String, Int],
                  metaParameters: Map[String, String],
                  resultDescription: String = ""){
    import spark.implicits._ 
    
    val isOnDemand = metaParameters("job_id").length > 0
    val outputName = metaParameters("output_name")

    val selSegmentsIdx = expandInput.map(m => segmentToIndex(m("segment_id").toString))
    val dstSegmentIdMap = expandInput.map(m => 
                  segmentToIndex(m("segment_id").toString) -> m("dst_segment_id").toString).toMap

    if (!isOnDemand){ // monthly expansion
      val dataExpansion = data
        .map(
            tup => 
              (tup._1._1, // device_id
              selSegmentsIdx
                .filter(segmentIdx => tup._2.apply(segmentIdx)) // select segments to expand
                .map(segmentIdx => dstSegmentIdMap(segmentIdx)) // get segment label
                .mkString(",") // toString
              )              
        )
      
      var outputDF = spark.createDataFrame(dataExpansion).toDF("device_id", "segments")
      // save
      outputDF
        .write
        .format("csv")
        .option("sep", "\t")
        .option("header", "false")
        .mode(SaveMode.Overwrite)
        .save(
        "/datascience/data_lookalike/expansion/day=%s/%s/".format(DateTime.now().toString("yyyyMMdd"), outputName)
        )
    }
    else{ // on demand expansion
      val jobId = metaParameters("job_id")
      val filePath = "/datascience/data_lookalike/expansion/ondemand/%s/".format(outputName)

      val dataExpansion = data
      .flatMap(
          tup => 
            (selSegmentsIdx
              .filter(segmentIdx => tup._2.apply(segmentIdx)) // select segments to expand
              .map(segmentIdx => (tup._1._2.toString, tup._1._1.toString, dstSegmentIdMap(segmentIdx))) // <device_type, device_id, segment>
            )   
      )

      var outputDF = spark.createDataFrame(dataExpansion).toDF("device_type", "device_id", "segment")

      // if there is only a segment to expand (most common case)
      if (expandInput.length == 1){
        // it limits the number of rows to avoid large differences with 'size' parameter.
        var q = (expandInput.head("size").toString.toInt * 1.1).toInt
        var limit = (q * 1.1).toInt
        outputDF = outputDF.limit(limit)
        println("Lookalike LOG: Extra limit of output devices = %s - size param = %s".format(limit, q))
      }
      else{
        println("Lookalike LOG: segments to expand = %s - no extra limit".format(expandInput.length))
      }
  
      // save
      outputDF
        .write
        .format("csv")
        .option("sep", "\t")
        .option("header", "false")
        .mode(SaveMode.Overwrite)
        .save(filePath)
      writeOutputMetaFile(filePath, jobId, resultDescription)
    }
    
  }

  /**
  * This method generates a new json file that will be used by the Ingester to push the recently
  * downloaded audiences into the corresponding DSPs.
  */
  def writeOutputMetaFile(
      file_path: String,
      jobId: String,
      resultDescription: String = ""
  ) {
    println("Lookalike LOG:\n\tPushing the audience to the ingester")

    var description = "Lookalike on demand - jobId = %s - %s".format(jobId, resultDescription)

    // Then we generate the content for the json file.
    val json_content = """{"filePath":"%s", "jobId":%s, "description":"%s"}"""
      .format(
        if (file_path.takeRight(1) == "/") file_path.dropRight(1) else file_path, // remove last '/''
        jobId,
        description
      )
      .replace("\n", "")
    println("Lookalike LOG:\n\t%s".format(json_content))

    // Finally we store the json.
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://rely-hdfs")
    val fs = FileSystem.get(conf)

    var hadoopPath = new Path("/datascience/ingester/ready/lal_%s.meta".format(jobId))
    if(fs.exists(hadoopPath))
      fs.delete(hadoopPath, false)

    val os = fs.create(hadoopPath)
    os.write(json_content.getBytes)
    os.close()
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
      if (line.contains("segmentId") & line("segmentId") != null){
        val segmentId = line("segmentId").toString        
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
    var country = ""
    var outputName = ""

    for (line <- data) {
      jobId =
          if (line.contains("jobId") && Option(line("jobId"))
            .getOrElse("")
            .toString
            .length > 0) line("jobId").toString
          else jobId
      country =
          if (line.contains("country") && Option(line("country"))
            .getOrElse("")
            .toString
            .length > 0) line("country").toString.toUpperCase
          else country
      outputName =
          if (line.contains("outputName") && Option(line("outputName"))
            .getOrElse("")
            .toString
            .length > 0) line("outputName").toString
          else outputName
    }

    if (outputName == ""){
      if(jobId != "")
        outputName = "jobId=%s".format(jobId)
      else
        outputName = "country=%s".format(country)
    }
    val map: Map[String, String] = Map(
        "job_id" -> jobId,
        "country" -> country,
        "output_name" -> outputName
    )
    map
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


  def getLatamStartappSegments(
  ): List[String] = {
    val segments =
      """182697,182699,182703,182705,182707,182709,182711,182713,182715,182717,182719,182721,182723,182725,182727,182729,182731,182733,182735,182737,182739,
      182741,182743,182745,182747,182749,182751,182753,182755,182761,182763,182765,182767,182769,182773,182775,182777,182779,182781,182783,182785,182787,
      182789,182791,182797,182799,182801,182803,182805,182809,182811,182813,182815,182819,182821,182915,182917,182919,182921,182923,182927,182929,182931,
      182933,182935,182937,182939,182941,182943,182945,182947,182949,182951,182953,182955,182957,182959,182961,182963,182965,182967,182969,182971,182973,
      182975,182977,182979,182981,182983,182985,182987,182989,182991,182993,182995,182997,182999,183001,183003,183005,183007,183009,183011,183013,183015,
      183017,183019,183021,183023,183025,183027,183029,183031,183033,183037,183039,183041,183043,183047,183049,183051,183053,183055,183057,183059,183061,
      183063,183065,183067,183069,183071,183073,183075,183077,183079,183083,183085,183087,183089,183091,183093,183095,183097,183099,183101,183103,183105,
      183107,183109,183111,183113,183115,183117,183119,183123,183125,183127,183129,183131,183133,183139,183141,183143,183145,183147,183149,183151,183153,
      183155,183157,183159,185069,185071,185073,185075,185077,185079,185081,185083,185085,185087,185089,185091,185093,183161,183163,183165,183167,183169,
      183171,183173,183177,183179,183181,183183,183185,183187,183189,183191,183193,183195,183197,183199,183201,183205,183207,183209,183211,183213,183215,
      183217,183219,183221,183225,183227,183229,183231,183233,183235,183237,183239,183241,183243,183245,183247,183271,183273,183275,183277,183279,183281,
      183283,183285,185095,185097,185099,185101,185103,185105,185107,185109,185111,185113,185115,185117,185119,185121,185123,185127,185129,185131,185133,
      185135,185137,185139,185141,185143,185145,185147,185149,185151,185153,185155,185157,185159,185161,185163,185165,185167,185169,185171,185173,185175,
      185177,185179,185181,185183,185185,185187,185189,185191,183287,183289,183291,183293,183295,183299,183301,183303,183305,183307,183309,183311,183313,
      183315,183317,183319,183321,183323,183325,183327,183329,183331,183333,183335,183337,183339,183341,183343,183345,183347,183349,183351,183353,183355,
      183357,183359,183361,183363,183365,183367,183415,183417,183419,183421,183423,183425,183427,183429,183431,183433,183437,183439,183441,183443,183445,
      183447,183449,183451,183453,183455,183457,183459,183461,183463,183465,183467,183469,183471,183473,183475,183477,183479,183481,183483,183485,183487,
      183489,183491,183493,183495,183497,183499,183501,183503,183505,183507,183509,183515,183517,183519,183521,183523,183525,183527,183529,183531,183533,
      183535,183537,183539,183541,183543,183545,183547,183549,183551,183553,183555,183557,183559,183561,183563,183565,183567,183569,183571,183573,183575,
      183577,183579,183581,183583,183585,183587,183589,183591,183593,183595,183597,183599"""
        .replace("\n", "")
        .split(",")
        .toList
    segments
  }


  def getFactualSegments(
  ): List[String] = {
    val segments =
      """105056,105057,105058,105059,105060,105061,105062,105063,105064,105065,105066,105067,105068,105069,105070,
         105071,105072,105073,105074,105075,105076,105077,105078,105079,105080,105081,105082,105083,105084,105085,
         105086,105087,105088,105089,105090,105091,105092,105093,105094,105095,105096,105097,105098,105099,105100,
         105101,105102,105103,105104,105105,105106,105107,105108,105109,105110,105111,105112,105113,105114,105115,
         105116,105117,105118,105119,105120,105121,105122,105123,105124,105125,105126,105127,105128,105129,105130,
         105131,105132,105133,105134,105135,105136,105137,105138,105139,105140,105141,105142,105143,105144,105145,
         105146,105147,105148,105149,105150,105151,105152,105153,105154,105155,105156,105157,105158,105159,105160,
         105161,105162,105163,105164,105165,105166,105167,105168,105169,105170,105171,105172,105173,105174,105175,
         105176,105177,105178,105179,105180,105181,105182,105183,105184,105185,105186,105187,105188,105189,105190,
         105191,105192,105193,105194,105195,105196,105197,105198,105199,105200,105201,105202,105203,105204,105205,
         105206,105207,105208,105209,105210,105211,105212,105213,105214,105215,105216,105217,105218,105219,105220,
         105221,105222,105223,105224,105225,105226,105227,105228,105229,105230,105231,105232,105233,105234,105235,
         105236,105237,105238,105239,105240,105241,105242,105243,105244,105245,105246,105247,105248,105249,105250,
         105251,105252,105253,105254,105255,105256,105257,105258,105259,105260,105261,105262,105263,105264,105265,
         105266,105267,105268,105269,105270,105271,105272,105273,105274,105275,105276,105277,105278,105279,105280,
         105281,105282,105283,105284,105285,105286,105287,105288,105289,105290,105291,105292,105293,105294,105295,
         105296,105297,105298,105299,105300,105301,105302,105303,105304,105305,105306,105307,105308,105309,105310,
         105311,105312,105313,105314,105315,105316,105317,105318,105319,105320,105321,105322,105323,105324,105325,
         105326,105327,105328,105329,105330,105331,105332,105333,105334,105335,105336,105337,105338"""
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
      case "--testCountry" :: value :: tail =>
        nextOption(map ++ Map('testCountry -> value), tail)
      case "--testSize" :: value :: tail =>
        nextOption(map ++ Map('testSize -> value), tail)
      case "--testSegmentId" :: value :: tail =>
        nextOption(map ++ Map('testSegmentId -> value), tail)
      case "--nDays" :: value :: tail =>
        nextOption(map ++ Map('nDays -> value), tail)
      case "--nDaysSegment" :: value :: tail =>
        nextOption(map ++ Map('nDaysSegment -> value), tail)
      case "--simThreshold" :: value :: tail =>
        nextOption(map ++ Map('simThreshold -> value), tail)
      case "--useFactualSegments" :: value :: tail =>
        nextOption(map ++ Map('useFactualSegments -> value), tail)
      case "--useStartapSegments" :: value :: tail =>
        nextOption(map ++ Map('useStartapSegments -> value), tail)
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
    val testCountry =
      if (options.contains('testCountry)) options('testCountry) else "PE"
    val testSize =
      if (options.contains('testSize)) options('testSize).toInt else 1000
    val testSegmentId =
      if (options.contains('testSegmentId)) options('testSegmentId) else ""
    val nDays =
      if (options.contains('nDays)) options('nDays).toInt else -1
    val nDaysSegment =
      if (options.contains('nDaysSegment)) options('nDaysSegment).toInt else 0
    val simThreshold =
      if (options.contains('simThreshold)) options('simThreshold).toDouble else 0.05
    val useFactualSegments =
     if (options.contains('useFactualSegments)) options('useFactualSegments).toBoolean else false
    val useStartapSegments = 
     if (options.contains('useStartapSegments)) options('useStartapSegments).toBoolean else false

    if(filePath.length > 0)
      runExpand(spark, filePath, nDays, nDaysSegment, simHits, simThreshold, predHits,
                useFactualSegments, useStartapSegments)
    else
      processPendingJobs(spark, nDays, nDaysSegment, simHits, simThreshold, predHits,
                         useFactualSegments, useStartapSegments)
  }
}
