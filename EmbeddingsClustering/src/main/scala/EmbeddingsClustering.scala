package main.scala

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vectors, Vector}

import java.io._
import scala.collection.mutable.WrappedArray

import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}



object EmbeddingsClustering {

  /**
  @param embeddingVectors: The training data, an RDD of Vectors.
  @param numClusters: Number of clusters.
  @param maxIterations: Max number of iterations.
  @param epsilon: The distance threshold within which we've consider centers to have converged.
  @param runs: Number of parallel runs, defaults to 1. The best model is returned.
  **/
  def kmeans(spark: SparkSession,
             embeddingVectors : RDD[Vector],
             numClusters : Int = 2,
             maxIterations: Int = 20,
             epsilon: Double = 1e-4,
             runs: Int = 1) {
    import spark.implicits._
    embeddingVectors.persist(StorageLevel.MEMORY_AND_DISK)
    println(s"kmeans conf: numClusters = $numClusters - maxIterations:$maxIterations - epsilon=$epsilon - runs=$runs")
    val clusters = KMeans.train(embeddingVectors, k=numClusters, max_iterations=maxIterations, epsilon=epsilon)
    val WSSSE = clusters.computeCost(embeddingVectors)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    var path="/datascience/data_clustering/kmeans_centers_k={}.csv".format(numClusters)
    println(s"Writing centroids in = $path")
    clusters
      .clusterCenters.map(v => v.toArray.mkString(",") )
      .toList
      .toDF()
      .write
      .format("csv")
      .option("sep", ";")
      .option("header", "false")
      .mode(SaveMode.Overwrite)
      .save(path)

  }

  type OptionMap = Map[Symbol, String]
  /**
    * This method parses the parameters sent.
    */
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--filePath" :: value :: tail =>
        nextOption(map ++ Map('filePath -> value), tail)
      case "--maxIterations" :: value :: tail =>
        nextOption(map ++ Map('maxIterations -> value), tail)
      case "--numClusters" :: value :: tail =>
        nextOption(map ++ Map('numClusters -> value), tail)
      case "--epsilon" :: value :: tail =>
        nextOption(map ++ Map('epsilon -> value), tail)
      case "--runs" :: value :: tail =>
        nextOption(map ++ Map('runs -> value), tail)
      case "--normalize" :: value :: tail =>
        nextOption(map ++ Map('normalize -> value), tail)
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Embeddings clustering")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val spark = sqlContext.sparkSession
    import sqlContext.implicits._

    Logger.getRootLogger.setLevel(Level.WARN)
    val options = nextOption(Map(), args.toList)

    val filePath =
      if (options.contains('filePath)) options('filePath) else ""
    val maxIterations =
      if (options.contains('maxIterations)) options('maxIterations).toInt else 20
    val numClusters =
      if (options.contains('numClusters)) options('numClusters).toInt else 2
    val epsilon =
      if (options.contains('epsilon)) options('epsilon).toDouble else 1e-4
    val normalize =
      if (options.contains('normalize)) true else false
    val runs =
      if (options.contains('runs)) options('runs).toInt else 1

  // read data
  var df = spark.read.format("csv").option("header", "true").load(filePath).drop("device_id")

  var embeddingVectors = {
    if (normalize)
      df.rdd
      .map(row=> row.toSeq.toArray.map(_.toString.toDouble))
      .map(values => (values, Vectors.norm(Vectors.dense(values), 2)))
      .map(tup => Vectors.dense(tup._1.map(v => v/tup._2)))
    else
      df.rdd.map(row=> Vectors.dense(row.toSeq.toArray.map(_.toString.toDouble)))
  }

  kmeans(spark,
          embeddingVectors,
          numClusters=numClusters,
          maxIterations=maxIterations,
          epsilon=epsilon,
          runs=runs)

  }
}

