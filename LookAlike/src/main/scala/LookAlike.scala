package main.scala

import org.apache.spark.mllib.recommendation.{ ALS }//, Rating }
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions.{ sum, col }
import org.apache.spark.sql.{ SaveMode, DataFrame, Row, SparkSession }
import org.apache.spark.rdd.RDD


object LookAlike {
  def getData(spark: SparkSession): DataFrame = {
    val data: DataFrame = spark.read.parquet("/datascience/data_demo/triplets_segments/")
    data
  }

  case class Rating(userId: Int, movieId: Int, rating: Double)
  
  def getRatings(triplets: DataFrame): DataFrame = {
    val indexer_devices = new StringIndexer().setInputCol("device_id").setOutputCol("device_id_index")
    val indexer_segments = new StringIndexer().setInputCol("feature").setOutputCol("feature_index")

    val data_dev_indexed = indexer_devices.fit(triplets).transform(triplets)
    val data_indexed = indexer_segments.fit(data_dev_indexed).transform(data_dev_indexed)

    val ratings: RDD[Rating] = data_indexed.select("device_id_index", "feature_index", "count")
                              .groupBy("device_id_index", "feature_index").agg(sum(col("count")).as("count"))
                              .rdd.map(_ match { case Row(user, item, rate) =>
      Rating(user.asInstanceOf[Double].toInt, item.asInstanceOf[Double].toInt, rate.asInstanceOf[Integer].toDouble)
    })

    ratings.toDF()
  }
  
  def train(training: DataFrame, test: DataFrame, numIter: Int, lambda: Double) {
    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(numIter)
      .setRegParam(lambda)
      .setUserCol("userId")
      .setItemCol("feature")
      .setRatingCol("value")
    val model = als.fit(training)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("value")
      .setPredictionCol("prediction")
    val predictions = model.transform(test)

    predictions.show()
    val rmse = evaluator.evaluate(predictions)
    println("RMSE (test) = " + rmse + " for the model trained with lambda = " + lambda + ", and numIter = " + numIter + ".")
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("LookAlike modeling").getOrCreate()

    val triplets = getData(spark)
    val ratings = getRatings(triplets)

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
    training.show()
    train(training, test, 5, 0.01)
  }
}
