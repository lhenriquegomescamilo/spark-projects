package main.scala

import org.apache.spark.mllib.recommendation.{ ALS , Rating }
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions.{ sum, col }
import org.apache.spark.sql.{ SaveMode, DataFrame, Row, SparkSession }
import org.apache.spark.rdd.RDD


object LookAlike {
  def getData(spark: SparkSession): DataFrame = {
    val data: DataFrame = spark.read.parquet("/datascience/data_demo/triplets_segments/part-05744-36693c74-c327-43a6-9482-2e83c0ead518-c000.snappy.parquet")
                               .groupBy("device_id", "feature").agg(sum(col("count")).as("count"))
    data
  }
  
  def getRatings(triplets: DataFrame): RDD[Rating] = {
    val indexer_devices = new StringIndexer().setInputCol("device_id").setOutputCol("device_id_index")
    val indexer_segments = new StringIndexer().setInputCol("feature").setOutputCol("feature_index")

    val data_dev_indexed = indexer_devices.fit(triplets).transform(triplets)
    val data_indexed = indexer_segments.fit(triplets).transform(data_dev_indexed)

    val ratings: RDD[Rating] = data_indexed.select("device_id_index", "feature_index", "count")
                                           .rdd.map(_ match { case Row(user, item, rate) =>
                                              Rating(user.asInstanceOf[Double].toInt, item.asInstanceOf[Double].toInt, rate.asInstanceOf[Integer].toDouble)
                                            })

    ratings
  }
  
  def train(training: RDD[Rating], test: RDD[(Int, Int)], rank: Int, numIter: Int, lambda: Double) {
    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setRank(rank)
      .setIterations(numIter)
      .setLambda(lambda)
    val model = als.run(training)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("value")
      .setPredictionCol("prediction")
    //val predictions = model.transform(test)
    val predictions = model.predict(test)

    println(predictions)
    predictions.take(20)
    //val rmse = evaluator.evaluate(predictions)
    //println("RMSE (test) = " + rmse + " for the model trained with lambda = " + lambda + ", and numIter = " + numIter + ".")
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("LookAlike modeling").getOrCreate()
    val sqlContext= new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    val triplets = getData(spark)
    val ratings = getRatings(triplets)

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
    training.take(20)
    train(training, test.map(rating => (rating.user, rating.product)), 8, 5, 0.01)
  }
}
