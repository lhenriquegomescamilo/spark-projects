package main.scala

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions.{sum, col, lit, broadcast}
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.Row

object LookAlike {
  def getData(spark: SparkSession): DataFrame = {
    val data: DataFrame = spark.read
      .parquet("/datascience/data_demo/triplets_segments/country=MX/")
    // .groupBy("device_id", "feature")
    // .agg(sum(col("count")).as("count"))
    data
  }

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

  def getRatings(triplets: DataFrame): RDD[Rating] = {
    // val indexer_devices = new StringIndexer()
    //   .setInputCol("device_id")
    //   .setOutputCol("device_id_index")
    // val indexer_segments = new StringIndexer()
    //     .setInputCol("feature")
    //     .setOutputCol("feature_index")

    // val data_dev_indexed =
    //   indexer_devices.fit(triplets.select("device_id")).transform(triplets)
    // val data_indexed =
    //   indexer_segments
    //     .fit(data_dev_indexed.select("feature"))
    //     .transform(data_dev_indexed)

    val ratings: RDD[Rating] = triplets//data_indexed
      .select("device_index", "feature_index", "count")
      .rdd
      .map(_ match {
        case Row(user, item, rate) =>
          Rating(
            user.toString.toInt,//.asInstanceOf[Long],//.toInt,
            item.toString.toInt,//.asInstanceOf[Long],//.toInt,
            rate.toString.toDouble
          )
      })

    ratings
  }

  def train(
      training: RDD[Rating],
      test: RDD[(Int, Int)],
      rank: Int,
      numIter: Int,
      lambda: Double
  ) {
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

    println("LOGGER")
    println(predictions)
    predictions.take(20).foreach(println)
    val rmse = evaluator.evaluate(predictions)
    println("RMSE (test) = " + rmse + " for the model trained with lambda = " + lambda + ", and numIter = " + numIter + ".")
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("LookAlike modeling").getOrCreate()
    val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    // val triplets = getData(spark)

    // dfZipWithIndex(
    //   triplets.select("device_id").distinct(),
    //   0,
    //   "device_index",
    //   false
    // ).withColumn("country", lit("MX"))
    //   .write
    //   .partitionBy("country")
    //   .save("/datascience/data_lookalike/device_index")

    // dfZipWithIndex(
    //   triplets.select("feature").distinct(),
    //   0,
    //   "feature_index",
    //   false
    // ).withColumn("country", lit("MX"))
    //   .write
    //   .partitionBy("country")
    //   .save("/datascience/data_lookalike/feature_index")

    // val device_index = spark.read.load("/datascience/data_lookalike/device_index/country=MX/")
    // val feature_index = spark.read.load("/datascience/data_lookalike/feature_index/country=MX/")
    // triplets.join(broadcast(feature_index), Seq("feature"))
    //         .join(device_index, Seq("device_id"))
    //         .withColumn("country", lit("MX"))
    //         .write
    //         .partitionBy("country")
    //         .save("/datascience/data_lookalike/segment_triplets_with_index")


    // val triplets = spark.read.load("/datascience/data_lookalike/segment_triplets_with_index/country=MX/")
    val triplets = spark.read.load("/datascience/data_lookalike/segment_triplets_with_index/country=MX/part-00171-70064560-b03f-4ebc-8631-66f4c987a21c.c000.snappy.parquet")
    val ratings = getRatings(triplets)

    val Array(training, test) = ratings.randomSplit(Array(0.9, 0.1))
    //training.take(20)
    train(
      training,
      test.map(rating => (rating.user, rating.product)),
      8,
      5,
      0.01
    )
  }
}
