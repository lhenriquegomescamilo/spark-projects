package main.scala

import org.apache.spark.mllib.recommendation.{
  ALS,
  Rating,
  MatrixFactorizationModel
}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions.{sum, col, lit, broadcast}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.spark.rdd.RDD

object LookAlike {

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
    val ratings: RDD[Rating] = data
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
      .mode(SaveMode.Overwrite)
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
      .setRank(rank)
      .setBlocks(200)
      .setCheckpointInterval(2)
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

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("LookAlike modeling").getOrCreate()
    val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import sqlContext.implicits._
    spark.sparkContext.setCheckpointDir(
      "/datascience/data_lookalike/als_checkpoint"
    )

    // getTripletsWithIndex(spark, "MX")

    val triplets = spark.read.load(
      "/datascience/data_lookalike/segment_triplets_with_index/country=MX/part-020*-3023c398-0b95-4e9d-afb5-196e424c15dd.c000.snappy.parquet"
    )
    val ratings = getRatings(triplets, "device_index")

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
    val model = train(
      spark,
      training,
      32,
      3,
      0.01
    )

    evaluate(spark, test, model)
  }
}
