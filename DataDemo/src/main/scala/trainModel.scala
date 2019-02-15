package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.joda.time.{Days, DateTime}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel}
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.joda.time.Days
import org.joda.time.DateTime
import org.apache.hadoop.conf.Configuration
import org.apache.spark.ml.classification.{RandomForestClassificationModel,RandomForestClassifier}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}


object TrainModel {
  def getTrainingSet(spark: SparkSession, country:String) {
    val df = spark.read.parquet("/datascience/data_demo/triplets_segments/country=%s".format(country))

    val gt_male = spark.read
      .format("csv")
      .option("sep", " ")
      .load("/datascience/devicer/processed/ground_truth_male")
      .withColumn("label", lit(1))
      .withColumnRenamed("_c1", "device_id")
      .select("device_id", "label")
    val gt_female = spark.read
      .format("csv")
      .option("sep", " ")
      .load("/datascience/devicer/processed/ground_truth_female")
      .withColumn("label", lit(0))
      .withColumnRenamed("_c1", "device_id")
      .select("device_id", "label")

    val gt = gt_male.unionAll(gt_female)

    /// Hacemos el join y sacamos todos los segmentos de genero.
    val joint = gt.join(df, Seq("device_id")).filter("feature NOT IN ('2','3','69207','69228','39525','40136','40881','36523')")

    joint.write.mode(SaveMode.Overwrite).save("/datascience/data_demo/training_set_%s/".format(country))
  }

  def getTestSet(spark: SparkSession, country:String){
    val df = spark.read.parquet("/datascience/data_demo/triplets_segments/country=%s".format(country))

    val users_no_gender = spark.read
      .format("csv")
      .option("sep", "\t")
      .load("/datascience/devicer/processed/users_no_gender/part-04616-cb74dcd0-b96b-4d5b-ad32-399a52128d55-c000.csv")
      .withColumnRenamed("_c1", "device_id")
      .select("device_id")
      .dropDuplicates("device_id")

    val joint = users_no_gender.join(df, Seq("device_id"))

    joint.write.mode(SaveMode.Overwrite).save("/datascience/data_demo/test_set_%s/".format(country))
  
  }
  def getLabeledPointTest(spark: SparkSession, country:String) {
     val data = spark.read.format("parquet").load("/datascience/data_demo/test_set_%s".format(country))
    
    // Leemo el indexer generado en el entrenamiento y lo usamos para indexar features
    val device_indexer =  new StringIndexer().setInputCol("device_id").setOutputCol("deviceIndex")
    val indexed1 = device_indexer.fit(data).transform(data)
    val feature_indexer = StringIndexerModel.read.load("/datascience/data_demo/feature_indexer").setInputCol("feature").setOutputCol("featureIndex")
    val indexed_data = feature_indexer.transform(indexed1)//.filter("featureIndex < 18777")

    device_indexer.write.overwrite.save("/datascience/data_demo/device_indexer_test")

    val maximo = 18776
    // Agrupamos y sumamos los counts por cada feature
    val grouped_indexed_data = indexed_data
      .groupBy("device_id", "featureIndex")
      .agg(sum("count").cast("int").as("count"))
    // Agrupamos nuevamente y nos quedamos con la lista de features para cada device_id
    val grouped_data = grouped_indexed_data
      .groupBy("device_id")
      .agg(
        collect_list("featureIndex").as("features"),
        collect_list("count").as("counts")
      )

    // Esta UDF arma un vector esparso con los features y sus valores de count.
    val udfFeatures = udf(
      (features: Seq[Double], counts: Seq[Int], maximo: Int) =>
        Vectors.sparse(
          maximo + 1,
          (features.toList.map(f => f.toInt) zip counts.toList.map(
            f => f.toDouble
          )).toSeq.distinct.sortWith((e1, e2) => e1._1 < e2._1).toSeq
        )
    )

    val df_final = grouped_data.withColumn(
      "features_sparse",
      udfFeatures(col("features"), col("counts"), lit(maximo))
    )
    df_final.write
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_demo/labeled_points_test_%s".format(country))
  }

  def generate_expansion(spark:SparkSession,country:String){
    val data = spark.read.format("parquet").load("/datascience/data_demo/labeled_points_test_%s".format(country))
    // Cargamos el pipeline entrenado
    val model = PipelineModel.read.load("/datascience/data_demo/pipeline_rf")
    // Predecimos sobre la data de test
    val predictions = model.transform(data)
    predictions.write.mode(SaveMode.Overwrite)
                    .save("/datascience/data_demo/expansion_%s".format(country))

  }

  def getLabeledPointTrain(spark: SparkSession, country:String) {
    val data = spark.read.format("parquet").load("/datascience/data_demo/training_set_%s".format(country))
    
    // Usamos indexamos los features y los devices id
    val device_indexer = new StringIndexer().setInputCol("device_id").setOutputCol("deviceIndex").fit(data)
    val indexed1 = device_indexer.transform(data)
    val feature_indexer = new StringIndexer().setInputCol("feature").setOutputCol("featureIndex").fit(indexed1)
    val indexed_data = feature_indexer.transform(indexed1)

    // Guardamos los indexers
    device_indexer.write.overwrite.save("/datascience/data_demo/device_indexer")
    feature_indexer.write.overwrite.save("/datascience/data_demo/feature_indexer")

    val maximo = indexed_data
      .agg(max("featureIndex"))
      .collect()(0)(0)
      .toString
      .toDouble
      .toInt

    // Agrupamos y sumamos los counts por cada feature
    val grouped_indexed_data = indexed_data
      .groupBy("device_id", "label", "featureIndex")
      .agg(sum("count").cast("int").as("count"))
    // Agrupamos nuevamente y nos quedamos con la lista de features para cada device_id
    val grouped_data = grouped_indexed_data
      .groupBy("device_id", "label")
      .agg(
        collect_list("featureIndex").as("features"),
        collect_list("count").as("counts")
      )

    // Esta UDF arma un vector esparso con los features y sus valores de count.
    val udfFeatures = udf(
      (label: Int, features: Seq[Double], counts: Seq[Int], maximo: Int) =>
        Vectors.sparse(
          maximo + 1,
          (features.toList.map(f => f.toInt) zip counts.toList.map(
            f => f.toDouble
          )).toSeq.distinct.sortWith((e1, e2) => e1._1 < e2._1).toSeq
        )
    )

    val df_final = grouped_data.withColumn(
      "features_sparse",
      udfFeatures(col("label"), col("features"), col("counts"), lit(maximo))
    )
    df_final.write
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_demo/labeled_points_%s".format(country))
  }

  def train_and_evaluate_model(spark: SparkSession,country:String) {
    import spark.implicits._

    val data = spark.read.format("parquet").load("/datascience/data_demo/labeled_points_%s".format(country))

    //We'll split the set into training and test data
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    val labelColumn = "label"

    //We define the assembler to collect the columns into a new column with a single vector - "features sparse"
    val assembler = new VectorAssembler()
      .setInputCols(Array("features_sparse"))
      .setOutputCol("features_sparse")

    val rf = new RandomForestClassifier()
        .setLabelCol(labelColumn)
        .setFeaturesCol("features_sparse")
        .setPredictionCol("predicted_" + labelColumn)
        .setNumTrees(100)

    //We define the Array with the stages of the pipeline
    val stages = Array(rf)

    //Construct the pipeline
    val pipeline = new Pipeline().setStages(stages)

    //We fit our DataFrame into the pipeline to generate a model
    val model = pipeline.fit(trainingData)

    //We'll make predictions using the model and the test data
    val predictions = model.transform(testData)
    predictions.write
      .mode(SaveMode.Overwrite)
      .save("/datascience/data_demo/predictions_rf")

    // We compute AUC and F1
    val predictionLabelsRDD = predictions
      .select("predicted_label", "label")
      .rdd
      .map(r => (r.getDouble(0), r.getInt(1).toDouble))
    val binMetrics = new BinaryClassificationMetrics(predictionLabelsRDD)

    val auc = binMetrics.areaUnderROC
    val f1 = binMetrics.fMeasureByThreshold

    //This will evaluate the error/deviation of the regression using the Root Mean Squared deviation
    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelColumn)
      .setPredictionCol("predicted_" + labelColumn)
      .setMetricName("rmse")

    //We compute the error using the evaluator
    val error = evaluator.evaluate(predictions)

    // We store the metrics in a json file
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://rely-hdfs")
    val fs = FileSystem.get(conf)
    val os = fs.create(new Path("/datascience/data_demo/metrics_rf.json"))
    val json_content =
      """{"auc":"%s", "f1":%s, "rmse":%s}""".format(auc, f1, error)
    os.write(json_content.getBytes)
    fs.close()

  }

  def train_model(spark:SparkSession, country:String){
    import spark.implicits._

    val trainingData = spark.read.format("parquet").load("/datascience/data_demo/labeled_points_%s".format(country))
  
    val labelColumn = "label"

    val rf = new RandomForestClassifier()
        .setLabelCol(labelColumn)
        .setFeaturesCol("features_sparse")
        .setPredictionCol("predicted_" + labelColumn)
        .setNumTrees(100)

    //We define the Array with the stages of the pipeline
    val stages = Array(rf)

    //Construct the pipeline
    val pipeline = new Pipeline().setStages(stages)

    //We fit our DataFrame into the pipeline to generate a model
    val model = pipeline.fit(trainingData)

    // Save the trained model
    model.write.overwrite.save("/datascience/data_demo/pipeline_rf")
  }




  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Train and evaluate model").getOrCreate()
    val country = if (args.length > 0) args(0).toString else "MX"
    
    //getTrainingSet(spark,country)
    //train_model(spark,country)
    //getTestSet(spark,country)
    //getLabeledPointTrain(spark,country)
    getLabeledPointTest(spark,country)
    generate_expansion(spark,country)
  }

}
