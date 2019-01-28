package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.joda.time.{Days,DateTime}
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.{ SaveMode, DataFrame }
import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.joda.time.Days
import org.joda.time.DateTime


object FromEventqueuePII {

    def getPII(spark: SparkSession,
               nDays: Integer) {

    val from = 1

    // Now we get the list of days to be downloaded
    val format = "yyyy/MM/dd"
    val end   = DateTime.now.minusDays(since)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))


    val files = nDays.map(day => "/data/eventqueue/%s/*.tsv.gz".format(day))
    val data = spark.read.format("csv").option("sep", "\t").option("header", "true").load(files:_*)
	   
              
    data.withColumn("day", lit(nDays))
        .filter(col("data_type").contains("hash") && (col("ml_sh2").isNotNull ||  col("mb_sh2").isNotNull ||  col("nid_sh2").isNotNull ))
        .select( "device_id", "device_type","country","id_partner","data_type","ml_sh2", "mb_sh2", "nid_sh2","timestamp")
        .write
        .format("parquet")
        .mode(SaveMode.Append)
        .partitionBy("day")
        .save("/datascience/pii_matching/pii_tuples")
	
			}


  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Get Pii from Eventqueue").getOrCreate()

    getPII(spark,1)

  }

}
