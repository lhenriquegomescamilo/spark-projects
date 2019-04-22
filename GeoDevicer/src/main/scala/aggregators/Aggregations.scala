package main.scala.aggregators

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode


object Aggregations {
  def regularAggregate(
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {
    // First we load the file with all the Geo information obtained after the join.
    val data = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load("/datascience/geo/%s".format(value_dictionary("poi_output_file")))

    // This function takes two lists: timestamps and distances. It checks that the user has been in a POI a number of minutes within the range:
    //                  umbralmin < n_minutes_in_poi < umbralmax
    // Also it checks that the corresponding distances is at most umbraldist (in meters).
    val hasUsedPoi = udf(
      (timestamps: Seq[String], stopids: Seq[String]) =>
        ((timestamps.slice(1, timestamps.length) zip timestamps).map(
          t =>
            (t._1.toInt - t._2.toInt < (value_dictionary("umbralmax").toInt * 60)) & ((value_dictionary(
              "umbralmin"
            ).toInt * 60) < t._1.toInt - t._2.toInt)
        ) zip (stopids.slice(1, stopids.length) zip stopids).map(
          s =>
            ((s._1.toFloat < (value_dictionary("umbraldist").toInt)) | (s._2.toFloat < (value_dictionary(
              "umbraldist"
            ).toInt)))
        )).exists(b => b._1 && b._2)
    )

    // Path where we will store the results.
    val output_path_anlytics = "/datascience/geo/geo_processed/%s_aggregated"
      .format(value_dictionary("poi_output_file"))

    // Here we do the aggregation
    data
      .withColumn("distance", col("distance").cast("double"))
      .groupBy(value_dictionary("poi_column_name"), "device_id", "device_type")
      // We obtain the list of timestamps and distances, along with the minimum distance
      .agg(
        collect_list(col("timestamp")).as("timestamp_list"),
        collect_list(col("distance")).as("distance_list"),
        min(col("distance")).as("min_distance")
      )
      // Now we obtain the frequency
      .withColumn("frequency", size(col("timestamp_list")))
      // Here we calculate if the user is valid based on the thresholds
      .withColumn(
        "validUser",
        hasUsedPoi(col("timestamp_list"), col("distance_list"))
      )
      // Here we transform the lists into strings
      .withColumn("timestamp_list", concat_ws(",", col("timestamp_list")))
      .withColumn("distance_list", concat_ws(",", col("distance_list")))
      // Finally we write the results
      .write
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(output_path_anlytics)

  }
}
