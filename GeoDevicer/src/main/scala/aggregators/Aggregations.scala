package main.scala.aggregators

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime

object Aggregations {

 
  def userAggregate(
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
//this function sis used to create the map


  def POIAggregate(
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {
    // First we load the data.
    val data = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .load(
        "/datascience/geo/geo_processed/%s_aggregated"
          .format(value_dictionary("poi_output_file"))
      )

    // Here we perform the aggregation
    data
      // First we keep the users that are valid in a new column
      .withColumn(
        "device_id_valid",
        when(col("validUser") === "true", col("device_id")).otherwise(null)
      )
      // Now we group by the POI id
      .groupBy(value_dictionary("poi_column_name"))
      // Here we do all the aggregations
      .agg(
        countDistinct("device_id_valid").as("unique_true_users"),
        count("device_id_valid").as("true_users_visits"),
        countDistinct("device_id").as("unique_paserby"),
        count("device_id").as("total_detections")
      )
      // Finally we store the results
      .write
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save(
        "/datascience/geo/map_data/%s_map"
          .format(value_dictionary("poi_output_file"))
      )
}

/*
    def get_segments  (
      spark: SparkSession,
      value_dictionary: Map[String, String]
  ) = {

        ////////////////////Getting web segments for users
            //  if(value_dictionary("crossdevice")=="1" 
            //    &  value_dictionary("audience")=="1" &  
            //    value_dictionary("web_days").toInt>0) {
           // Esta sección sólo va a tener sentido si se eligió hacer un crossdevice 

        // First we obtain the configuration to be allowed to watch if a file exists or not
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(conf)

        // Get the days to be loaded
        val format = "yyyyMMdd"
        val since = 1
        val end = DateTime.now.minusDays(since)
        val days = (0 until value_dictionary("web_days").toInt).map(end.minusDays(_)).map(_.toString(format))
        val path = "/datascience/data_keywords"

        // Now we obtain the list of hdfs folders to be read
        val hdfs_files = days
                    .map(day => path + "/day=%s/country=%s".format(day,value_dictionary("country")))
                    .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))

        val segments = spark.read.option("basePath", path).parquet(hdfs_files: _*)

        // Importamos implicits para que funcione el as[String]

        import spark.implicits._

        //hay que elegir una opción para la agregación de los segmentos
        //if(web_agreggator = "audience"
        //if  value_dictionary("poi_column_name")
        //    value_dictionary("audience_column_name")

        val data = spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", "\t")
        .load(
        "/datascience/geo/geo_processed/%s_aggregated"
          .format(value_dictionary("poi_output_file"))
        )


        val joint = data.select("device_id",value_dictionary("poi_column_name"))
                              .join(segments, Seq("device_id"))
                              .withColumn("segments", explode(col("segments")))
                              .groupBy("name", "segments")
                              .agg(countDistinct(col("device_id")) as "unique_count" )
                              
                              

        val output_path_segments = "/datascience/geo/geo_processed/%s_w_segments"
                                                            .format(value_dictionary("poi_output_file"))

        joint.write.format("csv")
                    .option("header", "true")
                    .mode(SaveMode.Overwrite)
                    .save(output_path_segments)

  }
*/


  //add segments


}
